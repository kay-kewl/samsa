const std = @import("std");
const transport = @import("../transport/module.zig");
const generated = @import("../generated/module.zig");
const header = @import("../protocol/header.zig");
const codec = @import("../protocol/codec.zig");
const types = @import("../protocol/types.zig");
const errors = @import("errors.zig");
const versions = @import("versions.zig");
const metadata_cache = @import("metadata_cache.zig");
const model = @import("model.zig");
const router = @import("router.zig");

fn bootstrapBrokerId(host: []const u8, port: u16) i32 {
    var hash = std.hash.Wyhash.init(42);
    hash.update(host);
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, port, .big);
    hash.update(&buf);
    return @intCast(hash.final() & 0x7fff_ffff);
}

pub const Endpoint = struct {
    host: []const u8,
    port: u16,
};

pub const Config = struct {
    bootstrap_host: []const u8 = "127.0.0.1",
    bootstrap_port: u16 = 9092,
    bootstrap_endpoints: ?[]const Endpoint = null,
    request_timeout_ms: i32 = 30_000,
    connect_timeout_ms: i32 = 10_000,
};

pub const Cluster = struct {
    allocator: std.mem.Allocator,
    config: Config,
    pool: transport.pool.Pool,
    version_registry: versions.Registry,
    cache: metadata_cache.Cache,
    metadata_epoch_ms: i64 = 0,
    metadata_ttl_ms: i32 = 30_000,
    next_metadata_retry_ms: i64 = 0,
    metadata_retry_backoff_ms: i32 = 200,
    metadata_retry_backoff_cap_ms: i32 = 5_000,

    pub fn init(allocator: std.mem.Allocator, config: Config) Cluster {
        return .{
            .allocator = allocator,
            .config = config,
            .pool = transport.pool.Pool.init(allocator),
            .version_registry = versions.Registry.init(allocator),
            .cache = metadata_cache.Cache.init(allocator),
        };
    }

    pub fn deinit(self: *Cluster) void {
        self.pool.deinit();
        self.version_registry.deinit();
        self.cache.deinit();
    }

    pub fn statistics(self: *const Cluster) model.ClusterStatistics {
        const now = std.time.milliTimestamp();
        const age = if (self.metadata_epoch_ms == 0) -1 else now - self.metadata_epoch_ms;

        return .{
            .broker_count = self.cache.brokers.count(),
            .topic_count = self.cache.leaders.count(),
            .metadata_age_ms = age,
        };
    }

    pub fn leaderFor(self: *Cluster, topic: []const u8, partition: i32) errors.ClusterError!i32 {
        return router.leaderFor(&self.cache, topic, partition);
    }

    fn responseHasUsableRoutes(response: generated.metadata.Response) bool {
        for (response.topics) |t| {
            if (t.error_code != 0) {
                return true;
            }

            for (t.partitions) |p| {
                if (p.error_code == 0 and p.leader_id >= 0) {
                    return true;
                }
            }
        }

        return false;
    }

    pub fn refreshMetadata(self: *Cluster) errors.ClusterError!void {
        const now = std.time.milliTimestamp();
        if (now < self.next_metadata_retry_ms) {
            return error.Timeout;
        }

        errdefer self.next_metadata_retry_ms = std.time.milliTimestamp() + self.metadata_retry_backoff_ms;
        errdefer self.metadata_retry_backoff_ms = @min(self.metadata_retry_backoff_ms * 2, self.metadata_retry_backoff_cap_ms);

        try self.ensureNegotiatedVersions();
        const version = try self.version_registry.choose(.Metadata, 12);

        const conn = try self.getBootstrapConnection();
        var buf: [4096]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        const is_flexible = version >= 9;
        try encodeMetadataRequest(&e, conn.correlation_id, version, null, true);

        const response = try self.callAndDecodeMetadata(conn, is_flexible, e.written(), version);

        self.cache.apply(response) catch return error.Unexpected;
        if (!responseHasUsableRoutes(response)) {
            return error.ProtocolError;
        }

        if (self.cache.brokers.count() == 0) {
            return error.NoBrokers;
        }

        self.metadata_epoch_ms = std.time.milliTimestamp();
        self.next_metadata_retry_ms = 0;
        self.metadata_retry_backoff_ms = 200;
    }

    pub fn refreshTopicMetadata(self: *Cluster, topic: []const u8) errors.ClusterError!void {
        try self.ensureNegotiatedVersions();
        const version = try self.version_registry.choose(.Metadata, 12);

        var conn = try self.getBootstrapConnection();
        var buf: [4096]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        const is_flexible = version >= 9;

        var one_topic = [_]generated.metadata.Request.MetadataRequestTopic{
            .{
                .name = topic,
            },
        };
        try encodeMetadataRequest(&e, conn.correlation_id, version, &one_topic, false);

        const frame = conn.call(.Metadata, is_flexible, e.written()) catch |err| return errors.mapTransportError(err);
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
        if (is_flexible) {
            _ = header.ResponseHeaderV1.decode(&d) catch return error.ProtocolError;
        } else {
            _ = header.ResponseHeaderV0.decode(&d) catch return error.ProtocolError;
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const response = generated.metadata.Response.decode(arena.allocator(), &d, version) catch return error.ProtocolError;
        if (d.remaining() != 0) {
            return error.ProtocolError;
        }

        self.cache.applyTopicOnly(response) catch return error.Unexpected;
        if (!responseHasUsableRoutes(response)) {
            return error.ProtocolError;
        }

        if (self.cache.brokers.count() == 0) {
            return error.NoBrokers;
        }

        self.metadata_epoch_ms = std.time.milliTimestamp();
    }

    fn metadataExpired(self: *Cluster) bool {
        if (self.metadata_epoch_ms == 0) {
            return true;
        }

        return (std.time.milliTimestamp() - self.metadata_epoch_ms) > self.metadata_ttl_ms;
    }

    pub fn invalidateMetadata(self: *Cluster) void {
        self.metadata_epoch_ms = 0;
        self.cache.clear();
    }

    pub fn refreshMetadataNow(self: *Cluster) errors.ClusterError!void {
        self.metadata_epoch_ms = 0;
        try self.refreshMetadata();
    }

    pub fn brokerForTopicPartition(self: *Cluster, topic: []const u8, partition: i32) errors.ClusterError!model.Broker {
        if (self.metadataExpired()) {
            try self.refreshMetadata();
        }

        return router.brokerFor(&self.cache, topic, partition) catch |err| switch (err) {
            error.UnknownTopic, error.UnknownPartition, error.NoLeader => {
                try self.refreshTopicMetadata(topic);
                return router.brokerFor(&self.cache, topic, partition) catch return error.StaleMetadata;
            },
            else => return err,
        };
    }

    fn getConnectionForBroker(self: *Cluster, b: model.Broker) errors.ClusterError!*transport.connection.Connection {
        return self.pool.getReady(b.node_id, .{
            .host = b.host,
            .port = b.port,
            .connect_timeout_ms = self.config.connect_timeout_ms,
            .request_timeout_ms = self.config.request_timeout_ms,
        }) catch |err| return errors.mapTransportError(err);
    }

    pub fn connectionForTopicPartition(self: *Cluster, topic: []const u8, partition: i32) errors.ClusterError!*transport.connection.Connection {
        const broker = try self.brokerForTopicPartition(topic, partition);
        return self.getConnectionForBroker(broker) catch |err| switch (err) {
            error.ConnectionReset, error.ConnectionRefused, error.NetworkUnreachable => {
                self.invalidateMetadata();
                try self.refreshMetadata();
                const b2 = try self.brokerForTopicPartition(topic, partition);
                return self.getConnectionForBroker(b2);
            },
            else => return err,
        };
    }

    fn encodeRequestHeader(
        e: *codec.Encoder,
        api_key: i16,
        version: i16,
        correlation_id: i32,
        is_flexible: bool,
    ) errors.ClusterError!void {
        if (is_flexible) {
            const request_header = header.RequestHeaderV2{
                .api_key = api_key,
                .api_version = version,
                .correlation_id = correlation_id,
                .client_id = "samsa-cluster",
            };
            request_header.encode(e) catch return error.Unexpected;
        } else {
            const request_header = header.RequestHeaderV1{
                .api_key = api_key,
                .api_version = version,
                .correlation_id = correlation_id,
                .client_id = "samsa-cluster",
            };
            request_header.encode(e) catch return error.Unexpected;
        }
    }

    fn encodeMetadataRequest(
        e: *codec.Encoder,
        correlation_id: i32,
        version: i16,
        topics: ?[]const generated.metadata.Request.MetadataRequestTopic,
        allow_auto_create: bool,
    ) errors.ClusterError!void {
        const is_flexible = version >= 9;
        try encodeRequestHeader(e, @intFromEnum(generated.metadata.api_key), version, correlation_id, is_flexible);

        const request = generated.metadata.Request{
            .topics = topics,
            .allow_auto_topic_creation = allow_auto_create,
            .include_cluster_authorized_operations = false,
            .include_topic_authorized_operations = false,
        };

        request.encode(e, version) catch return error.Unexpected;
    }

    fn getBootstrapConnection(self: *Cluster) errors.ClusterError!*transport.connection.Connection {
        if (self.config.bootstrap_endpoints) |endpoints| {
            var last_err: errors.ClusterError = error.NoBrokers;
            for (endpoints) |endpoint| {
                const c = self.pool.getReady(bootstrapBrokerId(endpoint.host, endpoint.port), .{
                    .host = endpoint.host,
                    .port = endpoint.port,
                    .connect_timeout_ms = self.config.connect_timeout_ms,
                    .request_timeout_ms = self.config.request_timeout_ms,
                }) catch |err| {
                    last_err = errors.mapTransportError(err);
                    continue;
                };

                return c;
            }

            return last_err;
        }

        return self.pool.getReady(bootstrapBrokerId(self.config.bootstrap_host, self.config.bootstrap_port), .{
            .host = self.config.bootstrap_host,
            .port = self.config.bootstrap_port,
            .connect_timeout_ms = self.config.connect_timeout_ms,
            .request_timeout_ms = self.config.request_timeout_ms,
        }) catch |err| {
            var last = errors.mapTransportError(err);
            var it = self.cache.brokers.iterator();
            while (it.next()) |entry| {
                const b = entry.value_ptr.*;
                const conn = self.pool.getReady(b.node_id, .{
                    .host = b.host,
                    .port = b.port,
                    .connect_timeout_ms = self.config.connect_timeout_ms,
                    .request_timeout_ms = self.config.request_timeout_ms,
                }) catch |e| {
                    last = errors.mapTransportError(e);
                    continue;
                };

                return conn;
            }

            return last;
        };
    }

    fn sendApiVersions(self: *Cluster, conn: *transport.connection.Connection, version: i16) errors.ClusterError!generated.api_versions.Response {
        var buf: [2048]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        try encodeRequestHeader(&e, @intFromEnum(generated.api_versions.api_key), version, conn.correlation_id, version >= 3);
        const request = generated.api_versions.Request{
            .client_software_name = "samsa",
            .client_software_version = "0.1.0",
        };
        request.encode(&e, version) catch return error.Unexpected;

        const frame = conn.call(.ApiVersions, version >= 3, e.written()) catch |err| return errors.mapTransportError(err);
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
        _ = header.ResponseHeaderV0.decode(&d) catch return error.ProtocolError;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const response = generated.api_versions.Response.decode(arena.allocator(), &d, version) catch return error.ProtocolError;
        if (d.remaining() != 0) {
            return error.ProtocolError;
        }

        self.version_registry.updateFromApiVersions(response) catch return error.Unexpected;
        return response;
    }

    fn callAndDecodeMetadata(
        self: *Cluster,
        conn: *transport.connection.Connection,
        is_flexible: bool,
        payload: []const u8,
        version: i16,
    ) errors.ClusterError!generated.metadata.Response {
        const frame = conn.call(.Metadata, is_flexible, payload) catch |err| return errors.mapTransportError(err);
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
        if (is_flexible) {
            _ = header.ResponseHeaderV1.decode(&d) catch return error.ProtocolError;
        } else {
            _ = header.ResponseHeaderV0.decode(&d) catch return error.ProtocolError;
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const response = generated.metadata.Response.decode(arena.allocator(), &d, version) catch return error.ProtocolError;
        if (d.remaining() != 0) {
            return error.ProtocolError;
        }

        return response;
    }

    fn sendApiVersionsCompact(self: *Cluster, conn: *transport.connection.Connection, version: i16) errors.ClusterError!generated.api_versions.Response {
        var buf: [2048]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        try encodeRequestHeader(&e, @intFromEnum(generated.api_versions.api_key), version, conn.correlation_id, version >= 3);
        const request = generated.api_versions.Request{
            .client_software_name = "samsa",
            .client_software_version = "0.1.0",
        };
        request.encode(&e, version) catch return error.Unexpected;

        const frame = conn.call(.ApiVersions, versions >= 3, e.written()) catch |err| return errors.mapTransportError(err);
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
        _ = header.ResponseHeaderV0.decode(&d) catch return error.ProtocolError;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const parsed = generated.api_versions.Response.decode(arena.allocator(), &d, version) catch |err| switch (err) {
            error.EndOfStream,
            error.InvalidArrayLength,
            error.VarintTooLong,
            error.NegativeLength,
            error.InvalidLength,
            error.ProtocolError,
            => {
                var d0 = codec.Decoder.init(frame);
                _ = header.ResponseHeaderV0.decode(&d0) catch return error.ProtocolError;
                generated.api_versions.Response.decode(arena.allocator(), &d0, 0) catch return error.ProtocolError;
            },
            else => return error.ProtocolError,
        };

        self.version_registry.updateFromApiVersions(parsed) catch return error.Unexpected;
        return parsed;
    }

    fn ensureNegotiatedVersions(self: *Cluster) errors.ClusterError!void {
        if (self.version_registry.has(.Metadata)) {
            return;
        }

        const conn = try self.getBootstrapConnection();
        const response_v4 = try self.sendApiVersionsCompact(conn, 4);
        if (response_v4.error_code == 35) {
            const response_v2 = try self.sendApiVersionsCompact(conn, 2);
            if (response_v2.error_code != 0) {
                return error.ProtocolError;
            }

            return;
        }

        if (response_v4.error_code != 0) {
            return error.ProtocolError;
        }
    }
};

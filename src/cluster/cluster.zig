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

    fn responseHasTopicErrors(response: generated.metadata.Response) bool {
        for (response.topics) |t| {
            if (t.error_code != 0) {
                return true;
            }

            for (t.partitions) |p| {
                if (p.error_code != 0) {
                    return true;
                }
            }
        }

        return false;
    }

    pub fn refreshMetadata(self: *Cluster) !void {
        const now = std.time.milliTimestamp();
        if (now < self.next_metadata_retry_ms) {
            return error.Timeout;
        }

        try self.ensureNegotiatedVersions();
        const version = try self.version_registry.choose(.Metadata, 10);

        var conn = try self.getBootstrapConnection();
        var buf: [4096]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        const is_flexible = version >= 9;
        try encodeRequestHeader(&e, @intFromEnum(generated.metadata.api_key), version, conn.correlation_id, is_flexible);

        const request = generated.metadata.Request{
            .topics = null,
            .allow_auto_topic_creation = true,
            .include_cluster_authorized_operations = false,
            .include_topic_authorized_operations = false,
        };
        try request.encode(&e, version);

        const frame = try conn.call(.Metadata, is_flexible, e.written());
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
        if (is_flexible) {
            _ = try header.ResponseHeaderV1.decode(&d);
        } else {
            _ = try header.ResponseHeaderV0.decode(&d);
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const response = generated.metadata.Response.decode(arena.allocator(), &d, version) catch return error.ProtocolError;
        if (d.remaining() != 0) {
            return error.ProtocolError;
        }

        if (responseHasTopicErrors(response)) {
            self.cache.apply(response) catch return error.Unexpected;
            return error.ProtocolError;
        }

        self.cache.apply(response) catch return error.Unexpected;
        if (self.cache.brokers.count() == 0) {
            return error.NoBrokers;
        }

        self.metadata_epoch_ms = std.time.milliTimestamp();
        self.next_metadata_retry_ms = 0;
    }

    pub fn refreshTopicMetadata(self: *Cluster, topic: []const u8) errors.ClusterError!void {
        try self.ensureNegotiatedVersions();
        const version = try self.version_registry.choose(.Metadata, 10);

        var conn = try self.getBootstrapConnection();
        var buf: [4096]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        const is_flexible = version >= 9;

        var one_topic = [_]generated.metadata.Request.MetadataRequestTopic{
            .{
                .name = topic,
            },
        };
        const request = generated.metadata.Request{
            .topics = &one_topic,
            .allow_auto_topic_creation = false,
            .include_cluster_authorized_operations = false,
            .include_topic_authorized_operations = false,
        };
        try request.encode(&e, version);

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

        if (responseHasTopicErrors(response)) {
            self.cache.apply(response) catch return error.Unexpected;
            return error.ProtocolError;
        }

        self.cache.applyTopicOnly(response) catch return error.Unexpected;
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
                return router.brokerFor(&self.cache, topic, partition);
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
            try request_header.encode(&e);
        } else {
            const request_header = header.RequestHeaderV1{
                .api_key = api_key,
                .api_version = version,
                .correlation_id = correlation_id,
                .client_id = "samsa-cluster",
            };
            try request_header.encode(&e);
        }
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
        }) catch |err| return errors.mapTransportError(err);
    }

    fn sendApiVersions(self: *Cluster, conn: *transport.connection.Connection, version: i16) errors.ClusterError!generated.api_versions.Response {
        var buf: [2048]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        try encodeRequestHeader(&e, @intFromEnum(generated.metadata.api_key), version, conn.correlation_id, version >= 3);
        const request = generated.api_versions.Request{
            .client_software_name = "samsa",
            .client_software_version = "0.1.0",
        };
        try request.encode(&e, version);

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

    fn ensureNegotiatedVersions(self: *Cluster) errors.ClusterError!void {
        if (self.version_registry.has(.Metadata)) {
            return;
        }

        var conn = try self.getBootstrapConnection();

        const response_v4 = try self.sendApiVersions(&conn, 4);
        if (response_v4.error_code == 35) {
            const response_v2 = try self.sendApiVersions(&conn, 2);
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

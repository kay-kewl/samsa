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
    return @intCast(host.final() & 0x7fff_ffff);
}

pub const Config = struct {
    bootstrap_host: []const u8,
    bootstrap_port: u16,
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

    pub fn leaderFor(self: *Cluster, topic: []const u8, partition: i32) errors.ClusterError!i32 {
        return router.leaderFor(&self.cache, topic, partition);
    }

    pub fn refreshMetadata(self: *Cluster) !void {
        try self.ensureNegotiatedVersions();
        const v = try self.version_registry.choose(.Metadata, 10);

        var conn = try self.getBootstrapConnection();
        var buf: [4096]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        const is_flexible = v >= 9;
        if (is_flexible) {
            const request_header = header.RequestHeaderV2{
                .api_key = @intFromEnum(generated.metadata.api_key),
                .api_version = v,
                .correlation_id = conn.correlation_id,
                .client_id = "samsa-cluster",
            };
            try request_header.encode(&e);
        } else {
            const request_header = header.RequestHeaderV1{
                .api_key = @intFromEnum(generated.metadata.api_key),
                .api_version = v,
                .correlation_id = conn.correlation_id,
                .client_id = "samsa-cluster",
            };
            try request_header.encode(&e);
        }

        const request = generated.metadata.Request{
            .topics = null,
            .allow_auto_topic_creation = true,
            .include_cluster_authorized_operations = false,
            .include_topic_authorized_operations = false,
        };
        try request.encode(&e, v);

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

        const response = generated.metadata.Response.decode(arena.allocator(), &d, v) catch return error.ProtocolError;
        if (d.remaining() != 0) {
            return error.ProtocolError;
        }

        self.cache.apply(response) catch return error.Unexpected;
        if (self.cache.brokers.count() == 0) {
            return error.NoBrokers;
        }

        self.metadata_epoch_ms = std.time.milliTimestamp();
    }

    pub fn refreshTopicMetadata(self: *Cluster, topic: []const u8) errors.ClusterError!void {
        try self.ensureNegotiatedVersions();
        const version = try self.version_registry.choose(.Metadata, 10);

        var conn = try self.getBootstrapConnection();
        var buf: [4096]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        const is_flexible = version >= 9;
        if (is_flexible) {
            const request_header = header.RequestHeaderV2{
                .api_key = @intFromEnum(generated.metadata.api_key),
                .api_version = version,
                .correlation_id = conn.correlation_id,
                .client_id = "samsa-cluster",
            };
            try request_header.encode(&e);
        } else {
            const request_header = header.RequestHeaderV1{
                .api_key = @intFromEnum(generated.metadata.api_key),
                .api_version = version,
                .correlation_id = conn.correlation_id,
                .client_id = "samsa-cluster",
            };
            try request_header.encode(&e);
        }

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

        self.cache.apply(response) catch return error.Unexpected;
    }

    fn metadataExpired(self: *Cluster) bool {
        if (self.metadata_epoch_ms == 0) {
            return true;
        }

        return (std.time.milliTimestamp() - self.metadata_epoch_ms) > self.metadata_ttl_ms;
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

    fn getBootstrapConnection(self: *Cluster) errors.ClusterError!*transport.connection.Connection {
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

        if (version >= 3) {
            const request_header = header.RequestHeaderV2{
                .api_key = @intFromEnum(generated.api_versions.api_key),
                .api_version = version,
                .correlation_id = conn.correlation_id,
                .client_id = "samsa-cluster",
            };
            try request_header.encode(&e);
        } else {
            const request_header = header.RequestHeaderV1{
                .api_key = @intFromEnum(generated.api_versions.api_key),
                .api_version = version,
                .correlation_id = conn.correlation_id,
                .client_id = "samsa-cluster",
            };
            try request_header.encode(&e);
        }

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

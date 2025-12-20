const std = @import("std");
const transport = @import("../transport/module.zig");
const generated = @import("../generated/module.zig");
const header = @import("../protocol/header.zig");
const codec = @import("../protocol/codec.zig");
const types = @import("../protocol/types.zig");
const errors = @import("errors.zig");
const versions = @import("versions.zig");
const metadata_cache = @import("metadata_cache.zig");
const router = @import("router.zig");

pub const Config = struct {
    bootstrap_host: []const u8,
    boostrap_port: u16,
    request_timeout_ms: i32 = 30_000,
    connect_timeout_ms: i32 = 10_000,
};

pub const Cluster = struct {
    allocator: std.mem.Allocator,
    config: Config,
    pool: transport.pool.Pool,
    version_registry: versions.Registry,
    cache: metadata_cache.Cache,

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

        var conn = try self.pool.getReady(0, .{
            .host = self.config.bootstrap_host,
            .port = self.config.boostrap_port,
            .connect_timeout_ms = self.config.connect_timeout_ms,
            .request_timeout_ms = self.config.request_timeout_ms,
        });

        var buf: [4096]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        const is_flexible = v >= 9;
        if (is_flexible) {
            const request_header = try header.RequestHeaderV2{
                .api_key = @intFromEnum(generated.metadata.api_key),
                .api_version = v,
                .correlation_id = conn.correlation_id,
                .client_id = "samsa-cluster",
            };
            request_header.encode(&e);
        } else {
            const request_header = try header.RequestHeaderV1{
                .api_key = @intFromEnum(generated.metadata.api_key),
                .api_version = v,
                .correlation_id = conn.correlation_id,
                .client_id = "samsa-cluster",
            };
            request_header.encode(&e);
        }

        const request = try generated.metadata.Request{
            .topics = null,
            .allow_auto_topic_creation = true,
            .include_cluster_authorized_operations = false,
            .include_topic_authorized_operations = false,
        };
        request.encode(&e, v);

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

        const response = try generated.metadata.Response.decode(arena.allocator(), &d, v);
        try self.cache.apply(response);
    }
};

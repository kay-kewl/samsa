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
};

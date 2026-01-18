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

fn deadlineMsFromNow(timeout_ms: i32) i64 {
    return std.time.milliTimestamp() + timeout_ms;
}

fn remainingMs(deadline_ms: i64) i32 {
    const now = std.time.milliTimestamp();
    const remaining = deadline_ms - now;
    if (remaining <= 0) {
        return 0;
    }

    if (remaining > std.math.maxInt(i32)) {
        return std.math.maxInt(i32);
    }

    return @intCast(remaining);
}

const MetadataRefreshScope = enum {
    all_topics,
    brokers_only,
    one_topic,
};

pub const Endpoint = struct {
    host: []const u8,
    port: u16,
};

pub const HostnameRewrite = struct {
    from_host: []const u8,
    from_port: u16,
    to_host: []const u8,
    to_port: u16,
};

pub const Config = struct {
    bootstrap_host: []const u8 = "127.0.0.1",
    bootstrap_port: u16 = 9092,
    bootstrap_endpoints: ?[]const Endpoint = null,
    request_timeout_ms: i32 = 30_000,
    connect_timeout_ms: i32 = 10_000,
    max_frame_bytes: usize = 16 * 1024 * 1024,
    tcp_nodelay: bool = false,
    enable_tcp_keepalive: bool = false,
    metadata_recovery_rebootstrap_trigger_ms: i32 = 300_000,
    metadata_recovery_strategy_rebootstrap: bool = true,
    hostname_rewrite: ?HostnameRewrite = null,
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
    metadata_last_success_ms: i64 = 0,
    metadata_refresh_backoff_ms: i32 = 100,
    metadata_refresh_inflight: bool = false,
    metadata_refresh_not_before_ms: i64 = 0,
    metadata_refresh_attempts: u64 = 0,
    metadata_refresh_failures: u64 = 0,
    metadata_rebootstrap_count: u64 = 0,

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
        const retry_in = if (self.next_metadata_retry_ms <= now) 0 else self.next_metadata_retry_ms - now;

        return .{
            .broker_count = self.cache.brokers.count(),
            .topic_count = self.cache.leaders.count(),
            .metadata_age_ms = age,
            .controller_id = self.cache.controller_id,
            .has_cluster_id = self.cache.cluster_id != null,
            .next_metadata_retry_in_ms = retry_in,
            .metadata_refresh_inflight = self.metadata_refresh_inflight,
            .metadata_refresh_attempts = self.metadata_refresh_attempts,
            .metadata_refresh_failures = self.metadata_refresh_failures,
            .metadata_rebootstrap_count = self.metadata_rebootstrap_count,
        };
    }

    pub fn leaderFor(self: *Cluster, topic: []const u8, partition: i32) errors.ClusterError!i32 {
        return router.leaderFor(&self.cache, topic, partition);
    }

    pub fn leaderEpochFor(self: *Cluster, topic: []const u8, partition: i32) ?i32 {
        return self.cache.leaderEpochFor(topic, partition);
    }

    pub fn clearPartitionLeaderEpoch(self: *Cluster, topic: []const u8, partition: i32) void {
        _ = self.cache.clearLeaderEpoch(topic, partition);
    }

    fn rewriteEndpoint(self: *const Cluster, host: []const u8, port: u16) Endpoint {
        if (self.config.hostname_rewrite) |rw| {
            if (rw.from_port == port and std.mem.eql(u8, rw.from_host, host)) {
                return .{
                    .host = rw.to_host,
                    .port = rw.to_port,
                };
            }
        }

        return .{
            .host = host,
            .port = port,
        };
    }

    pub fn topicGeneration(self: *Cluster, topic: []const u8) ?u64 {
        return self.cache.topicGeneration(topic);
    }

    fn responseHasUsableRoutes(scope: MetadataRefreshScope, response: generated.metadata.Response) bool {
        if (scope == .brokers_only) {
            return response.brokers.len > 0;
        }

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

    fn waitForMetadataRefreshSlot(self: *Cluster, deadline_ms: i64) errors.ClusterError!void {
        while (true) {
            const now = std.time.milliTimestamp();
            const blocked = self.metadata_refresh_inflight or
                now < self.metadata_refresh_not_before_ms or
                now < self.next_metadata_retry_ms;

            if (!blocked) {
                return;
            }

            const remaining = remainingMs(deadline_ms);
            if (remaining <= 0) {
                return error.Timeout;
            }

            var sleep_ms: i32 = 10;
            if (now < self.metadata_refresh_not_before_ms) {
                const wait_until_not_before: i32 = @intCast(self.metadata_refresh_not_before_ms - now);
                sleep_ms = @min(sleep_ms, wait_until_not_before);
            }

            if (now < self.next_metadata_retry_ms) {
                const wait_until_retry: i32 = @intCast(self.next_metadata_retry_ms - now);
                sleep_ms = @min(sleep_ms, wait_until_retry);
            }

            sleep_ms = @max(@as(i32, 1), @min(sleep_ms, remaining));
            std.Thread.sleep(@as(u64, @intCast(sleep_ms)) * std.time.ns_per_ms);
        }
    }

    fn refreshMetadataScoped(
        self: *Cluster,
        scope: MetadataRefreshScope,
        topic: ?[]const u8,
        allow_auto_create: bool,
    ) errors.ClusterError!void {
        try self.ensureNegotiatedVersions();
        const version = try self.version_registry.choose(.Metadata);
        const conn = try self.getBootstrapConnection();

        var buf: [4096]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        var one_topic = [_]generated.metadata.Request.MetadataRequestTopic{
            .{
                .name = topic orelse "",
            },
        };

        const empty_topics = [_]generated.metadata.Request.MetadataRequestTopic{};
        const topics_ptr: ?[]const generated.metadata.Request.MetadataRequestTopic = switch (scope) {
            .all_topics => null,
            .brokers_only => &empty_topics,
            .one_topic => &one_topic,
        };

        const is_flexible = version >= 9;
        try encodeMetadataRequest(&e, conn.correlation_id, version, topics_ptr, allow_auto_create);
        try self.callAndApplyMetadata(conn, is_flexible, e.written(), version, scope);
    }

    pub fn refreshMetadata(self: *Cluster) errors.ClusterError!void {
        const deadline_ms = deadlineMsFromNow(self.config.request_timeout_ms);
        try self.waitForMetadataRefreshSlot(deadline_ms);

        self.metadata_refresh_attempts += 1;
        errdefer self.metadata_refresh_failures += 1;

        self.metadata_refresh_inflight = true;
        defer self.metadata_refresh_inflight = false;
        defer self.metadata_refresh_not_before_ms = std.time.milliTimestamp() + self.metadata_refresh_backoff_ms;

        errdefer self.next_metadata_retry_ms = std.time.milliTimestamp() + self.metadata_retry_backoff_ms;
        errdefer self.metadata_retry_backoff_ms = @min(self.metadata_retry_backoff_ms * 2, self.metadata_retry_backoff_cap_ms);

        try self.refreshMetadataScoped(.all_topics, null, true);
        self.adoptBootstrapConnectionIfPossible();

        self.metadata_epoch_ms = std.time.milliTimestamp();
        self.metadata_last_success_ms = self.metadata_epoch_ms;
        self.next_metadata_retry_ms = 0;
        self.metadata_retry_backoff_ms = 200;
    }

    pub fn refreshTopicMetadata(self: *Cluster, topic: []const u8) errors.ClusterError!void {
        const deadline_ms = deadlineMsFromNow(self.config.request_timeout_ms);
        try self.waitForMetadataRefreshSlot(deadline_ms);

        self.metadata_refresh_attempts += 1;
        errdefer self.metadata_refresh_failures += 1;

        self.metadata_refresh_inflight = true;
        defer self.metadata_refresh_inflight = false;
        defer self.metadata_refresh_not_before_ms = std.time.milliTimestamp() + self.metadata_refresh_backoff_ms;

        errdefer self.next_metadata_retry_ms = std.time.milliTimestamp() + self.metadata_retry_backoff_ms;
        errdefer self.metadata_retry_backoff_ms = @min(self.metadata_retry_backoff_ms * 2, self.metadata_retry_backoff_cap_ms);

        try self.refreshMetadataScoped(.one_topic, topic, false);
        self.adoptBootstrapConnectionIfPossible();

        self.metadata_epoch_ms = std.time.milliTimestamp();
        self.metadata_last_success_ms = self.metadata_epoch_ms;
        self.next_metadata_retry_ms = 0;
        self.metadata_retry_backoff_ms = 200;
    }

    pub fn refreshBrokersOnlyMetadata(self: *Cluster) errors.ClusterError!void {
        const deadline_ms = deadlineMsFromNow(self.config.request_timeout_ms);
        try self.waitForMetadataRefreshSlot(deadline_ms);

        self.metadata_refresh_attempts += 1;
        errdefer self.metadata_refresh_failures += 1;

        self.metadata_refresh_inflight = true;
        defer self.metadata_refresh_inflight = false;
        defer self.metadata_refresh_not_before_ms = std.time.milliTimestamp() + self.metadata_refresh_backoff_ms;

        errdefer self.next_metadata_retry_ms = std.time.milliTimestamp() + self.metadata_retry_backoff_ms;
        errdefer self.metadata_retry_backoff_ms = @min(self.metadata_retry_backoff_ms * 2, self.metadata_retry_backoff_cap_ms);

        try self.refreshMetadataScoped(.brokers_only, null, false);
        self.adoptBootstrapConnectionIfPossible();

        self.metadata_epoch_ms = std.time.milliTimestamp();
        self.metadata_last_success_ms = self.metadata_epoch_ms;
        self.next_metadata_retry_ms = 0;
        self.metadata_retry_backoff_ms = 200;
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

    pub fn triggerRebootstrap(self: *Cluster) void {
        self.metadata_rebootstrap_count += 1;
        self.invalidateMetadata();
        self.pool.closeAll();
        self.version_registry.reset();

        self.next_metadata_retry_ms = 0;
        self.metadata_refresh_not_before_ms = 0;
        self.metadata_retry_backoff_ms = 200;
    }

    pub fn refreshMetadataNow(self: *Cluster) errors.ClusterError!void {
        if (self.metadata_refresh_inflight) {
            return error.MetadataUnavailable;
        }

        self.metadata_epoch_ms = 0;
        self.next_metadata_retry_ms = 0;
        self.metadata_refresh_not_before_ms = 0;
        self.metadata_retry_backoff_ms = 200;

        try self.refreshMetadata();
    }

    fn shouldTopicRefreshForRouteError(err: errors.ClusterError) bool {
        return switch (err) {
            error.UnknownTopic, error.UnknownPartition, error.NoLeader => true,
            else => false,
        };
    }

    pub fn brokerForTopicPartition(self: *Cluster, topic: []const u8, partition: i32) errors.ClusterError!model.Broker {
        if (self.metadataExpired()) {
            if (self.metadata_refresh_inflight) {
                if (self.cache.brokers.count() == 0) {
                    return error.MetadataUnavailable;
                }
            } else {
                self.refreshMetadata() catch |err| switch (err) {
                    error.MetadataUnavailable => {
                        if (self.cache.brokers.count() == 0) {
                            return err;
                        }
                    },
                    else => return err,
                };
            }
        }

        return router.brokerFor(&self.cache, topic, partition) catch |err| {
            if (shouldTopicRefreshForRouteError(err)) {
                try self.refreshTopicMetadata(topic);
                return router.brokerFor(&self.cache, topic, partition) catch return error.StaleMetadata;
            }

            return err;
        };
    }

    fn getConnectionForBroker(self: *Cluster, b: model.Broker) errors.ClusterError!*transport.connection.Connection {
        const endpoint = self.rewriteEndpoint(b.host, b.port);

        return self.pool.getReady(b.node_id, .{
            .host = endpoint.host,
            .port = endpoint.port,
            .connect_timeout_ms = self.config.connect_timeout_ms,
            .request_timeout_ms = self.config.request_timeout_ms,
        }) catch |err| return errors.mapTransportError(err);
    }

    pub fn connectionForTopicPartition(self: *Cluster, topic: []const u8, partition: i32) errors.ClusterError!*transport.connection.Connection {
        const broker = try self.brokerForTopicPartition(topic, partition);
        return self.getConnectionForBroker(broker) catch |err| switch (err) {
            error.ConnectionReset, error.ConnectionRefused, error.NetworkUnreachable => {
                self.pool.remove(broker.node_id);
                self.clearPartitionLeaderEpoch(topic, partition);

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
                const rewritten = self.rewriteEndpoint(endpoint.host, endpoint.port);

                const c = self.pool.getReady(bootstrapBrokerId(endpoint.host, endpoint.port), .{
                    .host = rewritten.host,
                    .port = rewritten.port,
                    .connect_timeout_ms = self.config.connect_timeout_ms,
                    .request_timeout_ms = self.config.request_timeout_ms,
                    .max_frame_bytes = self.config.max_frame_bytes,
                    .tcp_nodelay = self.config.tcp_nodelay,
                    .enable_tcp_keepalive = self.config.enable_tcp_keepalive,
                }) catch |err| {
                    last_err = errors.mapTransportError(err);
                    continue;
                };

                return c;
            }

            return self.fallbackToKnownBrokersOrRebootstrap(last_err);
        }

        const rewritten_bootstrap = self.rewriteEndpoint(self.config.bootstrap_host, self.config.bootstrap_port);

        return self.pool.getReady(bootstrapBrokerId(self.config.bootstrap_host, self.config.bootstrap_port), .{
            .host = rewritten_bootstrap.host,
            .port = rewritten_bootstrap.port,
            .connect_timeout_ms = self.config.connect_timeout_ms,
            .request_timeout_ms = self.config.request_timeout_ms,
            .max_frame_bytes = self.config.max_frame_bytes,
            .tcp_nodelay = self.config.tcp_nodelay,
            .enable_tcp_keepalive = self.config.enable_tcp_keepalive,
        }) catch |err| {
            return self.fallbackToKnownBrokersOrRebootstrap(errors.mapTransportError(err));
        };
    }

    fn fallbackToKnownBrokersOrRebootstrap(
        self: *Cluster,
        initial_last: errors.ClusterError,
    ) errors.ClusterError!*transport.connection.Connection {
        var last = initial_last;

        var it = self.cache.brokers.iterator();
        while (it.next()) |entry| {
            const b = entry.value_ptr.*;
            const endpoint = self.rewriteEndpoint(b.host, b.port);
            const conn = self.pool.getReady(b.node_id, .{
                .host = endpoint.host,
                .port = endpoint.port,
                .connect_timeout_ms = self.config.connect_timeout_ms,
                .request_timeout_ms = self.config.request_timeout_ms,
                .max_frame_bytes = self.config.max_frame_bytes,
                .tcp_nodelay = self.config.tcp_nodelay,
                .enable_tcp_keepalive = self.config.enable_tcp_keepalive,
            }) catch |e| {
                last = errors.mapTransportError(e);
                continue;
            };

            return conn;
        }

        const now = std.time.milliTimestamp();
        const since_success = if (self.metadata_last_success_ms == 0) now else now - self.metadata_last_success_ms;
        if (self.config.metadata_recovery_strategy_rebootstrap and since_success >= self.config.metadata_recovery_rebootstrap_trigger_ms) {
            self.triggerRebootstrap();
        }

        return last;
    }

    fn adoptBootstrapConnectionIfPossible(self: *Cluster) void {
        const adoptOne = struct {
            fn run(cluster: *Cluster, host: []const u8, port: u16) void {
                const boot_id = bootstrapBrokerId(host, port);
                if (!cluster.pool.map.contains(boot_id)) {
                    return;
                }

                const rewritten = cluster.rewriteEndpoint(host, port);

                var it = cluster.cache.brokers.iterator();
                while (it.next()) |entry| {
                    const b = entry.value_ptr.*;
                    if (b.port == rewritten.port and std.mem.eql(u8, b.host, rewritten.host)) {
                        cluster.pool.rekey(boot_id, b.node_id) catch {};
                        return;
                    }
                }
            }
        }.run;

        if (self.config.bootstrap_endpoints) |endpoints| {
            for (endpoints) |endpoint| {
                adoptOne(self, endpoint.host, endpoint.port);
            }
        } else {
            adoptOne(self, self.config.bootstrap_host, self.config.bootstrap_port);
        }
    }

    fn prunePoolToKnownBrokers(self: *Cluster) void {
        var stale_ids: std.ArrayList(i32) = .empty;
        defer stale_ids.deinit(self.allocator);

        var it = self.pool.map.iterator();
        while (it.next()) |entry| {
            const broker_id = entry.key_ptr.*;
            if (!self.cache.brokers.contains(broker_id)) {
                stale_ids.append(self.allocator, broker_id) catch {};
            }
        }

        for (stale_ids.items) |id| {
            self.pool.remove(id);
        }
    }

    fn callAndApplyMetadata(
        self: *Cluster,
        conn: *transport.connection.Connection,
        is_flexible: bool,
        payload: []const u8,
        version: i16,
        scope: MetadataRefreshScope,
    ) errors.ClusterError!void {
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

        if (scope == .one_topic) {
            self.cache.applyTopicOnly(response) catch return error.Unexpected;
        } else if (scope == .brokers_only) {
            self.cache.applyBrokersOnly(response) catch return error.Unexpected;
            self.prunePoolToKnownBrokers();
        } else {
            self.cache.apply(response) catch return error.Unexpected;
            self.prunePoolToKnownBrokers();
        }

        if (!responseHasUsableRoutes(scope, response)) {
            return error.ProtocolError;
        }

        if (self.cache.brokers.count() == 0) {
            return error.NoBrokers;
        }
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

        const frame = conn.call(.ApiVersions, version >= 3, e.written()) catch |err| return errors.mapTransportError(err);
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
        _ = header.ResponseHeaderV0.decode(&d) catch return error.ProtocolError;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var used_fallback = false;

        const parsed = generated.api_versions.Response.decode(arena.allocator(), &d, version) catch |err| switch (err) {
            error.EndOfStream,
            error.InvalidLength,
            error.Overflow,
            error.InvalidVariant,
            => fallback: {
                used_fallback = true;

                var d0 = codec.Decoder.init(frame);
                _ = header.ResponseHeaderV0.decode(&d0) catch return error.ProtocolError;
                if (d0.remaining() == 0) {
                    return error.ProtocolError;
                }

                const parsed0 = generated.api_versions.Response.decode(arena.allocator(), &d0, 0) catch return error.ProtocolError;
                if (d0.remaining() != 0) {
                    return error.ProtocolError;
                }

                break :fallback parsed0;
            },
            else => return error.ProtocolError,
        };

        if (!used_fallback and d.remaining() != 0) {
            return error.ProtocolError;
        }

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

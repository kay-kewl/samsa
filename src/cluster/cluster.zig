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
const protocol_limits = @import("../protocol/limits.zig");

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

fn jitteredDelayMs(max_ms: i32) i32 {
    if (max_ms <= 0) {
        return 0;
    }

    if (max_ms == 1) {
        return 1;
    }

    return @as(i32, @intCast(std.crypto.random.intRangeAtMost(u32, 0, @as(u32, @intCast(max_ms)))));
}

fn scheduleJitteredMs(max_ms: i32) i64 {
    const delay = @max(@as(i32, 1), jitteredDelayMs(max_ms));
    return std.time.milliTimestamp() + delay;
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
    max_total_connections: ?usize = null,
    tcp_nodelay: bool = false,
    enable_tcp_keepalive: bool = false,
    protocol_limits: protocol_limits.Limits = .{},

    metadata_ttl_ms: i32 = 60_000,
    metadata_retry_backoff_ms: i32 = 200,
    metadata_retry_backoff_cap_ms: i32 = 5_000,
    metadata_refresh_backoff_ms: i32 = 100,
    metadata_recovery_rebootstrap_trigger_ms: i32 = 300_000,
    metadata_recovery_strategy_rebootstrap: bool = true,

    hostname_rewrite: ?HostnameRewrite = null,
    client_id: []const u8 = "samsa",
    client_software_name: []const u8 = "samsa",
    client_software_version: []const u8 = "0.1.0",

    pub fn validate(self: @This()) !void {
        if (self.request_timeout_ms <= 0 or self.connect_timeout_ms <= 0) {
            return error.InvalidConfiguration;
        }

        if (self.max_frame_bytes == 0 or self.max_frame_bytes > std.math.maxInt(i32)) {
            return error.InvalidConfiguration;
        }

        if (self.metadata_retry_backoff_ms <= 0 or
            self.metadata_retry_backoff_cap_ms <= 0 or
            self.metadata_refresh_backoff_ms <= 0 or
            self.metadata_retry_backoff_cap_ms < self.metadata_retry_backoff_ms)
        {
            return error.InvalidConfiguration;
        }

        if (self.protocol_limits.max_string_bytes == 0 or
            self.protocol_limits.max_bytes_field_bytes == 0 or
            self.protocol_limits.max_array_elements == 0 or
            self.protocol_limits.max_tagged_field_bytes == 0 or
            self.protocol_limits.decode_depth_max == 0)
        {
            return error.InvalidConfiguration;
        }

        if (self.client_id.len == 0 or self.client_software_name.len == 0 or self.client_software_version.len == 0) {
            return error.InvalidConfiguration;
        }

        if (self.bootstrap_endpoints) |endpoints| {
            if (endpoints.len == 0) {
                return error.InvalidConfiguration;
            }

            for (endpoints) |ep| {
                if (ep.host.len == 0 or ep.port == 0) {
                    return error.InvalidConfiguration;
                }
            }
        } else {
            if (self.bootstrap_host.len == 0 or self.bootstrap_port == 0) {
                return error.InvalidConfiguration;
            }
        }
    }
};

pub const Cluster = struct {
    allocator: std.mem.Allocator,
    config: Config,
    pool: transport.pool.Pool,
    version_registry: versions.Registry,
    broker_version_ranges: std.AutoHashMap(i32, std.AutoHashMap(i16, versions.Range)),
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
            .pool = transport.pool.Pool.initWithLimit(allocator, config.max_total_connections),
            .version_registry = versions.Registry.init(allocator),
            .broker_version_ranges = std.AutoHashMap(i32, std.AutoHashMap(i16, versions.Range)).init(allocator),
            .cache = metadata_cache.Cache.init(allocator),
            .metadata_retry_backoff_ms = config.metadata_retry_backoff_ms,
            .metadata_retry_backoff_cap_ms = config.metadata_retry_backoff_cap_ms,
            .metadata_refresh_backoff_ms = config.metadata_refresh_backoff_ms,
        };
    }

    fn clearBrokerVersionRanges(self: *Cluster) void {
        var it = self.broker_version_ranges.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }

        self.broker_version_ranges.clearRetainingCapacity();
    }

    pub fn pruneBrokerVersionRangesToKnownBrokers(self: *Cluster) void {
        var stale_ids: std.ArrayList(i32) = .empty;
        defer stale_ids.deinit(self.allocator);

        var it = self.broker_version_ranges.iterator();
        while (it.next()) |entry| {
            if (!self.cache.brokers.contains(entry.key_ptr.*)) {
                stale_ids.append(self.allocator, entry.key_ptr.*) catch {};
            }
        }

        for (stale_ids.items) |id| {
            if (self.broker_version_ranges.fetchRemove(id)) |old| {
                var old_ranges = old.value;
                old_ranges.deinit();
            }
        }
    }

    pub fn deinit(self: *Cluster) void {
        self.clearBrokerVersionRanges();
        self.broker_version_ranges.deinit();
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
        deadline_ms: i64,
    ) errors.ClusterError!void {
        try self.ensureNegotiatedVersions(deadline_ms);
        const version = try self.version_registry.choose(.Metadata);
        const conn = try self.getBootstrapConnection(deadline_ms);

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
        try encodeMetadataRequest(&e, conn.correlation_id, version, topics_ptr, self.config.client_id, allow_auto_create);
        try self.callAndApplyMetadata(conn, is_flexible, e.written(), version, scope, deadline_ms);
    }

    pub fn refreshMetadataWithDeadline(self: *Cluster, deadline_ms: i64) errors.ClusterError!void {
        try self.waitForMetadataRefreshSlot(deadline_ms);

        self.metadata_refresh_attempts += 1;
        errdefer self.metadata_refresh_failures += 1;

        self.metadata_refresh_inflight = true;
        defer self.metadata_refresh_inflight = false;
        defer self.metadata_refresh_not_before_ms = scheduleJitteredMs(self.metadata_refresh_backoff_ms);

        errdefer self.next_metadata_retry_ms = scheduleJitteredMs(self.metadata_retry_backoff_ms);
        errdefer self.metadata_retry_backoff_ms = @min(self.metadata_retry_backoff_ms * 2, self.metadata_retry_backoff_cap_ms);

        try self.refreshMetadataScoped(.all_topics, null, true, deadline_ms);
        self.adoptBootstrapConnectionIfPossible();

        self.metadata_epoch_ms = std.time.milliTimestamp();
        self.metadata_last_success_ms = self.metadata_epoch_ms;
        self.next_metadata_retry_ms = 0;
        self.metadata_retry_backoff_ms = self.config.metadata_retry_backoff_ms;
    }

    pub fn refreshMetadata(self: *Cluster) errors.ClusterError!void {
        return self.refreshMetadataWithDeadline(deadlineMsFromNow(self.config.request_timeout_ms));
    }

    pub fn refreshTopicMetadataWithPolicyWithDeadline(
        self: *Cluster,
        topic: []const u8,
        allow_auto_create: bool,
        deadline_ms: i64,
    ) errors.ClusterError!void {
        try self.waitForMetadataRefreshSlot(deadline_ms);

        self.metadata_refresh_attempts += 1;
        errdefer self.metadata_refresh_failures += 1;

        self.metadata_refresh_inflight = true;
        defer self.metadata_refresh_inflight = false;
        defer self.metadata_refresh_not_before_ms = scheduleJitteredMs(self.metadata_refresh_backoff_ms);

        errdefer self.next_metadata_retry_ms = scheduleJitteredMs(self.metadata_retry_backoff_ms);
        errdefer self.metadata_retry_backoff_ms = @min(self.metadata_retry_backoff_ms * 2, self.metadata_retry_backoff_cap_ms);

        try self.refreshMetadataScoped(.one_topic, topic, allow_auto_create, deadline_ms);
        self.adoptBootstrapConnectionIfPossible();

        self.metadata_epoch_ms = std.time.milliTimestamp();
        self.metadata_last_success_ms = self.metadata_epoch_ms;
        self.next_metadata_retry_ms = 0;
        self.metadata_retry_backoff_ms = self.config.metadata_retry_backoff_ms;
    }

    pub fn refreshTopicMetadataWithDeadline(self: *Cluster, topic: []const u8, deadline_ms: i64) errors.ClusterError!void {
        return self.refreshTopicMetadataWithPolicyWithDeadline(topic, false, deadline_ms);
    }

    pub fn refreshTopicMetadata(self: *Cluster, topic: []const u8) errors.ClusterError!void {
        return self.refreshTopicMetadataWithPolicyWithDeadline(topic, false, deadlineMsFromNow(self.config.request_timeout_ms));
    }

    pub fn ensureTopicMetadataWithPolicyWithDeadline(
        self: *Cluster,
        topic: []const u8,
        deadline_ms: i64,
        allow_auto_create: bool,
    ) errors.ClusterError!void {
        const topic_known = self.cache.partition_state.contains(topic);
        if (!topic_known or self.metadataExpired()) {
            try self.refreshTopicMetadataWithPolicyWithDeadline(topic, allow_auto_create, deadline_ms);
        }
    }

    pub fn ensureTopicMetadataWithDeadline(self: *Cluster, topic: []const u8, deadline_ms: i64) errors.ClusterError!void {
        return self.ensureTopicMetadataWithPolicyWithDeadline(topic, false, deadline_ms);
    }

    pub fn ensureTopicMetadata(self: *Cluster, topic: []const u8) errors.ClusterError!void {
        return self.ensureTopicMetadataWithDeadline(topic, deadlineMsFromNow(self.config.request_timeout_ms));
    }

    pub fn refreshBrokersOnlyMetadata(self: *Cluster) errors.ClusterError!void {
        const deadline_ms = deadlineMsFromNow(self.config.request_timeout_ms);
        try self.waitForMetadataRefreshSlot(deadline_ms);

        self.metadata_refresh_attempts += 1;
        errdefer self.metadata_refresh_failures += 1;

        self.metadata_refresh_inflight = true;
        defer self.metadata_refresh_inflight = false;
        defer self.metadata_refresh_not_before_ms = scheduleJitteredMs(self.metadata_refresh_backoff_ms);

        errdefer self.next_metadata_retry_ms = scheduleJitteredMs(self.metadata_retry_backoff_ms);
        errdefer self.metadata_retry_backoff_ms = @min(self.metadata_retry_backoff_ms * 2, self.metadata_retry_backoff_cap_ms);

        try self.refreshMetadataScoped(.brokers_only, null, false, deadline_ms);
        self.adoptBootstrapConnectionIfPossible();

        self.metadata_epoch_ms = std.time.milliTimestamp();
        self.metadata_last_success_ms = self.metadata_epoch_ms;
        self.next_metadata_retry_ms = 0;
        self.metadata_retry_backoff_ms = self.config.metadata_retry_backoff_ms;
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
        self.clearBrokerVersionRanges();
    }

    pub fn triggerRebootstrap(self: *Cluster) void {
        self.metadata_rebootstrap_count += 1;
        self.invalidateMetadata();
        self.pool.closeAll();
        self.version_registry.reset();

        self.next_metadata_retry_ms = 0;
        self.metadata_refresh_not_before_ms = 0;
        self.metadata_retry_backoff_ms = self.config.metadata_retry_backoff_ms;
    }

    pub fn refreshMetadataNow(self: *Cluster) errors.ClusterError!void {
        if (self.metadata_refresh_inflight) {
            return error.MetadataUnavailable;
        }

        self.metadata_epoch_ms = 0;
        self.next_metadata_retry_ms = 0;
        self.metadata_refresh_not_before_ms = 0;
        self.metadata_retry_backoff_ms = self.config.metadata_retry_backoff_ms;

        try self.refreshMetadata();
    }

    fn shouldTopicRefreshForRouteError(err: errors.ClusterError) bool {
        return switch (err) {
            error.UnknownTopic, error.UnknownPartition, error.NoLeader => true,
            else => false,
        };
    }

    pub fn brokerForTopicPartitionWithDeadline(self: *Cluster, topic: []const u8, partition: i32, deadline_ms: i64) errors.ClusterError!model.Broker {
        if (self.metadataExpired()) {
            if (self.metadata_refresh_inflight) {
                if (self.cache.brokers.count() == 0) {
                    return error.MetadataUnavailable;
                }
            } else {
                self.refreshMetadataWithDeadline(deadline_ms) catch |err| switch (err) {
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
                try self.refreshTopicMetadataWithDeadline(topic, deadline_ms);
                return router.brokerFor(&self.cache, topic, partition) catch return error.StaleMetadata;
            }

            return err;
        };
    }

    pub fn brokerForTopicPartition(self: *Cluster, topic: []const u8, partition: i32) errors.ClusterError!model.Broker {
        return self.brokerForTopicPartitionWithDeadline(topic, partition, deadlineMsFromNow(self.config.request_timeout_ms));
    }

    fn connectionConfigForEndpoint(self: *const Cluster, endpoint: Endpoint) transport.connection.Config {
        return .{
            .host = endpoint.host,
            .port = endpoint.port,
            .connect_timeout_ms = self.config.connect_timeout_ms,
            .request_timeout_ms = self.config.request_timeout_ms,
            .max_frame_bytes = self.config.max_frame_bytes,
            .tcp_nodelay = self.config.tcp_nodelay,
            .enable_tcp_keepalive = self.config.enable_tcp_keepalive,
            .decoder_limits = self.config.protocol_limits,
            .client_id = self.config.client_id,
            .client_software_name = self.config.client_software_name,
            .client_software_version = self.config.client_software_version,
        };
    }

    fn getConnectionForBroker(self: *Cluster, b: model.Broker, deadline_ms: i64) errors.ClusterError!*transport.connection.Connection {
        const endpoint = self.rewriteEndpoint(b.host, b.port);

        return self.pool.getReady(b.node_id, deadline_ms, self.connectionConfigForEndpoint(endpoint)) catch |err| return errors.mapTransportError(err);
    }

    pub fn connectionForTopicPartitionWithDeadline(self: *Cluster, topic: []const u8, partition: i32, deadline_ms: i64) errors.ClusterError!*transport.connection.Connection {
        const broker = try self.brokerForTopicPartitionWithDeadline(topic, partition, deadline_ms);
        return self.getConnectionForBroker(broker, deadline_ms) catch |err| switch (err) {
            error.ConnectionReset, error.ConnectionRefused, error.NetworkUnreachable => {
                self.pool.remove(broker.node_id);
                self.clearPartitionLeaderEpoch(topic, partition);

                self.invalidateMetadata();
                try self.refreshMetadataWithDeadline(deadline_ms);

                const b2 = try self.brokerForTopicPartitionWithDeadline(topic, partition, deadline_ms);
                return self.getConnectionForBroker(b2, deadline_ms);
            },
            else => return err,
        };
    }

    pub fn connectionForTopicPartition(self: *Cluster, topic: []const u8, partition: i32) errors.ClusterError!*transport.connection.Connection {
        return self.connectionForTopicPartitionWithDeadline(topic, partition, deadlineMsFromNow(self.config.request_timeout_ms));
    }

    fn encodeRequestHeader(
        e: *codec.Encoder,
        api_key: i16,
        version: i16,
        correlation_id: i32,
        is_flexible: bool,
        client_id: []const u8,
    ) errors.ClusterError!void {
        if (is_flexible) {
            const request_header = header.RequestHeaderV2{
                .api_key = api_key,
                .api_version = version,
                .correlation_id = correlation_id,
                .client_id = client_id,
            };
            request_header.encode(e) catch return error.Unexpected;
        } else {
            const request_header = header.RequestHeaderV1{
                .api_key = api_key,
                .api_version = version,
                .correlation_id = correlation_id,
                .client_id = client_id,
            };
            request_header.encode(e) catch return error.Unexpected;
        }
    }

    fn encodeMetadataRequest(
        e: *codec.Encoder,
        correlation_id: i32,
        version: i16,
        topics: ?[]const generated.metadata.Request.MetadataRequestTopic,
        client_id: []const u8,
        allow_auto_create: bool,
    ) errors.ClusterError!void {
        const is_flexible = version >= 9;
        try encodeRequestHeader(e, @intFromEnum(generated.metadata.api_key), version, correlation_id, is_flexible, client_id);

        const request = generated.metadata.Request{
            .topics = topics,
            .allow_auto_topic_creation = allow_auto_create,
            .include_cluster_authorized_operations = false,
            .include_topic_authorized_operations = false,
        };

        request.encode(e, version) catch return error.Unexpected;
    }

    fn getBootstrapConnection(self: *Cluster, deadline_ms: i64) errors.ClusterError!*transport.connection.Connection {
        if (self.config.bootstrap_endpoints) |endpoints| {
            var last_err: errors.ClusterError = error.NoBrokers;
            for (endpoints) |endpoint| {
                const rewritten = self.rewriteEndpoint(endpoint.host, endpoint.port);

                const c = self.pool.getReady(
                    bootstrapBrokerId(endpoint.host, endpoint.port),
                    deadline_ms,
                    self.connectionConfigForEndpoint(rewritten),
                ) catch |err| {
                    last_err = errors.mapTransportError(err);
                    continue;
                };

                return c;
            }

            return self.fallbackToKnownBrokersOrRebootstrap(last_err, deadline_ms);
        }

        const rewritten_bootstrap = self.rewriteEndpoint(self.config.bootstrap_host, self.config.bootstrap_port);

        return self.pool.getReady(
            bootstrapBrokerId(self.config.bootstrap_host, self.config.bootstrap_port),
            deadline_ms,
            self.connectionConfigForEndpoint(rewritten_bootstrap),
        ) catch |err| {
            return self.fallbackToKnownBrokersOrRebootstrap(errors.mapTransportError(err), deadline_ms);
        };
    }

    fn fallbackToKnownBrokersOrRebootstrap(
        self: *Cluster,
        initial_last: errors.ClusterError,
        deadline_ms: i64,
    ) errors.ClusterError!*transport.connection.Connection {
        var last = initial_last;

        var it = self.cache.brokers.iterator();
        while (it.next()) |entry| {
            const b = entry.value_ptr.*;
            const endpoint = self.rewriteEndpoint(b.host, b.port);
            const conn = self.pool.getReady(
                b.node_id,
                deadline_ms,
                self.connectionConfigForEndpoint(endpoint),
            ) catch |e| {
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

        self.pruneBrokerVersionRangesToKnownBrokers();
    }

    fn callAndApplyMetadata(
        self: *Cluster,
        conn: *transport.connection.Connection,
        is_flexible: bool,
        payload: []const u8,
        version: i16,
        scope: MetadataRefreshScope,
        deadline_ms: i64,
    ) errors.ClusterError!void {
        const frame = conn.callWithDeadline(.Metadata, is_flexible, payload, deadline_ms) catch |err| return errors.mapTransportError(err);
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

    fn sendApiVersionsCompact(
        self: *Cluster,
        conn: *transport.connection.Connection,
        version: i16,
        deadline_ms: i64,
    ) errors.ClusterError!generated.api_versions.Response {
        var buf: [2048]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        try encodeRequestHeader(&e, @intFromEnum(generated.api_versions.api_key), version, conn.correlation_id, version >= 3, self.config.client_id);
        const request = generated.api_versions.Request{
            .client_software_name = self.config.client_software_name,
            .client_software_version = self.config.client_software_version,
        };
        request.encode(&e, version) catch return error.Unexpected;

        const frame = conn.callWithDeadline(.ApiVersions, version >= 3, e.written(), deadline_ms) catch |err| return errors.mapTransportError(err);
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
                if (parsed0.error_code != 35) {
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

    fn updateBrokerVersionRanges(self: *Cluster, broker_id: i32, response: generated.api_versions.Response) errors.ClusterError!void {
        var by_api = std.AutoHashMap(i16, versions.Range).init(self.allocator);
        errdefer by_api.deinit();

        for (response.api_keys) |k| {
            by_api.put(k.api_key, .{
                .min = k.min_version,
                .max = k.max_version,
            }) catch return error.Unexpected;
        }

        if (self.broker_version_ranges.fetchRemove(broker_id)) |old| {
            var old_ranges = old.value;
            old_ranges.deinit();
        }

        self.broker_version_ranges.put(broker_id, by_api) catch return error.Unexpected;
    }

    fn chooseVersionForBrokerId(self: *Cluster, broker_id: i32, api_key: types.ApiKey) errors.ClusterError!i16 {
        const by_api_ptr = self.broker_version_ranges.getPtr(broker_id) orelse return self.version_registry.choose(api_key);
        const range = by_api_ptr.get(@intFromEnum(api_key)) orelse return self.version_registry.choose(api_key);

        for (versions.Registry.preferredVersions(api_key)) |version| {
            if (version >= range.min and version <= range.max) {
                return version;
            }
        }

        return error.NoSupportedVersion;
    }

    fn negotiateBrokerVersionsWithDeadline(self: *Cluster, broker: model.Broker, deadline_ms: i64) errors.ClusterError!void {
        if (self.broker_version_ranges.contains(broker.node_id)) {
            return;
        }

        const conn = try self.getConnectionForBroker(broker, deadline_ms);
        const response_v4 = try self.sendApiVersionsCompact(conn, 4, deadline_ms);

        if (response_v4.error_code == 35) {
            const response_v2 = try self.sendApiVersionsCompact(conn, 2, deadline_ms);
            if (response_v2.error_code != 0) {
                return error.ProtocolError;
            }

            try self.updateBrokerVersionRanges(broker.node_id, response_v2);
            return;
        }

        if (response_v4.error_code != 0) {
            return error.ProtocolError;
        }

        try self.updateBrokerVersionRanges(broker.node_id, response_v4);
    }

    pub fn versionForTopicPartitionWithDeadline(
        self: *Cluster,
        topic: []const u8,
        partition: i32,
        api_key: types.ApiKey,
        deadline_ms: i64,
    ) errors.ClusterError!i16 {
        const broker = try self.brokerForTopicPartitionWithDeadline(topic, partition, deadline_ms);
        try self.negotiateBrokerVersionsWithDeadline(broker, deadline_ms);

        return self.chooseVersionForBrokerId(broker.node_id, api_key);
    }

    fn ensureNegotiatedVersions(self: *Cluster, deadline_ms: i64) errors.ClusterError!void {
        if (self.version_registry.has(.Metadata)) {
            return;
        }

        const conn = try self.getBootstrapConnection(deadline_ms);
        const response_v4 = try self.sendApiVersionsCompact(conn, 4, deadline_ms);
        if (response_v4.error_code == 35) {
            const response_v2 = try self.sendApiVersionsCompact(conn, 2, deadline_ms);
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

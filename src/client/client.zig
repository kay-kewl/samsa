const std = @import("std");
const cluster = @import("../cluster/module.zig");
const generated = @import("../generated/module.zig");
const header = @import("../protocol/header.zig");
const codec = @import("../protocol/codec.zig");
const batch = @import("../protocol/records/batch.zig");
const types = @import("../protocol/types.zig");
const transport = @import("../transport/module.zig");

pub const ClusterConfig = cluster.cluster.Config;

pub const Acks = enum(i16) {
    none = 0,
    leader = 1,
    all = -1,
};

pub const StartPosition = enum { earliest, latest };

pub const ProducerConfig = struct {
    acks: Acks = .all,
    request_ms: i32 = 30_000,
    max_request_bytes: usize = 1024 * 1024,
    max_record_bytes: usize = 1024 * 1024,
    retries_max_attempts: u8 = 5,
    allow_auto_topic_creation: bool = true,

    pub fn validate(self: @This(), cluster_config: ClusterConfig) !void {
        if (self.request_ms <= 0 or
            self.max_request_bytes == 0 or
            self.max_record_bytes == 0 or
            self.max_record_bytes > self.max_request_bytes or
            self.max_request_bytes > cluster_config.max_frame_bytes)
        {
            return error.InvalidConfiguration;
        }
    }
};

pub const ConsumerConfig = struct {
    fetch_min_bytes: i32 = 1,
    fetch_max_wait_ms: i32 = 500,
    fetch_max_bytes: i32 = 8 * 1024 * 1024,
    max_partition_fetch_bytes: i32 = 1024 * 1024,
    max_poll_records: usize = 500,
    max_poll_bytes: usize = 50 * 1024 * 1024,
    request_ms: i32 = 30_000,
    retries_max_attempts: u8 = 3,
    start_position: StartPosition = .latest,
    crc_validation_enabled: bool = true,
    allow_auto_topic_creation: bool = false,
    recent_poll_errors_capacity: usize = 256,

    pub fn validate(self: @This(), cluster_config: ClusterConfig) !void {
        if (self.request_ms <= 0 or
            self.fetch_max_bytes <= 0 or
            self.fetch_min_bytes <= 0 or
            self.fetch_max_wait_ms <= 0 or
            self.max_partition_fetch_bytes <= 0 or
            self.max_partition_fetch_bytes > self.fetch_max_bytes or
            self.max_poll_records == 0 or
            self.max_poll_bytes == 0 or
            self.retries_max_attempts == 0 or
            self.recent_poll_errors_capacity == 0 or
            cluster_config.max_frame_bytes == 0 or
            cluster_config.max_frame_bytes > std.math.maxInt(i32))
        {
            return error.InvalidConfiguration;
        }

        const max_frame_i32: i32 = @intCast(cluster_config.max_frame_bytes);
        if (self.fetch_max_bytes > max_frame_i32) {
            return error.InvalidConfiguration;
        }
    }
};

pub const ProduceResult = struct {
    topic: []const u8,
    partition: i32,
    base_offset: i64,
    timestamp: i64,
};

pub const RecordProduceError = struct {
    batch_index: i32,
    message: ?[]const u8 = null,
};

pub const ProduceError = struct {
    topic: []u8,
    partition: i32,
    error_code: i16,
    error_name: []const u8,
    message: ?[]u8 = null,
    record_errors: ?[]RecordProduceError = null,
};

pub const RecordHeader = struct {
    key: []const u8,
    value: ?[]const u8,
};

pub const Record = struct {
    topic: []const u8,
    partition: i32,
    offset: i64,
    timestamp: i64,
    key: ?[]const u8,
    value: ?[]const u8,
    headers: []const RecordHeader,
};

pub const OwnedRecord = struct {
    topic: []const u8,
    partition: i32,
    offset: i64,
    timestamp: i64,
    key: ?[]u8,
    value: ?[]u8,
    headers: []RecordHeader,
};

fn freeOwnedHeaders(allocator: std.mem.Allocator, headers: []RecordHeader) void {
    for (headers) |h| {
        allocator.free(h.key);
        if (h.value) |v| {
            allocator.free(v);
        }
    }

    allocator.free(headers);
}

fn freeOwnedRecord(allocator: std.mem.Allocator, record: OwnedRecord) void {
    allocator.free(record.topic);

    if (record.key) |k| {
        allocator.free(k);
    }

    if (record.value) |v| {
        allocator.free(v);
    }

    freeOwnedHeaders(allocator, record.headers);
}

pub fn freeOwnedRecords(allocator: std.mem.Allocator, records: []OwnedRecord) void {
    for (records) |record| {
        freeOwnedRecord(allocator, record);
    }

    allocator.free(records);
}

const BrokerDisposition = enum {
    fatal,
    retry,
    refresh_and_retry,
    rebootstrap,
};

pub const PollErrorSource = enum { broker, local };

pub const LocalPollErrorKind = enum {
    operation_failed,
    unsupported_compression,
    crc_mismatch,
    batch_parse_error,
    topic_recreated,
};

pub const PartitionError = struct {
    topic: []const u8,
    partition: i32,
    error_code: i16 = 0,
    source: PollErrorSource = .broker,
    local_kind: ?LocalPollErrorKind = null,
    error_message: ?[]const u8 = null,
};

pub const Statistics = struct {
    produce_calls: u64 = 0,
    produce_errors: u64 = 0,
    produce_retries: u64 = 0,

    poll_calls: u64 = 0,
    poll_errors: u64 = 0,
    poll_retries: u64 = 0,
    poll_error_drops: u64 = 0,

    empty_polls: u64 = 0,
    records_returned: u64 = 0,
    control_batches_skipped: u64 = 0,
    crc_mismatch_count: u64 = 0,
    record_decode_error_count: u64 = 0,

    metadata_refreshes: u64 = 0,
    metadata_refresh_attempts: u64 = 0,
    metadata_refresh_successes: u64 = 0,
    metadata_refresh_failures: u64 = 0,

    connection_drop_events: u64 = 0,
    retry_exhausted: u64 = 0,

    bytes_encoded: u64 = 0,
    bytes_decoded: u64 = 0,
    observed_throttle_time_ms_total: u64 = 0,
};

pub const Client = struct {
    cluster: cluster.cluster.Cluster,

    pub fn init(allocator: std.mem.Allocator, config: cluster.cluster.Config) !Client {
        try config.validate();

        return .{
            .cluster = cluster.cluster.Cluster.init(allocator, config),
        };
    }

    pub fn deinit(self: *Client) void {
        self.cluster.deinit();
    }

    pub fn refreshMetadata(self: *Client) !void {
        try self.cluster.refreshMetadata();
    }

    pub fn leaderFor(self: *Client, topic: []const u8, partition: i32) !i32 {
        return try self.cluster.leaderFor(topic, partition);
    }
};

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

fn retryBackoffWithJitterMs(base_ms: i32, cap_ms: i32, attempt: u8) i32 {
    const exp: u5 = @intCast(@min(@as(u8, 10), attempt));
    const raw = base_ms * (@as(i32, 1) << exp);
    const max_delay = @min(cap_ms, raw);
    if (max_delay <= 0) {
        return 0;
    }

    return @as(i32, @intCast(std.crypto.random.intRangeAtMost(u32, 0, @as(u32, @intCast(max_delay)))));
}

fn sleepBackoffUntilDeadline(delay_ms: i32, deadline_ms: i64) !void {
    if (delay_ms <= 0) {
        return;
    }

    const remaining = remainingMs(deadline_ms);
    if (remaining <= 0) {
        return error.Timeout;
    }

    const clipped = @max(@as(i32, 1), @min(delay_ms, remaining));
    std.Thread.sleep(@as(u64, @intCast(clipped)) * std.time.ns_per_ms);
}

fn encodeRequestHeader(
    e: *codec.Encoder,
    api_key: i16,
    version: i16,
    correlation_id: i32,
    is_flexible: bool,
    client_id: []const u8,
) !void {
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

fn decodeResponseHeader(
    d: *codec.Decoder,
    api_key: types.ApiKey,
    is_flexible: bool,
) !void {
    const response_header = header.responseHeaderVersion(api_key, is_flexible);
    switch (response_header) {
        .v0 => _ = try header.ResponseHeaderV0.decode(d),
        .v1, .v2 => _ = try header.ResponseHeaderV1.decode(d),
    }
}

const InitialPositionDisposition = enum {
    stale_metadata,
    retryable,
    offset_out_of_range,
    fatal,
};

fn classifyInitialPositionBrokerCode(code: i16) InitialPositionDisposition {
    if (code == 1) {
        return .offset_out_of_range;
    }

    return switch (classifyBrokerCode(code)) {
        .rebootstrap, .refresh_and_retry => .stale_metadata,
        .retry => .retryable,
        .fatal => .fatal,
    };
}

fn classifyBrokerCode(code: i16) BrokerDisposition {
    const err_code: types.BrokerErrorCode = @enumFromInt(code);

    return switch (err_code) {
        .REBOOTSTRAP_REQUIRED => .rebootstrap,
        .UNKNOWN_TOPIC_OR_PARTITION,
        .LEADER_NOT_AVAILABLE,
        .NOT_LEADER_OR_FOLLOWER,
        .FENCED_LEADER_EPOCH,
        .UNKNOWN_LEADER_EPOCH,
        => .refresh_and_retry,
        .REQUEST_TIMED_OUT,
        .NOT_ENOUGH_REPLICAS,
        .NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        .KAFKA_STORAGE_ERROR,
        => .retry,
        else => .fatal,
    };
}

pub fn isRouteRefreshError(code: i16) bool {
    return switch (classifyBrokerCode(code)) {
        .refresh_and_retry, .rebootstrap => true,
        else => false,
    };
}

fn brokerErrorName(code: i16) []const u8 {
    return switch (code) {
        1 => "OFFSET_OUT_OF_RANGE",
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        5 => "LEADER_NOT_AVAILABLE",
        6 => "NOT_LEADER_OR_FOLLOWER",
        7 => "REQUEST_TIMED_OUT",
        10 => "MESSAGE_TOO_LARGE",
        19 => "NOT_ENOUGH_REPLICAS",
        20 => "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        56 => "KAFKA_STORAGE_ERROR",
        74 => "FENCED_LEADER_EPOCH",
        75 => "UNKNOWN_LEADER_EPOCH",
        129 => "REBOOTSTRAP_REQUIRED",
        else => "UNKNOWN_BROKER_ERROR",
    };
}

fn isRetryableBrokerError(code: i16) bool {
    return switch (classifyBrokerCode(code)) {
        .retry, .refresh_and_retry => true,
        else => false,
    };
}

pub fn isRetryableSendError(err: anyerror) bool {
    return switch (err) {
        error.Timeout,
        error.RetryBackoffActive,
        error.ConnectionReset,
        error.ConnectionRefused,
        error.NetworkUnreachable,
        error.MetadataUnavailable,
        error.StaleMetadata,
        error.RetryableBroker,
        error.BrokenPipe,
        error.EndOfStream,
        => true,
        else => false,
    };
}

fn refreshAllMetadataTracked(c: *cluster.cluster.Cluster, statistics: *Statistics, deadline_ms: i64) void {
    statistics.metadata_refreshes += 1;
    statistics.metadata_refresh_attempts += 1;

    if (c.refreshMetadataWithDeadline(deadline_ms)) |_| {
        statistics.metadata_refresh_successes += 1;
    } else |_| {
        statistics.metadata_refresh_failures += 1;
    }
}

fn refreshTopicMetadataTracked(
    c: *cluster.cluster.Cluster,
    statistics: *Statistics,
    topic: []const u8,
    deadline_ms: i64,
    allow_auto_create: bool,
) void {
    statistics.metadata_refreshes += 1;
    statistics.metadata_refresh_attempts += 1;

    if (c.refreshTopicMetadataWithPolicyWithDeadline(topic, allow_auto_create, deadline_ms)) |_| {
        statistics.metadata_refresh_successes += 1;
    } else |_| {
        statistics.metadata_refresh_failures += 1;
    }
}

pub const Producer = struct {
    allocator: std.mem.Allocator,
    cluster: cluster.cluster.Cluster,
    config: ProducerConfig,
    batch_builder: batch.BatchBuilder,
    rr_cursor_by_topic: std.StringHashMap(usize),
    topic_generation_by_topic: std.StringHashMap(u64),
    last_produce_error: ?ProduceError = null,
    request_buf: []u8,
    statistics: Statistics,

    pub fn init(allocator: std.mem.Allocator, cluster_config: ClusterConfig, config: ProducerConfig) !Producer {
        try cluster_config.validate();
        try config.validate(cluster_config);

        const request_buf = try allocator.alloc(u8, config.max_request_bytes);
        errdefer allocator.free(request_buf);

        return .{
            .allocator = allocator,
            .cluster = cluster.cluster.Cluster.init(allocator, cluster_config),
            .config = config,
            .batch_builder = batch.BatchBuilder.init(allocator),
            .rr_cursor_by_topic = std.StringHashMap(usize).init(allocator),
            .topic_generation_by_topic = std.StringHashMap(u64).init(allocator),
            .last_produce_error = null,
            .request_buf = request_buf,
            .statistics = .{},
        };
    }

    pub fn deinit(self: *Producer) void {
        self.clearLastProduceError();

        var it = self.rr_cursor_by_topic.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }

        {
            var gen_it = self.topic_generation_by_topic.iterator();
            while (gen_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
        }

        self.allocator.free(self.request_buf);
        self.topic_generation_by_topic.deinit();
        self.rr_cursor_by_topic.deinit();
        self.batch_builder.deinit();
        self.cluster.deinit();
    }

    pub fn close(self: *Producer) void {
        self.deinit();
    }

    pub fn getStatistics(self: *Producer) Statistics {
        return self.statistics;
    }

    pub fn getMetrics(self: *Producer) Statistics {
        return self.getStatistics();
    }

    pub fn getClusterStatistics(self: *const Producer) cluster.model.ClusterStatistics {
        return self.cluster.statistics();
    }

    inline fn requestClientId(self: *const Producer) []const u8 {
        return self.cluster.config.client_id;
    }

    pub fn getLastProduceError(self: *const Producer) ?ProduceError {
        return self.last_produce_error;
    }

    fn clearLastProduceError(self: *Producer) void {
        if (self.last_produce_error) |state| {
            self.allocator.free(state.topic);

            if (state.message) |m| {
                self.allocator.free(m);
            }

            if (state.record_errors) |items| {
                for (items) |item| {
                    if (item.message) |m| {
                        self.allocator.free(m);
                    }
                }

                self.allocator.free(items);
            }

            self.last_produce_error = null;
        }
    }

    fn setLastProduceError(
        self: *Producer,
        topic: []const u8,
        partition: i32,
        part: generated.produce.Response.TopicProduceResponse.PartitionProduceResponse,
    ) !void {
        self.clearLastProduceError();

        var out: ProduceError = .{
            .topic = try self.allocator.dupe(u8, topic),
            .partition = partition,
            .error_code = part.error_code,
            .error_name = brokerErrorName(part.error_code),
            .message = if (part.error_message) |m| try self.allocator.dupe(u8, m) else null,
            .record_errors = null,
        };
        errdefer {
            self.allocator.free(out.topic);

            if (out.message) |m| {
                self.allocator.free(m);
            }

            if (out.record_errors) |items| {
                for (items) |item| {
                    if (item.message) |m| {
                        self.allocator.free(m);
                    }
                }

                self.allocator.free(items);
            }
        }

        if (part.record_errors.len > 0) {
            const items = try self.allocator.alloc(RecordProduceError, part.record_errors.len);
            for (part.record_errors, 0..) |src, i| {
                items[i] = .{
                    .batch_index = src.batch_index,
                    .message = if (src.batch_index_error_message) |m| try self.allocator.dupe(u8, m) else null,
                };
            }

            out.record_errors = items;
        }

        self.last_produce_error = out;
    }

    fn choosePartition(self: *Producer, topic: []const u8, key: ?[]const u8) !i32 {
        const by_partition = self.cluster.cache.partition_state.get(topic) orelse return error.UnknownTopic;

        var ids: std.ArrayList(i32) = .empty;
        defer ids.deinit(self.allocator);

        var it = by_partition.iterator();
        while (it.next()) |entry| {
            const ps = entry.value_ptr.*;
            if (ps.error_code == 0 and ps.leader_id != null) {
                try ids.append(self.allocator, entry.key_ptr.*);
            }
        }

        if (ids.items.len == 0) {
            return error.NoLeader;
        }

        std.mem.sort(i32, ids.items, {}, std.sort.asc(i32));

        if (key) |k| {
            const h: usize = @intCast(std.hash.Murmur2_32.hash(k) & 0x7fff_ffff);
            return ids.items[h % ids.items.len];
        }

        const observed_generation = self.cluster.topicGeneration(topic) orelse 0;
        const gen_gop = try self.topic_generation_by_topic.getOrPut(topic);
        if (!gen_gop.found_existing) {
            gen_gop.key_ptr.* = try self.allocator.dupe(u8, topic);
            gen_gop.value_ptr.* = observed_generation;
        } else if (gen_gop.value_ptr.* != observed_generation) {
            gen_gop.value_ptr.* = observed_generation;
            if (self.rr_cursor_by_topic.getPtr(topic)) |cursor| {
                cursor.* = @as(usize, @intCast(std.hash.Wyhash.hash(0, topic) % @as(u64, @intCast(ids.items.len))));
            }
        }

        const gop = try self.rr_cursor_by_topic.getOrPut(topic);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, topic);
            gop.value_ptr.* = @as(usize, @intCast(std.hash.Wyhash.hash(0, topic) % @as(u64, @intCast(ids.items.len))));
        }

        const index = gop.value_ptr.* % ids.items.len;
        gop.value_ptr.* +%= 1;
        return ids.items[index];
    }

    pub fn sendOnce(self: *Producer, topic: []const u8, key: ?[]const u8, value: ?[]const u8, deadline_ms: i64) !ProduceResult {
        try self.cluster.ensureTopicMetadataWithPolicyWithDeadline(topic, deadline_ms, self.config.allow_auto_topic_creation);

        const partition = self.choosePartition(topic, key) catch |err| switch (err) {
            error.UnknownTopic, error.NoLeader => blk: {
                try self.cluster.refreshTopicMetadataWithPolicyWithDeadline(topic, self.config.allow_auto_topic_creation, deadline_ms);
                break :blk try self.choosePartition(topic, key);
            },
            else => return err,
        };
        const conn = try self.cluster.connectionForTopicPartitionWithDeadline(topic, partition, deadline_ms);

        const version = try self.cluster.versionForTopicPartitionWithDeadline(topic, partition, .Produce, deadline_ms);
        const is_flexible = version >= 9;

        const now_ms = std.time.milliTimestamp();
        const record_batch = try self.batch_builder.buildSingleRecord(now_ms, key, value);

        var part_data = [_]generated.produce.Request.TopicProduceData.PartitionProduceData{
            .{
                .index = partition,
                .records = record_batch,
            },
        };

        var topic_data = [_]generated.produce.Request.TopicProduceData{
            .{
                .name = topic,
                .partition_data = &part_data,
            },
        };

        const request = generated.produce.Request{
            .transactional_id = null,
            .acks = @intFromEnum(self.config.acks),
            .timeout_ms = @max(1, remainingMs(deadline_ms)),
            .topic_data = &topic_data,
        };

        var e = codec.Encoder.init(self.request_buf);
        try encodeRequestHeader(&e, @intFromEnum(generated.produce.api_key), version, conn.correlation_id, is_flexible, self.requestClientId());
        request.encode(&e, version) catch |err| switch (err) {
            error.NoSpace => return error.FrameTooLarge,
            else => return err,
        };

        const payload = e.written();
        if (payload.len > self.cluster.config.max_frame_bytes) {
            return error.FrameTooLarge;
        }
        self.statistics.bytes_encoded +%= @as(u64, @intCast(payload.len));

        if (self.config.acks == .none) {
            try conn.callNoResponseWithDeadline(payload, deadline_ms);
            return .{
                .topic = topic,
                .partition = partition,
                .base_offset = -1,
                .timestamp = -1,
            };
        }

        const frame = try conn.callWithDeadline(.Produce, is_flexible, payload, deadline_ms);
        defer self.allocator.free(frame);
        self.statistics.bytes_decoded +%= @as(u64, @intCast(frame.len));

        var d = codec.Decoder.initWithLimits(frame, self.cluster.config.protocol_limits);
        try decodeResponseHeader(&d, .Produce, is_flexible);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const response = try generated.produce.Response.decode(arena.allocator(), &d, version);
        if (response.throttle_time_ms > 0) {
            self.statistics.observed_throttle_time_ms_total +%= @as(u64, @intCast(response.throttle_time_ms));
        }
        if (d.remaining() != 0 or response.responses.len == 0 or response.responses[0].partition_responses.len == 0) {
            return error.ProtocolError;
        }

        const p = response.responses[0].partition_responses[0];
        if (p.error_code != 0) {
            self.setLastProduceError(topic, partition, p) catch {};

            const code = p.error_code;
            const disposition = classifyBrokerCode(code);
            std.log.warn("producer broker error {s} ({d}) topic={s} partition={d}", .{ brokerErrorName(code), code, topic, partition });

            if (code == 10) {
                return error.RecordTooLarge;
            }

            switch (disposition) {
                .rebootstrap => {
                    if (p.error_message) |message| {
                        std.log.warn("producer broker message topic={s} partition={d}: {s}", .{ topic, partition, message });
                    }

                    if (p.record_errors.len > 0) {
                        for (p.record_errors) |re| {
                            std.log.warn("producer record error topic={s} partition={d} batch_index={d} message={s}", .{
                                topic, partition, re.batch_index, re.batch_index_error_message orelse "none",
                            });
                        }
                    }

                    self.cluster.triggerRebootstrap();
                    refreshAllMetadataTracked(&self.cluster, &self.statistics, deadline_ms);
                    return error.StaleMetadata;
                },
                .refresh_and_retry => {
                    if (code == 74 or code == 75) {
                        self.cluster.clearPartitionLeaderEpoch(topic, partition);
                    }

                    refreshTopicMetadataTracked(&self.cluster, &self.statistics, topic, deadline_ms, self.config.allow_auto_topic_creation);
                    return error.StaleMetadata;
                },
                .retry => return error.RetryableBroker,
                .fatal => return error.BrokerError,
            }
        }

        const out_timestamp = if (p.log_append_time_ms >= 0) p.log_append_time_ms else now_ms;

        return .{
            .topic = topic,
            .partition = partition,
            .base_offset = p.base_offset,
            .timestamp = out_timestamp,
        };
    }

    pub fn send(self: *Producer, topic: []const u8, key: ?[]const u8, value: ?[]const u8) !ProduceResult {
        self.clearLastProduceError();
        self.statistics.produce_calls += 1;
        errdefer self.statistics.produce_errors += 1;

        const key_len = if (key) |k| k.len else 0;
        const val_len = if (value) |v| v.len else 0;
        if (key_len + val_len > self.config.max_record_bytes) {
            return error.RecordTooLarge;
        }

        const deadline_ms = deadlineMsFromNow(self.config.request_ms);

        if (self.config.acks == .none) {
            return self.sendOnce(topic, key, value, deadline_ms);
        }

        const max_attempts: u8 = @max(self.config.retries_max_attempts, @as(u8, 1));

        var attempt: u8 = 0;
        while (attempt < max_attempts) : (attempt += 1) {
            if (remainingMs(deadline_ms) <= 0) {
                return error.Timeout;
            }

            const result = self.sendOnce(topic, key, value, deadline_ms) catch |err| {
                if (err == error.ConnectionReset or err == error.BrokenPipe or err == error.EndOfStream) {
                    self.statistics.connection_drop_events += 1;
                }

                const is_last_attempt = (attempt + 1) >= max_attempts;
                if (is_last_attempt or !isRetryableSendError(err)) {
                    if (is_last_attempt and isRetryableSendError(err)) {
                        self.statistics.retry_exhausted += 1;
                    }

                    return err;
                }

                self.statistics.produce_retries += 1;
                const delay_ms = retryBackoffWithJitterMs(50, 1000, attempt);
                try sleepBackoffUntilDeadline(delay_ms, deadline_ms);
                continue;
            };

            return result;
        }

        return error.Timeout;
    }
};

pub const Assignment = struct {
    topic: []u8,
    partition: i32,
    position: ?i64 = null,
    topic_generation: ?u64 = null,
};

const BrokerFetchPartition = struct {
    assignment_index: usize,
    partition: i32,
    fetch_offset: i64,
    leader_epoch: i32,
};

const BrokerFetchGroup = struct {
    broker_id: i32,
    topic_partitions: std.StringHashMap(std.ArrayList(BrokerFetchPartition)),
};

const PendingPartition = struct {
    assignment_index: usize,
    raw_records: []const u8,
};

pub const Consumer = struct {
    allocator: std.mem.Allocator,
    cluster: cluster.cluster.Cluster,
    config: ConsumerConfig,
    assignments: std.ArrayList(Assignment),
    recent_errors: std.ArrayList(PartitionError),
    poll_arena: std.heap.ArenaAllocator,
    next_assignment_start: usize,
    request_buf: []u8,
    statistics: Statistics,

    pub fn init(allocator: std.mem.Allocator, cluster_config: ClusterConfig, config: ConsumerConfig) !Consumer {
        try cluster_config.validate();
        try config.validate(cluster_config);

        const request_buf = try allocator.alloc(u8, cluster_config.max_frame_bytes);
        errdefer allocator.free(request_buf);

        return .{
            .allocator = allocator,
            .cluster = cluster.cluster.Cluster.init(allocator, cluster_config),
            .config = config,
            .assignments = .empty,
            .recent_errors = .empty,
            .poll_arena = std.heap.ArenaAllocator.init(allocator),
            .next_assignment_start = 0,
            .request_buf = request_buf,
            .statistics = .{},
        };
    }

    pub fn deinit(self: *Consumer) void {
        for (self.assignments.items) |a| {
            self.allocator.free(a.topic);
        }

        self.allocator.free(self.request_buf);
        self.assignments.deinit(self.allocator);
        self.recent_errors.deinit(self.allocator);
        self.poll_arena.deinit();
        self.cluster.deinit();
    }

    pub fn close(self: *Consumer) void {
        self.deinit();
    }

    pub fn getStatistics(self: *Consumer) Statistics {
        return self.statistics;
    }

    pub fn getMetrics(self: *Consumer) Statistics {
        return self.getStatistics();
    }

    pub fn getClusterStatistics(self: *const Consumer) cluster.model.ClusterStatistics {
        return self.cluster.statistics();
    }

    inline fn requestClientId(self: *const Consumer) []const u8 {
        return self.cluster.config.client_id;
    }

    fn deinitFetchGroups(self: *Consumer, groups: *std.ArrayList(BrokerFetchGroup)) void {
        for (groups.items) |*g| {
            var topic_it = g.topic_partitions.iterator();
            while (topic_it.next()) |entry| {
                entry.value_ptr.deinit(self.allocator);
            }

            g.topic_partitions.deinit();
        }

        groups.deinit(self.allocator);
    }

    fn pushGroupLocalError(self: *Consumer, group: *const BrokerFetchGroup, kind: LocalPollErrorKind, message: ?[]const u8) void {
        var topic_it = group.topic_partitions.iterator();
        while (topic_it.next()) |entry| {
            for (entry.value_ptr.items) |bp| {
                const a = self.assignments.items[bp.assignment_index];
                self.pushLocalPollError(a.topic, a.partition, kind, message);
            }
        }
    }

    fn appendFetchedRecordsFromPartition(
        self: *Consumer,
        a: *Assignment,
        raw_records: []const u8,
        out: *std.ArrayList(Record),
        bytes_accumulator: *usize,
        max_records_to_take: usize,
    ) !usize {
        if (a.position == null or max_records_to_take == 0) {
            return 0;
        }

        var delivered: usize = 0;
        var cursor: usize = 0;
        while (cursor < raw_records.len) {
            var parser = batch.BatchParser.init(
                raw_records[cursor..],
                self.cluster.config.protocol_limits,
                .{ .validate_crc = self.config.crc_validation_enabled },
            ) catch |err| switch (err) {
                error.UnsupportedCompression => {
                    self.pushLocalPollError(a.topic, a.partition, .unsupported_compression, "UnsupportedCompression");
                    break;
                },
                error.CrcMismatch => {
                    self.statistics.crc_mismatch_count += 1;
                    self.pushLocalPollError(a.topic, a.partition, .crc_mismatch, "CrcMismatch");
                    break;
                },
                else => {
                    self.pushLocalPollError(a.topic, a.partition, .batch_parse_error, "BatchParseError");
                    break;
                },
            };

            const batch_wire_len: usize = @as(usize, @intCast(12 + parser.batch_length));
            const batch_end_offset = parser.base_offset + parser.last_offset_delta + 1;

            if (parser.isControlBatch()) {
                self.statistics.control_batches_skipped += 1;
                if (a.position.? < batch_end_offset) {
                    a.position = batch_end_offset;
                }

                cursor += batch_wire_len;
                continue;
            }

            while (true) {
                const next_record = parser.next(self.poll_arena.allocator()) catch |err| {
                    self.statistics.record_decode_error_count += 1;
                    self.pushLocalPollError(a.topic, a.partition, .batch_parse_error, @errorName(err));
                    break;
                };

                if (next_record == null) {
                    break;
                }

                const r = next_record.?;

                const abs_offset = parser.base_offset + r.offset_delta;
                if (abs_offset < a.position.?) {
                    continue;
                }

                var record_bytes: usize = (if (r.key) |k| k.len else 0) + (if (r.value) |v| v.len else 0);
                for (r.headers) |h| {
                    record_bytes += h.key.len;
                    if (h.value) |v| {
                        record_bytes += v.len;
                    }
                }

                if (out.items.len >= self.config.max_poll_records) {
                    return delivered;
                }

                if (bytes_accumulator.* + record_bytes > self.config.max_poll_bytes) {
                    bytes_accumulator.* = self.config.max_poll_bytes;
                    return delivered;
                }

                var owned_headers = try self.poll_arena.allocator().alloc(RecordHeader, r.headers.len);
                for (r.headers, 0..) |h, i| {
                    owned_headers[i] = .{ .key = h.key, .value = h.value };
                }

                try out.append(self.poll_arena.allocator(), .{
                    .topic = a.topic,
                    .partition = a.partition,
                    .offset = abs_offset,
                    .timestamp = parser.base_timestamp + r.timestamp_delta,
                    .key = r.key,
                    .value = r.value,
                    .headers = owned_headers,
                });

                bytes_accumulator.* += record_bytes;
                a.position = abs_offset + 1;
                delivered += 1;

                if (delivered >= max_records_to_take) {
                    return delivered;
                }
            }

            if (a.position.? < batch_end_offset) {
                a.position = batch_end_offset;
            }

            cursor += batch_wire_len;
        }

        return delivered;
    }

    fn buildFetchGroups(self: *Consumer, start_index: usize, deadline_ms: i64) !std.ArrayList(BrokerFetchGroup) {
        var groups: std.ArrayList(BrokerFetchGroup) = .empty;
        errdefer self.deinitFetchGroups(&groups);

        var broker_to_group = std.AutoHashMap(i32, usize).init(self.allocator);
        defer broker_to_group.deinit();

        if (self.assignments.items.len == 0) {
            return groups;
        }

        var i: usize = 0;
        while (i < self.assignments.items.len) : (i += 1) {
            if (remainingMs(deadline_ms) <= 0) {
                break;
            }

            const assignment_index = (start_index + i) % self.assignments.items.len;
            const a = self.assignments.items[assignment_index];
            const fetch_offset = a.position orelse continue;

            const broker = self.cluster.brokerForTopicPartitionWithDeadline(a.topic, a.partition, deadline_ms) catch |err| {
                self.pushLocalPollError(a.topic, a.partition, .operation_failed, @errorName(err));
                continue;
            };

            const group_gop = try broker_to_group.getOrPut(broker.node_id);
            if (!group_gop.found_existing) {
                try groups.append(self.allocator, .{
                    .broker_id = broker.node_id,
                    .topic_partitions = std.StringHashMap(std.ArrayList(BrokerFetchPartition)).init(self.allocator),
                });
                group_gop.value_ptr.* = groups.items.len - 1;
            }

            var group = &groups.items[group_gop.value_ptr.*];

            const topic_gop = try group.topic_partitions.getOrPut(a.topic);
            if (!topic_gop.found_existing) {
                topic_gop.key_ptr.* = a.topic;
                topic_gop.value_ptr.* = .empty;
            }

            try topic_gop.value_ptr.append(self.allocator, .{
                .assignment_index = assignment_index,
                .partition = a.partition,
                .fetch_offset = fetch_offset,
                .leader_epoch = self.cluster.leaderEpochFor(a.topic, a.partition) orelse -1,
            });
        }

        return groups;
    }

    fn fetchBrokerGroupOnce(
        self: *Consumer,
        group: *BrokerFetchGroup,
        out: *std.ArrayList(Record),
        bytes_accumulator: *usize,
        deadline_ms: i64,
    ) !void {
        if (out.items.len >= self.config.max_poll_records or bytes_accumulator.* >= self.config.max_poll_bytes) {
            return;
        }

        var seed_assignment_index: ?usize = null;
        var seed_it = group.topic_partitions.iterator();
        while (seed_it.next()) |entry| {
            if (entry.value_ptr.items.len > 0) {
                seed_assignment_index = entry.value_ptr.items[0].assignment_index;
                break;
            }
        }

        const first_index = seed_assignment_index orelse return;
        const seed = self.assignments.items[first_index];

        const timeout_ms = remainingMs(deadline_ms);
        if (timeout_ms <= 0) {
            return error.Timeout;
        }

        const seed_broker = try self.cluster.brokerForTopicPartitionWithDeadline(seed.topic, seed.partition, deadline_ms);
        if (seed_broker.node_id != group.broker_id) {
            return error.StaleMetadata;
        }

        const conn = try self.cluster.connectionForTopicPartitionWithDeadline(seed.topic, seed.partition, deadline_ms);
        const version = try self.cluster.versionForTopicPartitionWithDeadline(seed.topic, seed.partition, .Fetch, deadline_ms);
        const is_flexible = version >= 12;

        const fetch_max_bytes_usize: usize = @intCast(self.config.fetch_max_bytes);
        const bytes_budget = self.config.max_poll_bytes - bytes_accumulator.*;
        const request_max_bytes: i32 = @intCast(@min(fetch_max_bytes_usize, bytes_budget));
        if (request_max_bytes <= 0) {
            return;
        }

        const request_min_bytes = @max(@as(i32, 1), @min(self.config.fetch_min_bytes, request_max_bytes));

        var request_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer request_arena.deinit();

        const request_alloc = request_arena.allocator();
        var request_topics: std.ArrayList(generated.fetch.Request.FetchTopic) = .empty;
        defer request_topics.deinit(request_alloc);

        var topic_it = group.topic_partitions.iterator();
        while (topic_it.next()) |entry| {
            const src_parts = entry.value_ptr.items;
            if (src_parts.len == 0) {
                continue;
            }

            const request_parts = try request_alloc.alloc(generated.fetch.Request.FetchTopic.FetchPartition, src_parts.len);
            for (src_parts, 0..) |src, i| {
                request_parts[i] = .{
                    .partition = src.partition,
                    .current_leader_epoch = src.leader_epoch,
                    .fetch_offset = src.fetch_offset,
                    .last_fetched_epoch = -1,
                    .log_start_offset = -1,
                    .partition_max_bytes = self.config.max_partition_fetch_bytes,
                };
            }

            try request_topics.append(request_alloc, .{
                .topic = entry.key_ptr.*,
                .partitions = request_parts,
            });
        }

        if (request_topics.items.len == 0) {
            return;
        }

        const request = generated.fetch.Request{
            .replica_id = -1,
            .max_wait_ms = @min(self.config.fetch_max_wait_ms, timeout_ms),
            .min_bytes = request_min_bytes,
            .max_bytes = request_max_bytes,
            .isolation_level = 0,
            .session_id = 0,
            .session_epoch = -1,
            .topics = request_topics.items,
            .forgotten_topics_data = &.{},
            .rack_id = "",
        };

        var e = codec.Encoder.init(self.request_buf);
        try encodeRequestHeader(&e, @intFromEnum(generated.fetch.api_key), version, conn.correlation_id, is_flexible, self.requestClientId());
        request.encode(&e, version) catch |err| switch (err) {
            error.NoSpace => return error.FrameTooLarge,
            else => return err,
        };

        const fetch_payload = e.written();
        self.statistics.bytes_encoded +%= @as(u64, @intCast(fetch_payload.len));

        const frame = try conn.callWithDeadline(.Fetch, is_flexible, fetch_payload, deadline_ms);
        defer self.allocator.free(frame);
        self.statistics.bytes_decoded +%= @as(u64, @intCast(frame.len));

        var d = codec.Decoder.initWithLimits(frame, self.cluster.config.protocol_limits);
        try decodeResponseHeader(&d, .Fetch, is_flexible);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const response = try generated.fetch.Response.decode(arena.allocator(), &d, version);
        if (response.throttle_time_ms > 0) {
            self.statistics.observed_throttle_time_ms_total +%= @as(u64, @intCast(response.throttle_time_ms));
        }
        if (d.remaining() != 0) {
            return error.ProtocolError;
        }

        if (response.error_code != 0) {
            const code = response.error_code;
            var refreshed = false;

            var group_it = group.topic_partitions.iterator();
            while (group_it.next()) |entry| {
                for (entry.value_ptr.items) |bp| {
                    const a = self.assignments.items[bp.assignment_index];

                    if (!refreshed and isRouteRefreshError(code)) {
                        self.maybeRefreshTopicOnRouteErrorWithDeadline(a.topic, a.partition, code, deadline_ms);
                        refreshed = true;
                    }

                    self.pushBrokerPollError(a.topic, a.partition, code, brokerErrorName(code));
                }
            }

            if (isRouteRefreshError(code)) {
                return error.StaleMetadata;
            }
            if (isRetryableBrokerError(code)) {
                return error.RetryableBroker;
            }

            return error.BrokerError;
        }

        var pending: std.ArrayList(PendingPartition) = .empty;
        defer pending.deinit(self.allocator);

        var refreshed_for_partition_errors = false;
        for (response.responses) |topic_response| {
            const requested_parts = group.topic_partitions.getPtr(topic_response.topic) orelse continue;

            for (topic_response.partitions) |partition_response| {
                var matched: ?BrokerFetchPartition = null;
                for (requested_parts.items) |candidate| {
                    if (candidate.partition == partition_response.partition_index) {
                        matched = candidate;
                        break;
                    }
                }

                const bp = matched orelse continue;
                const a = &self.assignments.items[bp.assignment_index];

                if (partition_response.error_code != 0) {
                    if (!refreshed_for_partition_errors and isRouteRefreshError(partition_response.error_code)) {
                        self.maybeRefreshTopicOnRouteErrorWithDeadline(a.topic, a.partition, partition_response.error_code, deadline_ms);
                        refreshed_for_partition_errors = true;
                    }

                    self.pushBrokerPollError(a.topic, a.partition, partition_response.error_code, brokerErrorName(partition_response.error_code));
                    continue;
                }

                const raw_records = partition_response.records orelse continue;
                try pending.append(self.allocator, .{
                    .assignment_index = bp.assignment_index,
                    .raw_records = raw_records,
                });
            }
        }

        while (out.items.len < self.config.max_poll_records or bytes_accumulator.* < self.config.max_poll_bytes) {
            var progressed = false;

            for (pending.items) |item| {
                if (out.items.len >= self.config.max_poll_records or bytes_accumulator.* >= self.config.max_poll_bytes) {
                    break;
                }

                const a = &self.assignments.items[item.assignment_index];
                const before_position = a.position;
                const appended = try self.appendFetchedRecordsFromPartition(a, item.raw_records, out, bytes_accumulator, 1);

                if (appended > 0 or before_position != a.position) {
                    progressed = true;
                }
            }

            if (!progressed) {
                break;
            }
        }
    }

    fn fetchBrokerGroupWithRetry(
        self: *Consumer,
        group: *BrokerFetchGroup,
        out: *std.ArrayList(Record),
        bytes_accumulator: *usize,
        deadline_ms: i64,
    ) !void {
        const max_attempts: u8 = @max(@as(u8, 1), self.config.retries_max_attempts);
        var attempt: u8 = 0;
        while (attempt < max_attempts) : (attempt += 1) {
            self.fetchBrokerGroupOnce(group, out, bytes_accumulator, deadline_ms) catch |err| {
                if (err == error.ConnectionReset or err == error.BrokenPipe or err == error.EndOfStream) {
                    self.statistics.connection_drop_events += 1;
                }

                const is_last_attempt = (attempt + 1) >= max_attempts;
                if (is_last_attempt or !isRetryableSendError(err) or remainingMs(deadline_ms) <= 0) {
                    if (is_last_attempt and isRetryableSendError(err)) {
                        self.statistics.retry_exhausted += 1;
                    }

                    return err;
                }

                self.statistics.poll_retries += 1;
                const delay_ms = retryBackoffWithJitterMs(50, 500, attempt);
                try sleepBackoffUntilDeadline(delay_ms, deadline_ms);
                continue;
            };

            return;
        }
    }

    pub fn assign(self: *Consumer, topic: []const u8, partition: i32) !void {
        for (self.assignments.items) |a| {
            if (a.partition == partition and std.mem.eql(u8, a.topic, topic)) {
                return;
            }
        }

        try self.assignments.append(self.allocator, .{
            .topic = try self.allocator.dupe(u8, topic),
            .partition = partition,
            .position = null,
            .topic_generation = self.cluster.topicGeneration(topic),
        });
    }

    pub fn assignMultiple(self: *Consumer, items: []const Assignment) !void {
        for (items) |a| {
            try self.assign(a.topic, a.partition);
        }
    }

    pub fn seek(self: *Consumer, topic: []const u8, partition: i32, offset: i64) !void {
        for (self.assignments.items) |*a| {
            if (a.partition == partition and std.mem.eql(u8, a.topic, topic)) {
                a.position = offset;
                return;
            }
        }

        return error.NotAssigned;
    }

    pub fn peekRecentPollErrors(self: *const Consumer) []const PartitionError {
        return self.recent_errors.items;
    }

    pub fn takeRecentPollErrors(self: *Consumer, allocator: std.mem.Allocator) ![]PartitionError {
        const out = try allocator.dupe(PartitionError, self.recent_errors.items);
        self.recent_errors.clearRetainingCapacity();
        return out;
    }

    pub fn getRecentPollErrors(self: *Consumer, allocator: std.mem.Allocator) ![]PartitionError {
        return self.takeRecentPollErrors(allocator);
    }

    fn appendRecentPollError(self: *Consumer, item: PartitionError) void {
        if (self.recent_errors.items.len >= self.config.recent_poll_errors_capacity) {
            _ = self.recent_errors.orderedRemove(0);
            self.statistics.poll_error_drops += 1;
        }

        self.recent_errors.append(self.allocator, item) catch {
            self.statistics.poll_error_drops += 1;
        };
    }

    fn pushBrokerPollError(self: *Consumer, topic: []const u8, partition: i32, code: i16, message: ?[]const u8) void {
        self.statistics.poll_errors += 1;
        self.appendRecentPollError(.{
            .topic = topic,
            .partition = partition,
            .error_code = code,
            .source = .broker,
            .local_kind = null,
            .error_message = message,
        });
    }

    fn pushLocalPollError(self: *Consumer, topic: []const u8, partition: i32, kind: LocalPollErrorKind, message: ?[]const u8) void {
        self.statistics.poll_errors += 1;
        self.appendRecentPollError(.{
            .topic = topic,
            .partition = partition,
            .error_code = 0,
            .source = .local,
            .local_kind = kind,
            .error_message = message,
        });
    }

    fn maybeRefreshTopicOnRouteErrorWithDeadline(self: *Consumer, topic: []const u8, partition: i32, code: i16, deadline_ms: i64) void {
        switch (classifyBrokerCode(code)) {
            .rebootstrap => {
                self.cluster.triggerRebootstrap();
                refreshAllMetadataTracked(&self.cluster, &self.statistics, deadline_ms);
            },
            .refresh_and_retry => {
                if (code == 74 or code == 75) {
                    self.cluster.clearPartitionLeaderEpoch(topic, partition);
                }

                refreshTopicMetadataTracked(&self.cluster, &self.statistics, topic, deadline_ms, self.config.allow_auto_topic_creation);
            },
            else => {},
        }
    }

    fn resolveInitialPosition(self: *Consumer, a: *Assignment, deadline_ms: i64) !void {
        if (a.position != null) {
            return;
        }

        const ts: i64 = switch (self.config.start_position) {
            .earliest => -2,
            .latest => -1,
        };

        try self.cluster.ensureTopicMetadataWithPolicyWithDeadline(a.topic, deadline_ms, self.config.allow_auto_topic_creation);

        const conn = try self.cluster.connectionForTopicPartitionWithDeadline(a.topic, a.partition, deadline_ms);
        const version = try self.cluster.versionForTopicPartitionWithDeadline(a.topic, a.partition, .ListOffsets, deadline_ms);
        const is_flexible = version >= 6;

        var req_partitions = [_]generated.list_offsets.Request.ListOffsetsTopic.ListOffsetsPartition{
            .{
                .partition_index = a.partition,
                .current_leader_epoch = self.cluster.leaderEpochFor(a.topic, a.partition) orelse -1,
                .timestamp = ts,
            },
        };

        var req_topics = [_]generated.list_offsets.Request.ListOffsetsTopic{
            .{
                .name = a.topic,
                .partitions = &req_partitions,
            },
        };

        const timeout_ms = @max(1, remainingMs(deadline_ms));
        const req = generated.list_offsets.Request{
            .replica_id = -1,
            .isolation_level = 0,
            .topics = &req_topics,
            .timeout_ms = timeout_ms,
        };

        var e = codec.Encoder.init(self.request_buf);
        try encodeRequestHeader(&e, @intFromEnum(generated.list_offsets.api_key), version, conn.correlation_id, is_flexible, self.cluster.config.client_id);
        req.encode(&e, version) catch |err| switch (err) {
            error.NoSpace => return error.FrameTooLarge,
            else => return err,
        };

        const frame = try conn.callWithDeadline(.ListOffsets, is_flexible, e.written(), deadline_ms);
        defer self.allocator.free(frame);

        var d = codec.Decoder.initWithLimits(frame, self.cluster.config.protocol_limits);
        try decodeResponseHeader(&d, .ListOffsets, is_flexible);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const resp = try generated.list_offsets.Response.decode(arena.allocator(), &d, version);
        if (d.remaining() != 0) {
            return error.ProtocolError;
        }

        if (resp.topics.len == 0 or resp.topics[0].partitions.len == 0) {
            return error.ProtocolError;
        }

        const p = resp.topics[0].partitions[0];
        if (p.error_code != 0) {
            const code = p.error_code;
            self.maybeRefreshTopicOnRouteErrorWithDeadline(a.topic, a.partition, p.error_code, deadline_ms);
            self.pushBrokerPollError(a.topic, a.partition, p.error_code, brokerErrorName(p.error_code));

            return switch (classifyInitialPositionBrokerCode(code)) {
                .stale_metadata => error.StaleMetadata,
                .retryable => error.RetryableBroker,
                .offset_out_of_range => error.OffsetOutOfRange,
                .fatal => error.BrokerError,
            };
        }

        a.position = p.offset;
    }

    fn resolveInitialPositionWithRetry(self: *Consumer, a: *Assignment, deadline_ms: i64) !void {
        const max_attempts: u8 = @max(@as(u8, 1), self.config.retries_max_attempts);
        var attempt: u8 = 0;
        while (attempt < max_attempts) : (attempt += 1) {
            self.resolveInitialPosition(a, deadline_ms) catch |err| {
                const retryable = (err == error.StaleMetadata or err == error.RetryableBroker);
                const is_last_attempt = (attempt + 1) >= max_attempts;
                if (!retryable or is_last_attempt or remainingMs(deadline_ms) <= 0) {
                    return err;
                }

                const delay_ms = retryBackoffWithJitterMs(50, 500, attempt);
                try sleepBackoffUntilDeadline(delay_ms, deadline_ms);
                continue;
            };

            return;
        }
    }

    pub fn poll(self: *Consumer, timeout_ms: i32) ![]const Record {
        self.statistics.poll_calls += 1;

        _ = self.poll_arena.reset(.retain_capacity);
        self.recent_errors.clearRetainingCapacity();

        var out = std.ArrayList(Record).empty;
        defer out.deinit(self.poll_arena.allocator());

        const effective_timeout_ms = @max(@as(i32, 1), @min(timeout_ms, self.config.request_ms));
        const deadline_ms = deadlineMsFromNow(effective_timeout_ms);
        var bytes_accumulator: usize = 0;

        if (self.assignments.items.len == 0) {
            return out.items;
        }

        const start = self.next_assignment_start % self.assignments.items.len;
        self.next_assignment_start = (start + 1) % self.assignments.items.len;

        var assignment: usize = 0;
        while (assignment < self.assignments.items.len) : (assignment += 1) {
            if (remainingMs(deadline_ms) <= 0) {
                break;
            }

            const index = (start + assignment) % self.assignments.items.len;
            var a = &self.assignments.items[index];

            const observed_generation = self.cluster.topicGeneration(a.topic);
            if (a.topic_generation != observed_generation) {
                a.topic_generation = observed_generation;
                a.position = null;
                self.pushLocalPollError(a.topic, a.partition, .topic_recreated, "TopicIdentityChanged");
            }

            self.resolveInitialPositionWithRetry(a, deadline_ms) catch |err| {
                self.pushLocalPollError(a.topic, a.partition, .operation_failed, @errorName(err));
                continue;
            };
        }

        if (remainingMs(deadline_ms) > 0) {
            var groups = try self.buildFetchGroups(start, deadline_ms);
            defer self.deinitFetchGroups(&groups);

            for (groups.items) |*g| {
                if (remainingMs(deadline_ms) <= 0) {
                    break;
                }

                self.fetchBrokerGroupWithRetry(g, &out, &bytes_accumulator, deadline_ms) catch |err| switch (err) {
                    error.Timeout => break,
                    error.StaleMetadata, error.RetryableBroker => continue,
                    else => {
                        self.pushGroupLocalError(g, .operation_failed, @errorName(err));
                        continue;
                    },
                };

                if (out.items.len >= self.config.max_poll_records or bytes_accumulator >= self.config.max_poll_bytes) {
                    break;
                }
            }
        }

        self.statistics.records_returned += @as(u64, @intCast(out.items.len));
        if (out.items.len == 0) {
            self.statistics.empty_polls += 1;
        }

        return out.items;
    }

    pub fn pollOwned(self: *Consumer, allocator: std.mem.Allocator, timeout_ms: i32) ![]OwnedRecord {
        const records = try self.poll(timeout_ms);

        var out: std.ArrayList(OwnedRecord) = .empty;
        errdefer {
            for (out.items) |r| {
                freeOwnedRecord(allocator, r);
            }
            out.deinit(allocator);
        }

        for (records) |r| {
            var headers = try allocator.alloc(RecordHeader, r.headers.len);
            var headers_initialized: usize = 0;
            errdefer {
                var i: usize = 0;
                while (i < headers_initialized) : (i += 1) {
                    allocator.free(headers[i].key);
                    if (headers[i].value) |v| {
                        allocator.free(v);
                    }
                }

                allocator.free(headers);
            }

            for (r.headers, 0..) |h, i| {
                headers[i] = .{
                    .key = try allocator.dupe(u8, h.key),
                    .value = if (h.value) |v| try allocator.dupe(u8, v) else null,
                };
                headers_initialized = i + 1;
            }

            try out.append(allocator, .{
                .topic = try allocator.dupe(u8, r.topic),
                .partition = r.partition,
                .offset = r.offset,
                .timestamp = r.timestamp,
                .key = if (r.key) |k| try allocator.dupe(u8, k) else null,
                .value = if (r.value) |v| try allocator.dupe(u8, v) else null,
                .headers = headers,
            });
        }

        return out.toOwnedSlice(allocator);
    }
};

const testing = std.testing;

fn buildEmptyBatchForTest(
    allocator: std.mem.Allocator,
    base_offset: i64,
    last_offset_delta: i32,
    timestamp_ms: i64,
) ![]u8 {
    const total_wire_size: usize = 61;
    const batch_length: i32 = 49;

    const bytes = try allocator.alloc(u8, total_wire_size);
    errdefer allocator.free(bytes);

    var e = codec.Encoder.init(bytes);
    try e.writeI64(base_offset);
    try e.writeI32(batch_length);
    try e.writeI32(-1);
    try e.writeI8(2);

    const crc_pos = e.pos;
    try e.writeU32(0);
    try e.writeI16(0);
    try e.writeI32(last_offset_delta);
    try e.writeI64(timestamp_ms);
    try e.writeI64(timestamp_ms);
    try e.writeI64(-1);
    try e.writeI16(-1);
    try e.writeI32(-1);
    try e.writeI32(0);

    const crc = @import("../protocol/crc32c.zig").calculate(bytes[21..]);
    bytes[crc_pos] = @as(u8, @truncate(crc >> 24));
    bytes[crc_pos + 1] = @as(u8, @truncate(crc >> 16));
    bytes[crc_pos + 2] = @as(u8, @truncate(crc >> 8));
    bytes[crc_pos + 3] = @as(u8, @truncate(crc));

    return bytes;
}

test "consumer buildFetchGroups groups partitions by broker" {
    const allocator = testing.allocator;

    var c = try Consumer.init(allocator, .{}, .{});
    defer c.deinit();

    c.cluster.metadata_epoch_ms = std.time.milliTimestamp();

    try c.cluster.cache.brokers.put(1, .{
        .node_id = 1,
        .host = "127.0.0.1",
        .port = 9092,
    });
    try c.cluster.cache.brokers.put(2, .{
        .node_id = 2,
        .host = "127.0.0.1",
        .port = 9093,
    });

    const topic_name = try allocator.dupe(u8, "events");
    var parts = std.AutoHashMap(i32, cluster.model.PartitionState).init(allocator);

    try parts.put(0, .{
        .error_code = 0,
        .leader_id = 1,
        .leader_epoch = 11,
        .replica_ids = try allocator.alloc(i32, 0),
        .isr_ids = try allocator.alloc(i32, 0),
        .offline_replica_ids = try allocator.alloc(i32, 0),
    });
    try parts.put(1, .{
        .error_code = 0,
        .leader_id = 1,
        .leader_epoch = 12,
        .replica_ids = try allocator.alloc(i32, 0),
        .isr_ids = try allocator.alloc(i32, 0),
        .offline_replica_ids = try allocator.alloc(i32, 0),
    });
    try parts.put(2, .{
        .error_code = 0,
        .leader_id = 2,
        .leader_epoch = 21,
        .replica_ids = try allocator.alloc(i32, 0),
        .isr_ids = try allocator.alloc(i32, 0),
        .offline_replica_ids = try allocator.alloc(i32, 0),
    });

    try c.cluster.cache.partition_state.put(topic_name, parts);

    try c.assign("events", 0);
    try c.assign("events", 1);
    try c.assign("events", 2);

    try c.seek("events", 0, 100);
    try c.seek("events", 1, 200);
    try c.seek("events", 2, 300);

    var groups = try c.buildFetchGroups(0, deadlineMsFromNow(1000));
    defer c.deinitFetchGroups(&groups);

    try testing.expectEqual(@as(usize, 2), groups.items.len);

    var seen_0 = false;
    var seen_1 = false;
    var seen_2 = false;
    var total_partitions: usize = 0;

    for (groups.items) |g| {
        const by_topic = g.topic_partitions.getPtr("events") orelse continue;
        total_partitions += by_topic.items.len;

        for (by_topic.items) |p| {
            switch (p.partition) {
                0 => {
                    seen_0 = true;
                    try testing.expectEqual(@as(i64, 100), p.fetch_offset);
                    try testing.expectEqual(@as(i32, 11), p.leader_epoch);
                },
                1 => {
                    seen_1 = true;
                    try testing.expectEqual(@as(i64, 200), p.fetch_offset);
                    try testing.expectEqual(@as(i32, 12), p.leader_epoch);
                },
                2 => {
                    seen_2 = true;
                    try testing.expectEqual(@as(i64, 300), p.fetch_offset);
                    try testing.expectEqual(@as(i32, 21), p.leader_epoch);
                },
                else => {
                    try testing.expect(false);
                },
            }
        }
    }

    try testing.expectEqual(@as(usize, 3), total_partitions);
    try testing.expect(seen_0 and seen_1 and seen_2);
}

test "broker error naming and retry classification" {
    try testing.expectEqualStrings("REQUEST_TIMED_OUT", brokerErrorName(7));
    try testing.expect(isRetryableBrokerError(7));
    try testing.expect(!isRetryableBrokerError(3));
    try testing.expect(isRetryableSendError(error.RetryableBroker));
}

test "initial-position broker classification covers all runtime branches" {
    try testing.expectEqual(InitialPositionDisposition.offset_out_of_range, classifyInitialPositionBrokerCode(1));
    try testing.expectEqual(InitialPositionDisposition.retryable, classifyInitialPositionBrokerCode(7));
    try testing.expectEqual(InitialPositionDisposition.stale_metadata, classifyInitialPositionBrokerCode(6));
    try testing.expectEqual(InitialPositionDisposition.stale_metadata, classifyInitialPositionBrokerCode(129));
    try testing.expectEqual(InitialPositionDisposition.fatal, classifyInitialPositionBrokerCode(42));
}

test "consumer append helper advances position for empty batch" {
    const allocator = testing.allocator;

    var c = try Consumer.init(allocator, .{}, .{});
    defer c.deinit();

    try c.assign("events", 0);
    try c.seek("events", 0, 5);

    const empty_batch = try buildEmptyBatchForTest(allocator, 5, 3, std.time.milliTimestamp());
    defer allocator.free(empty_batch);

    _ = c.poll_arena.reset(.retain_capacity);
    var out = std.ArrayList(Record).empty;
    defer out.deinit(c.poll_arena.allocator());

    var bytes_accumulator: usize = 0;
    const a = &c.assignments.items[0];

    const delivered = try c.appendFetchedRecordsFromPartition(a, empty_batch, &out, &bytes_accumulator, 1);
    try testing.expectEqual(@as(usize, 0), delivered);
    try testing.expectEqual(@as(?i64, 9), a.position);
    try testing.expectEqual(@as(usize, 0), out.items.len);
}

test "consumer append helper respects max_records_to_take across concatenated batches" {
    const allocator = testing.allocator;

    var c = try Consumer.init(allocator, .{}, .{});
    defer c.deinit();

    try c.assign("events", 0);
    try c.seek("events", 0, 0);

    var builder = batch.BatchBuilder.init(allocator);
    defer builder.deinit();

    const ts = std.time.milliTimestamp();

    const first_batch = try allocator.dupe(u8, try builder.buildSingleRecord(ts, "k1", "v1"));
    defer allocator.free(first_batch);

    const second_batch = try allocator.dupe(u8, try builder.buildSingleRecord(ts, "k2", "v2"));
    defer allocator.free(second_batch);

    std.mem.writeInt(i64, second_batch[0..8], 1, .big);

    var raw = std.ArrayList(u8).empty;
    defer raw.deinit(allocator);
    try raw.appendSlice(allocator, first_batch);
    try raw.appendSlice(allocator, second_batch);

    _ = c.poll_arena.reset(.retain_capacity);
    var out = std.ArrayList(Record).empty;
    defer out.deinit(c.poll_arena.allocator());

    var bytes_accumulator: usize = 0;
    const a = &c.assignments.items[0];

    const first_take = try c.appendFetchedRecordsFromPartition(a, raw.items, &out, &bytes_accumulator, 1);
    try testing.expectEqual(@as(usize, 1), first_take);
    try testing.expectEqual(@as(?i64, 1), a.position);
    try testing.expectEqual(@as(usize, 1), out.items.len);
    try testing.expectEqual(@as(i64, 0), out.items[0].offset);

    const second_take = try c.appendFetchedRecordsFromPartition(a, raw.items, &out, &bytes_accumulator, 1);
    try testing.expectEqual(@as(usize, 1), second_take);
    try testing.expectEqual(@as(?i64, 2), a.position);
    try testing.expectEqual(@as(usize, 2), out.items.len);
    try testing.expectEqual(@as(i64, 0), out.items[1].offset);
}

test "consumer append helper surfaces record decode failure as local poll errors" {
    const allocator = testing.allocator;

    var c = try Consumer.init(allocator, .{}, .{
        .crc_validation_enabled = false,
    });
    defer c.deinit();

    try c.assign("events", 0);
    try c.seek("events", 0, 0);

    var builder = batch.BatchBuilder.init(allocator);
    defer builder.deinit();

    const bytes_const = try builder.buildSingleRecord(std.time.milliTimestamp(), "k", "v");
    var corrupted = try allocator.dupe(u8, bytes_const);
    defer allocator.free(corrupted);

    corrupted[61] = 0x7f;

    _ = c.poll_arena.reset(.retain_capacity);
    c.recent_errors.clearRetainingCapacity();

    var out = std.ArrayList(Record).empty;
    defer out.deinit(c.poll_arena.allocator());

    var bytes_accumulator: usize = 0;
    const a = &c.assignments.items[0];

    const delivered = try c.appendFetchedRecordsFromPartition(a, corrupted, &out, &bytes_accumulator, 10);
    try testing.expectEqual(@as(usize, 0), delivered);
    try testing.expect(c.recent_errors.items.len >= 1);
    try testing.expectEqual(LocalPollErrorKind.batch_parse_error, c.recent_errors.items[0].local_kind.?);
    try testing.expect(c.statistics.record_decode_error_count >= 1);
}

test "consumer recent poll errors are bounded by capacity" {
    var c = try Consumer.init(testing.allocator, .{}, .{
        .recent_poll_errors_capacity = 2,
    });
    defer c.deinit();

    c.pushLocalPollError("events", 0, .operation_failed, "e0");
    c.pushLocalPollError("events", 1, .operation_failed, "e1");
    c.pushLocalPollError("events", 2, .operation_failed, "e2");

    try testing.expectEqual(@as(usize, 2), c.recent_errors.items.len);
    try testing.expectEqual(@as(i32, 1), c.recent_errors.items[0].partition);
    try testing.expectEqual(@as(i32, 2), c.recent_errors.items[1].partition);
    try testing.expectEqual(@as(u64, 1), c.statistics.poll_error_drops);
}

fn seedSinglePartitionStateForRouteTest(c: *Consumer, topic: []const u8, leader_epoch: i32) !void {
    try c.cluster.cache.brokers.put(1, .{
        .node_id = 1,
        .host = "127.0.0.1",
        .port = 9092,
    });

    const topic_name = try testing.allocator.dupe(u8, topic);
    var parts = std.AutoHashMap(i32, cluster.model.PartitionState).init(testing.allocator);
    try parts.put(0, .{
        .error_code = 0,
        .leader_id = 1,
        .leader_epoch = leader_epoch,
        .replica_ids = try testing.allocator.alloc(i32, 0),
        .isr_ids = try testing.allocator.alloc(i32, 0),
        .offline_replica_ids = try testing.allocator.alloc(i32, 0),
    });

    try c.cluster.cache.partition_state.put(topic_name, parts);
}

test "consumer route error UNKNOWN_LEADER_EPOCH clears cached leader_epoch" {
    var c = try Consumer.init(testing.allocator, .{
        .connect_timeout_ms = 1,
        .request_timeout_ms = 1,
    }, .{});
    defer c.deinit();

    try seedSinglePartitionStateForRouteTest(&c, "events", 9);
    try testing.expectEqual(@as(?i32, 9), c.cluster.leaderEpochFor("events", 0));

    c.maybeRefreshTopicOnRouteErrorWithDeadline("events", 0, 75, deadlineMsFromNow(10));

    try testing.expectEqual(@as(?i32, null), c.cluster.leaderEpochFor("events", 0));
    try testing.expect(c.statistics.metadata_refreshes >= 1);
    try testing.expect(c.statistics.metadata_refresh_attempts >= 1);
    try testing.expectEqual(c.statistics.metadata_refresh_attempts, c.statistics.metadata_refresh_successes + c.statistics.metadata_refresh_failures);
}

test "consumer route error NOT_LEADER_OR_FOLLOWER keeps cached leader_epoch" {
    var c = try Consumer.init(testing.allocator, .{
        .connect_timeout_ms = 1,
        .request_timeout_ms = 1,
    }, .{});
    defer c.deinit();

    try seedSinglePartitionStateForRouteTest(&c, "events", 9);
    try testing.expectEqual(@as(?i32, 9), c.cluster.leaderEpochFor("events", 0));

    c.maybeRefreshTopicOnRouteErrorWithDeadline("events", 0, 6, deadlineMsFromNow(10));

    try testing.expectEqual(@as(?i32, 9), c.cluster.leaderEpochFor("events", 0));
    try testing.expect(c.statistics.metadata_refreshes >= 1);
    try testing.expect(c.statistics.metadata_refresh_attempts >= 1);
    try testing.expect(c.statistics.metadata_refresh_attempts, c.statistics.metadata_refresh_successes + c.statistics.metadata_refresh_failures);
}

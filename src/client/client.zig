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
};

pub const ConsumerConfig = struct {
    fetch_min_bytes: i32 = 1,
    fetch_max_wait_ms: i32 = 500,
    fetch_max_bytes: i32 = 8 * 1024 * 1024,
    max_partition_fetch_bytes: i32 = 1024 * 1024,
    max_poll_records: usize = 500,
    max_poll_bytes: usize = 50 * 1024 * 1024,
    start_position: StartPosition = .latest,
    crc_validation_enabled: bool = true,
};

pub const ProduceResult = struct {
    topic: []const u8,
    partition: i32,
    base_offset: i64,
    timestamp: i64,
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

pub const PartitionError = struct {
    topic: []const u8,
    partition: i32,
    error_code: i16,
    error_message: ?[]const u8 = null,
};

pub const Statistics = struct {
    produce_calls: u64 = 0,
    produce_errors: u64 = 0,
    poll_calls: u64 = 0,
    poll_errors: u64 = 0,
    records_returned: u64 = 0,
};

pub const Client = struct {
    cluster: cluster.cluster.Cluster,

    pub fn init(allocator: std.mem.Allocator, config: cluster.cluster.Config) Client {
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

fn encodeRequestHeader(
    e: *codec.Encoder,
    api_key: i16,
    version: i16,
    correlation_id: i32,
    is_flexible: bool,
) !void {
    if (is_flexible) {
        const request_header = header.RequestHeaderV2{
            .api_key = api_key,
            .api_version = version,
            .correlation_id = correlation_id,
            .client_id = "samsa-client",
        };
        request_header.encode(e) catch return error.Unexpected;
    } else {
        const request_header = header.RequestHeaderV1{
            .api_key = api_key,
            .api_version = version,
            .correlation_id = correlation_id,
            .client_id = "samsa-client",
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

fn isRouteRefreshError(code: i16) bool {
    return switch (code) {
        3, // UNKNOWN_TOPIC_OR_PARTITION
        5, // LEADER_NOT_AVAILABLE
        6, // NOT_LEADER_OR_FOLLOWER
        74, // FENCED_LEADER_EPOCH
        75, // UNKNOWN_LEADER_EPOCH
        129, // REBOOTSTRAP_REQUIRED
        => true,
        else => false,
    };
}

fn isRetryableSendError(err: anyerror) bool {
    return switch (err) {
        error.Timeout,
        error.ConnectionReset,
        error.ConnectionRefused,
        error.NetworkUnreachable,
        error.MetadataUnavailable,
        error.StaleMetadata,
        => true,
        else => false,
    };
}

pub const Producer = struct {
    allocator: std.mem.Allocator,
    cluster: cluster.cluster.Cluster,
    config: ProducerConfig,
    batch_builder: batch.BatchBuilder,
    rr_cursor_by_topic: std.StringHashMap(usize),
    statistics: Statistics,

    pub fn init(allocator: std.mem.Allocator, cluster_config: ClusterConfig, config: ProducerConfig) Producer {
        return .{
            .allocator = allocator,
            .cluster = cluster.cluster.Cluster.init(allocator, cluster_config),
            .config = config,
            .batch_builder = batch.BatchBuilder.init(allocator),
            .rr_cursor_by_topic = std.StringHashMap(usize).init(allocator),
            .statistics = .{},
        };
    }

    pub fn deinit(self: *Producer) void {
        var it = self.rr_cursor_by_topic.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }

        self.rr_cursor_by_topic.deinit();
        self.batch_builder.deinit();
        self.cluster.deinit();
    }

    pub fn getStatistics(self: *Producer) Statistics {
        return self.statistics;
    }

    pub fn close(self: *Producer) void {
        self.deinit();
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
        try self.cluster.refreshTopicMetadata(topic);

        const partition = try self.choosePartition(topic, key);
        const conn = try self.cluster.connectionForTopicPartition(topic, partition);

        const version = try self.cluster.version_registry.choose(.Produce);
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

        const request_buf = try self.allocator.alloc(u8, self.config.max_request_bytes);
        defer self.allocator.free(request_buf);

        var e = codec.Encoder.init(request_buf);
        try encodeRequestHeader(&e, @intFromEnum(generated.produce.api_key), version, conn.correlation_id, is_flexible);
        try request.encode(&e, version);

        if (self.config.acks == .none) {
            try conn.callNoResponse(e.written());
            return .{
                .topic = topic,
                .partition = partition,
                .base_offset = -1,
                .timestamp = -1,
            };
        }

        const frame = try conn.call(.Produce, is_flexible, e.written());
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
        try decodeResponseHeader(&d, .Produce, is_flexible);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const response = try generated.produce.Response.decode(arena.allocator(), &d, version);
        if (d.remaining() != 0 or response.responses.len == 0 or response.responses[0].partition_responses.len == 0) {
            return error.ProtocolError;
        }

        const p = response.responses[0].partition_responses[0];
        if (p.error_code != 0) {
            if (p.error_code == 74 or p.error_code == 75) {
                self.cluster.clearPartitionLeaderEpoch(topic, partition);
            }

            if (isRouteRefreshError(p.error_code)) {
                _ = self.cluster.refreshTopicMetadata(topic) catch {};
            }

            return error.StaleMetadata;
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
        self.statistics.produce_calls += 1;
        errdefer self.statistics.produce_errors += 1;

        const key_len = if (key) |k| k.len else 0;
        const val_len = if (value) |v| v.len else 0;
        if (key_len + val_len > self.config.max_record_bytes) {
            return error.RecordTooLarge;
        }

        const deadline_ms = deadlineMsFromNow(self.config.request_ms);
        const max_attempts: u8 = @max(self.config.retries_max_attempts, @as(u8, 1));

        var attempt: u8 = 0;
        while (attempt < max_attempts) : (attempt += 1) {
            if (remainingMs(deadline_ms) <= 0) {
                return error.Timeout;
            }

            const result = self.sendOnce(topic, key, value, deadline_ms) catch |err| {
                const is_last_attempt = (attempt + 1) >= max_attempts;
                if (is_last_attempt or !isRetryableSendError(err)) {
                    return err;
                }

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
};

pub const Consumer = struct {
    allocator: std.mem.Allocator,
    cluster: cluster.cluster.Cluster,
    config: ConsumerConfig,
    assignments: std.ArrayList(Assignment),
    recent_errors: std.ArrayList(PartitionError),
    poll_arena: std.heap.ArenaAllocator,
    next_assignment_start: usize,
    statistics: Statistics,

    pub fn init(allocator: std.mem.Allocator, cluster_config: ClusterConfig, config: ConsumerConfig) Consumer {
        return .{
            .allocator = allocator,
            .cluster = cluster.cluster.Cluster.init(allocator, cluster_config),
            .config = config,
            .assignments = .empty,
            .recent_errors = .empty,
            .poll_arena = std.heap.ArenaAllocator.init(allocator),
            .next_assignment_start = 0,
            .statistics = .{},
        };
    }

    pub fn deinit(self: *Consumer) void {
        for (self.assignments.items) |a| {
            self.allocator.free(a.topic);
        }

        self.assignments.deinit(self.allocator);
        self.recent_errors.deinit(self.allocator);
        self.poll_arena.deinit();
        self.cluster.deinit();
    }

    pub fn getStatistics(self: *Consumer) Statistics {
        return self.statistics;
    }

    pub fn close(self: *Consumer) void {
        self.deinit();
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

    pub fn getRecentPollErrors(self: *Consumer) []const PartitionError {
        return self.recent_errors.items;
    }

    fn pushPollError(self: *Consumer, topic: []const u8, partition: i32, code: i16, message: ?[]const u8) void {
        self.statistics.poll_errors += 1;
        self.recent_errors.append(self.allocator, .{
            .topic = topic,
            .partition = partition,
            .error_code = code,
            .error_message = message,
        }) catch {};
    }

    fn maybeRefreshTopicOnRouteError(self: *Consumer, topic: []const u8, partition: i32, code: i16) void {
        if (code == 74 or code == 75) {
            self.cluster.clearPartitionLeaderEpoch(topic, partition);
        }

        if (isRouteRefreshError(code)) {
            _ = self.cluster.refreshTopicMetadata(topic) catch {};
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

        try self.cluster.refreshTopicMetadata(a.topic);

        const conn = try self.cluster.connectionForTopicPartition(a.topic, a.partition);
        const version = try self.cluster.version_registry.choose(.ListOffsets);
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

        var buf: [4096]u8 = undefined;
        var e = codec.Encoder.init(&buf);
        try encodeRequestHeader(&e, @intFromEnum(generated.list_offsets.api_key), version, conn.correlation_id, is_flexible);
        try req.encode(&e, version);

        const frame = try conn.call(.ListOffsets, is_flexible, e.written());
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
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
            self.maybeRefreshTopicOnRouteError(a.topic, a.partition, p.error_code);
            self.pushPollError(a.topic, a.partition, p.error_code, null);
            return error.StaleMetadata;
        }

        a.position = p.offset;
    }

    fn resolveInitialPositionWithRetry(self: *Consumer, a: *Assignment, deadline_ms: i64) !void {
        var attempt: u8 = 0;
        while (attempt < 2) : (attempt += 1) {
            self.resolveInitialPosition(a, deadline_ms) catch |err| {
                if (err != error.StaleMetadata or attempt == 1 or remainingMs(deadline_ms) <= 0) {
                    return err;
                }

                continue;
            };

            return;
        }
    }

    fn fetchPartition(self: *Consumer, a: *Assignment, deadline_ms: i64) !?generated.fetch.Response.FetchableTopicResponse.PartitionData {
        const timeout_ms = remainingMs(deadline_ms);
        if (timeout_ms <= 0) {
            return null;
        }

        const conn = try self.cluster.connectionForTopicPartition(a.topic, a.partition);
        const version = try self.cluster.version_registry.choose(.Fetch);
        const is_flexible = version >= 12;

        var req_parts = [_]generated.fetch.Request.FetchTopic.FetchPartition{
            .{
                .partition = a.partition,
                .current_leader_epoch = self.cluster.leaderEpochFor(a.topic, a.partition) orelse -1,
                .fetch_offset = a.position.?,
                .last_fetched_epoch = -1,
                .log_start_offset = -1,
                .partition_max_bytes = self.config.max_partition_fetch_bytes,
            },
        };

        var req_topics = [_]generated.fetch.Request.FetchTopic{
            .{
                .topic = a.topic,
                .partitions = &req_parts,
            },
        };

        const req = generated.fetch.Request{
            .replica_id = -1,
            .max_wait_ms = @min(self.config.fetch_max_wait_ms, timeout_ms),
            .min_bytes = self.config.fetch_min_bytes,
            .max_bytes = self.config.fetch_max_bytes,
            .isolation_level = 0,
            .session_id = 0,
            .session_epoch = -1,
            .topics = &req_topics,
            .forgotten_topics_data = &.{},
            .rack_id = "",
        };

        var buf: [8192]u8 = undefined;
        var e = codec.Encoder.init(&buf);
        try encodeRequestHeader(&e, @intFromEnum(generated.fetch.api_key), version, conn.correlation_id, is_flexible);
        try req.encode(&e, version);

        const frame = try conn.call(.Fetch, is_flexible, e.written());
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
        try decodeResponseHeader(&d, .Fetch, is_flexible);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const resp = try generated.fetch.Response.decode(arena.allocator(), &d, version);
        if (d.remaining() != 0) {
            return error.ProtocolError;
        }

        if (resp.error_code != 0) {
            self.maybeRefreshTopicOnRouteError(a.topic, a.partition, resp.error_code);
            return error.StaleMetadata;
        }

        if (resp.responses.len == 0 or resp.responses[0].partitions.len == 0) {
            return null;
        }

        return resp.responses[0].partitions[0];
    }

    fn fetchPartitionWithRetry(self: *Consumer, a: *Assignment, deadline_ms: i64) !?generated.fetch.Response.FetchableTopicResponse.PartitionData {
        var attempt: u8 = 0;
        while (attempt < 2) : (attempt += 1) {
            const part = self.fetchPartition(a, deadline_ms) catch |err| {
                if (err != error.StaleMetadata or attempt == 1 or remainingMs(deadline_ms) <= 0) {
                    return err;
                }

                continue;
            };

            return part;
        }

        return null;
    }

    pub fn poll(self: *Consumer, timeout_ms: i32) ![]const Record {
        self.statistics.poll_calls += 1;

        _ = self.poll_arena.reset(.retain_capacity);
        self.recent_errors.clearRetainingCapacity();

        var out = std.ArrayList(Record).empty;
        defer out.deinit(self.poll_arena.allocator());

        const deadline_ms = deadlineMsFromNow(timeout_ms);
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

            self.resolveInitialPosition(a, deadline_ms) catch |err| {
                self.pushPollError(a.topic, a.partition, -1, @errorName(err));
                continue;
            };

            const part = self.fetchPartition(a, deadline_ms) catch |err| {
                self.pushPollError(a.topic, a.partition, -1, @errorName(err));
                continue;
            } orelse continue;

            if (part.error_code != 0) {
                self.maybeRefreshTopicOnRouteError(a.topic, a.partition, part.error_code);
                self.pushPollError(a.topic, a.partition, part.error_code, null);
                continue;
            }

            const raw_records = part.records orelse continue;
            var cursor: usize = 0;

            while (cursor < raw_records.len) {
                var parser = batch.BatchParser.init(
                    raw_records[cursor..],
                    .{},
                    .{ .validate_crc = self.config.crc_validation_enabled },
                ) catch |err| switch (err) {
                    error.UnsupportedCompression => {
                        self.pushPollError(a.topic, a.partition, -2, "UnsupportedCompression");
                        break;
                    },
                    error.CrcMismatch => {
                        self.pushPollError(a.topic, a.partition, -3, "CrcMismatch");
                        break;
                    },
                    else => {
                        self.pushPollError(a.topic, a.partition, -4, "BatchParseError");
                        break;
                    },
                };

                if (parser.isControlBatch()) {
                    a.position = parser.base_offset + parser.last_offset_delta + 1;
                    cursor += @as(usize, @intCast(12 + parser.batch_length));
                    continue;
                }

                while (true) {
                    const next_record = parser.next(self.poll_arena.allocator()) catch break;
                    if (next_record == null) {
                        break;
                    }

                    const r = next_record.?;

                    const abs_offset = parser.base_offset + r.offset_delta;
                    if (abs_offset < a.position.?) {
                        continue;
                    }

                    const record_bytes: usize = (if (r.key) |k| k.len else 0) + (if (r.value) |v| v.len else 0);
                    for (r.headers) |h| {
                        record_bytes += h.key.len;
                        if (h.value) |v| {
                            record_bytes += v.len;
                        }
                    }

                    if (out.items.len >= self.config.max_poll_records) {
                        return out.items;
                    }

                    if (bytes_accumulator + record_bytes > self.config.max_poll_bytes) {
                        return out.items;
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

                    bytes_accumulator += record_bytes;
                    a.position = abs_offset + 1;
                }

                cursor += @as(usize, @intCast(12 + parser.batch_length));
            }
        }

        return out.items;
    }

    pub fn pollOwned(self: *Consumer, allocator: std.mem.Allocator, timeout_ms: i32) ![]OwnedRecord {
        const records = try self.poll(timeout_ms);

        var out: std.ArrayList(OwnedRecord) = .empty;
        errdefer {
            for (out.items) |r| {
                allocator.free(r.topic);
                if (r.key) |k| {
                    allocator.free(k);
                }

                if (r.value) |v| {
                    allocator.free(v);
                }

                allocator.free(r.headers);
            }
            out.deinit(allocator);
        }

        for (records) |r| {
            var headers = try allocator.alloc(RecordHeader, r.headers.len);
            for (r.headers, 0..) |h, i| {
                headers[i] = .{
                    .key = try allocator.dupe(u8, h.key),
                    .value = if (h.value) |v| try allocator.dupe(u8, v) else null,
                };
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

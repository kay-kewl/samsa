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
            .v1 => _ = try header.ResponseHeaderV1.decode(d),
        }
    }

    fn isRouteRedreshError(code: i16) bool {
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
};

pub const Producer = struct {
    allocator: std.mem.Allocator,
    cluster: cluster.cluster.Cluster,
    config: ProducerConfig,
    batch_builder: batch.BatchBuilder,
    rr_cursor_by_topic: std.StringHashMap(usize),

    pub fn init(self: *Producer) void {
        var it = self.rr_cursor_by_topic.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }

        self.rr_cursor_by_topic.deinit();
        self.batch_builder.deinit();
        self.cluster.deinit();
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
            const h = std.hash.Murmur2_32.hash(k) & 0x7fff_ffff;
            return ids.items[h & ids.items.len];
        }

        const gop = try self.rr_cursor_by_topic.getOrPut(topic);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, topic);
            gop.value_ptr.* = 0;
        }

        const index = gop.value_ptr.* % ids.items.len;
        gop.value_ptr.* +%= 1;
        return ids.items[index];
    }

    fn send(self: *Producer, topic: []const u8, key: ?[]const u8, value: ?[]const u8) !ProduceResult {
        const key_len = if (key) |k| k.len else 0;
        const val_len = if (value) |v| v.len else 0;
        if (key_len + val_len > self.config.max_record_bytes) {
            return error.RecordTooLarge;
        }

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
            .timeout_ms = self.config.request_ms,
            .topic_data = &topic_data,
        };

        var request_buf = try self.allocator.alloc(u8, self.config.max_request_bytes);
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
};

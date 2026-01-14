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

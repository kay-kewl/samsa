const std = @import("std");
const codec = @import("../codec.zig");
const crc32c = @import("../crc32c.zig");

pub const BatchError = error{
    BatchTooShort,
    InvalidMagic,
    CrcMismatch,
    UnsupportedCompression,
} || codec.CodecError;

pub const RecordHeader = struct {
    key: []const u8,
    value: ?[]const u8,
};

pub const Record = struct {
    offset_delta: i32,
    timestamp_delta: i64,
    key: ?[]const u8,
    value: ?[]const u8,
    headers: []const RecordHeader,
};

pub const ParseOptions = struct {
    validate_crc: bool = true,
};

pub const BatchParser = struct {
    decoder: codec.Decoder,
    limits: @import("../limits.zig").Limits,
    base_offset: i64,
    batch_length: i32,
    partition_leader_epoch: i32,
    magic: i8,
    crc: u32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records_count: i32,

    records_read: i32 = 0,

    pub fn init(bytes: []const u8, limits: @import("../limits.zig").Limits, options: ParseOptions) BatchError!BatchParser {
        var d = codec.Decoder.initWithLimits(bytes, limits);

        if (d.remaining() < 61) {
            return error.BatchTooShort;
        }

        const base_offset = try d.readI64();
        const batch_length = try d.readI32();

        if (batch_length < 49) {
            return error.BatchTooShort;
        }

        const total_wire_size = 12 + @as(usize, @intCast(batch_length));
        if (d.buf.len < total_wire_size) {
            return error.EndOfStream;
        }

        d.buf = bytes[0..total_wire_size];

        const partition_leader_epoch = try d.readI32();
        const magic = try d.readI8();

        if (magic != 2) {
            return error.InvalidMagic;
        }

        const crc_val = try d.readU32();
        if (options.validate_crc) {
            const crc_data = d.buf[21..total_wire_size];
            const computed_crc = crc32c.calculate(crc_data);
            if (computed_crc != crc_val) {
                return error.CrcMismatch;
            }
        }

        const attributes = try d.readI16();
        const compression = attributes & 0x07;
        if (compression != 0) {
            return error.UnsupportedCompression;
        }

        const last_offset_delta = try d.readI32();
        const base_timestamp = try d.readI64();
        const max_timestamp = try d.readI64();
        const producer_id = try d.readI64();
        const producer_epoch = try d.readI16();
        const base_sequence = try d.readI32();
        const records_count = try d.readI32();

        if (records_count < 0) {
            return error.InvalidLength;
        }
        if (@as(usize, @intCast(records_count)) > limits.max_array_elements) {
            return error.LimitExceeded;
        }

        return .{
            .decoder = d,
            .limits = limits,
            .base_offset = base_offset,
            .batch_length = batch_length,
            .partition_leader_epoch = partition_leader_epoch,
            .magic = magic,
            .crc = crc_val,
            .attributes = attributes,
            .last_offset_delta = last_offset_delta,
            .base_timestamp = base_timestamp,
            .max_timestamp = max_timestamp,
            .producer_id = producer_id,
            .producer_epoch = producer_epoch,
            .base_sequence = base_sequence,
            .records_count = records_count,
        };
    }

    pub fn next(self: *BatchParser, allocator: std.mem.Allocator) BatchError!?Record {
        if (self.records_read >= self.records_count) {
            if (self.decoder.pos != self.decoder.buf.len) {
                return error.InvalidLength;
            }

            return null;
        }

        const length = try self.decoder.readVarint32();
        if (length < 0) {
            return error.InvalidLength;
        }
        if (@as(usize, @intCast(length)) > self.decoder.remaining()) {
            return error.EndOfStream;
        }

        const record_start = self.decoder.pos;
        _ = try self.decoder.readI8();

        const timestamp_delta = try self.decoder.readVarint64();
        const offset_delta = try self.decoder.readVarint32();
        if (offset_delta < 0) {
            return error.InvalidLength;
        }

        const key_len = try self.decoder.readVarint32();
        if (key_len < -1) {
            return error.InvalidLength;
        }

        const key = if (key_len >= 0) try self.decoder.readBytes(@intCast(key_len)) else null;

        const val_len = try self.decoder.readVarint32();
        if (val_len < -1) {
            return error.InvalidLength;
        }

        const value = if (val_len >= 0) try self.decoder.readBytes(@intCast(val_len)) else null;

        const headers_count = try self.decoder.readVarint32();
        if (headers_count < 0) {
            return error.InvalidLength;
        }
        if (@as(usize, @intCast(headers_count)) > self.limits.max_array_elements) {
            return error.LimitExceeded;
        }

        var headers: std.ArrayList(RecordHeader) = .{};
        defer headers.deinit(allocator);

        var i: i32 = 0;
        while (i < headers_count) : (i += 1) {
            const h_key_len = try self.decoder.readVarint32();
            if (h_key_len < 0) {
                return error.InvalidLength;
            }

            const h_key = try self.decoder.readBytes(@intCast(h_key_len));

            const h_val_len = try self.decoder.readVarint32();
            if (h_val_len < -1) {
                return error.InvalidLength;
            }

            const h_val = if (h_val_len >= 0) try self.decoder.readBytes(@intCast(h_val_len)) else null;

            try headers.append(allocator, .{
                .key = h_key,
                .value = h_val,
            });
        }

        const bytes_read = self.decoder.pos - record_start;
        if (bytes_read != @as(usize, @intCast(length))) {
            return error.InvalidLength;
        }

        self.records_read += 1;

        return .{
            .offset_delta = offset_delta,
            .timestamp_delta = timestamp_delta,
            .key = key,
            .value = value,
            .headers = try headers.toOwnedSlice(allocator),
        };
    }

    pub fn isControlBatch(self: *const BatchParser) bool {
        return (self.attributes & (1 << 5)) != 0;
    }
};

pub const BatchBuilder = struct {
    list: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) BatchBuilder {
        return .{
            .list = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BatchBuilder) void {
        self.list.deinit(self.allocator);
    }

    pub fn buildSingleRecord(self: *BatchBuilder, timestamp: i64, key: ?[]const u8, value: ?[]const u8) ![]const u8 {
        self.list.clearRetainingCapacity();
        try self.list.appendNTimes(self.allocator, 0, 61);

        const record_start = 61;
        self.list.shrinkRetainingCapacity(record_start);

        const key_len = if (key) |k| k.len else 0;
        const val_len = if (value) |v| v.len else 0;
        const estimated = 32 + key_len + val_len;

        const record_buf = try self.allocator.alloc(u8, estimated);
        defer self.allocator.free(record_buf);
        var record_e = codec.Encoder.init(record_buf);
        try record_e.writeI8(0);
        try record_e.writeVarint64(0);
        try record_e.writeVarint32(0);

        if (key) |k| {
            try record_e.writeVarint32(@intCast(k.len));
            for (k) |b| {
                try record_e.writeI8(@bitCast(b));
            }
        } else {
            try record_e.writeVarint32(-1);
        }

        if (value) |v| {
            try record_e.writeVarint32(@intCast(v.len));
            for (v) |b| {
                try record_e.writeI8(@bitCast(b));
            }
        } else {
            try record_e.writeVarint32(-1);
        }

        try record_e.writeVarint32(0);
        const payload_len = record_e.pos;

        var len_buf: [5]u8 = undefined;
        var len_e = codec.Encoder.init(&len_buf);
        try len_e.writeVarint32(@intCast(payload_len));

        try self.list.appendSlice(self.allocator, len_e.written());
        try self.list.appendSlice(self.allocator, record_e.written());

        var header_e = codec.Encoder.init(self.list.items[0..61]);
        const batch_length = @as(i32, @intCast(self.list.items.len - 12));

        try header_e.writeI64(0);
        try header_e.writeI32(batch_length);
        try header_e.writeI32(-1);
        try header_e.writeI8(2);

        const crc_pos = header_e.pos;

        try header_e.writeU32(0);
        try header_e.writeI16(0);
        try header_e.writeI32(0);
        try header_e.writeI64(timestamp);
        try header_e.writeI64(timestamp);
        try header_e.writeI64(-1);
        try header_e.writeI16(-1);
        try header_e.writeI32(-1);
        try header_e.writeI32(1);

        const crc_data = self.list.items[21..self.list.items.len];
        const computed_crc = crc32c.calculate(crc_data);

        self.list.items[crc_pos] = @as(u8, @truncate(computed_crc >> 24));
        self.list.items[crc_pos + 1] = @as(u8, @truncate(computed_crc >> 16));
        self.list.items[crc_pos + 2] = @as(u8, @truncate(computed_crc >> 8));
        self.list.items[crc_pos + 3] = @as(u8, @truncate(computed_crc));

        return self.list.items;
    }
};

const testing = std.testing;

test "BatchBuilder to BatchParser round-trip" {
    var builder = BatchBuilder.init(testing.allocator);
    defer builder.deinit();

    const timestamp: i64 = 1_000_000_000_000;
    const batch_bytes = try builder.buildSingleRecord(timestamp, "test-key", "test-value");

    var parser = try BatchParser.init(batch_bytes, .{}, .{});

    try testing.expectEqual(@as(i64, 0), parser.base_offset);
    try testing.expectEqual(@as(i8, 2), parser.magic);
    try testing.expectEqual(@as(i16, 0), parser.attributes);
    try testing.expectEqual(@as(i32, 1), parser.records_count);
    try testing.expectEqual(timestamp, parser.base_timestamp);

    const record = (try parser.next(testing.allocator)).?;
    defer testing.allocator.free(record.headers);

    try testing.expectEqualStrings("test-key", record.key.?);
    try testing.expectEqualStrings("test-value", record.value.?);
    try testing.expectEqual(@as(i32, 0), record.offset_delta);
    try testing.expectEqual(@as(i64, 0), record.timestamp_delta);

    try testing.expectEqual(@as(?Record, null), try parser.next(testing.allocator));
}

test "BatchParser rejects trailing bytes after declared records" {
    var builder = BatchBuilder.init(testing.allocator);
    defer builder.deinit();

    const batch_bytes_const = try builder.buildSingleRecord(1_000_000_000_000, "k", "v");

    var owned = try testing.allocator.alloc(u8, batch_bytes_const.len + 1);
    defer testing.allocator.free(owned);
    @memcpy(owned[0..batch_bytes_const.len], batch_bytes_const);
    owned[owned.len - 1] = 0;

    const old_batch_length = std.mem.readInt(i32, owned[8..12], .big);
    std.mem.writeInt(i32, owned[8..12], old_batch_length + 1, .big);

    const new_crc = crc32c.calculate(owned[21..owned.len]);
    owned[17] = @as(u8, @truncate(new_crc >> 24));
    owned[18] = @as(u8, @truncate(new_crc >> 16));
    owned[19] = @as(u8, @truncate(new_crc >> 8));
    owned[20] = @as(u8, @truncate(new_crc));

    var parser = try BatchParser.init(owned, .{}, .{});
    _ = (try parser.next(testing.allocator)).?;
    try testing.expectError(error.InvalidLength, parser.next(testing.allocator));
}

test "BatchParser rejects crc mismatch" {
    var builder = BatchBuilder.init(testing.allocator);
    defer builder.deinit();

    const timestamp: i64 = 1_000_000_000_000;
    const batch_bytes_const = try builder.buildSingleRecord(timestamp, "k", "v");

    var owned = try testing.allocator.alloc(u8, batch_bytes_const.len);
    defer testing.allocator.free(owned);
    @memcpy(owned, batch_bytes_const);

    owned[30] ^= 0x01;

    try testing.expectError(error.CrcMismatch, BatchParser.init(owned, .{}, .{}));
}

test "BatchParser rejects compressed attributes" {
    var builder = BatchBuilder.init(testing.allocator);
    defer builder.deinit();

    const timestamp: i64 = 1_000_000_000_000;
    const batch_bytes_const = try builder.buildSingleRecord(timestamp, "k", "v");

    var owned = try testing.allocator.alloc(u8, batch_bytes_const.len);
    defer testing.allocator.free(owned);
    @memcpy(owned, batch_bytes_const);

    owned[21] = 0x00;
    owned[22] = 0x01;

    const new_crc = crc32c.calculate(owned[21..owned.len]);
    owned[17] = @as(u8, @truncate(new_crc >> 24));
    owned[18] = @as(u8, @truncate(new_crc >> 16));
    owned[19] = @as(u8, @truncate(new_crc >> 8));
    owned[20] = @as(u8, @truncate(new_crc));

    try testing.expectError(error.UnsupportedCompression, BatchParser.init(owned, .{}, .{}));
}

test "BatchParser rejects records_count above max_array_elements" {
    var builder = BatchBuilder.init(testing.allocator);
    defer builder.deinit();

    const bytes = try builder.buildSingleRecord(1_000_000_000_000, "k", "v");
    try testing.expectError(error.LimitExceeded, BatchParser.init(bytes, .{ .max_array_elements = 0 }, .{}));
}

test "BatchParser rejects headers_count above max_array_elements" {
    var builder = BatchBuilder.init(testing.allocator);
    defer builder.deinit();

    const bytes_const = try builder.buildSingleRecord(1_000_000_000_000, "k", "v");
    var bytes = try testing.allocator.alloc(u8, bytes_const.len);
    defer testing.allocator.free(bytes);
    @memcpy(bytes, bytes_const);

    bytes[bytes.len - 1] = 0x04;

    const new_crc = crc32c.calculate(bytes[21..bytes.len]);
    bytes[17] = @as(u8, @truncate(new_crc >> 24));
    bytes[18] = @as(u8, @truncate(new_crc >> 16));
    bytes[19] = @as(u8, @truncate(new_crc >> 8));
    bytes[20] = @as(u8, @truncate(new_crc));

    var parser = try BatchParser.init(bytes, .{ .max_array_elements = 1 }, .{});
    try testing.expectError(error.LimitExceeded, parser.next(testing.allocator));
}

test "BatchBuilder to BatchParser round-trip large value" {
    var builder = BatchBuilder.init(testing.allocator);
    defer builder.deinit();

    const value = try testing.allocator.alloc(u8, 128 * 1024);
    defer testing.allocator.free(value);
    @memset(value, 'x');

    const batch_bytes = try builder.buildSingleRecord(1_000_000_000_000, "oversized-key", value);

    var parser = try BatchParser.init(batch_bytes, .{}, .{});
    const record = (try parser.next(testing.allocator)).?;
    defer testing.allocator.free(record.headers);

    try testing.expectEqualStrings("oversized-key", record.key.?);
    try testing.expectEqual(@as(usize, value.len), record.value.?.len);
    try testing.expectEqualSlices(u8, value, record.value.?);
    try testing.expectEqual(@as(?Record, null), try parser.next(testing.allocator));
}

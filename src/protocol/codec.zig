const std = @import("std");
const limits = @import("limits.zig");

pub const CodecError = error{
    EndOfStream,
    NoSpace,
    InvalidLength,
    InvalidVariant,
    Overflow,
    OutOfMemory,
    LimitExceeded,
};

pub const Uuid = [16]u8;

fn UIntOf(comptime T: type) type {
    return std.meta.Int(.unsigned, @bitSizeOf(T));
}

fn readIntBE(comptime T: type, bytes: []const u8) T {
    const U = UIntOf(T);
    var u: U = 0;
    for (bytes) |b| {
        u = (u << 8) | @as(U, b);
    }

    return @bitCast(u);
}

fn writeIntBE(comptime T: type, out: []u8, value: T) void {
    const U = UIntOf(T);
    var u: U = @bitCast(value);
    var i: usize = 0;
    while (i < out.len) : (i += 1) {
        out[out.len - 1 - i] = @as(u8, @truncate(u));
        u >>= 8;
    }
}

pub fn u32Size(_: u32) usize {
    return 4;
}

pub fn uvarintSize32(v: u32) usize {
    var x = v;
    var n: usize = 1;
    while (x >= 0x80) : (x >>= 7) {
        n += 1;
    }

    return n;
}

pub fn uvarintSize64(v: u64) usize {
    var x = v;
    var n: usize = 1;
    while (x >= 0x80) : (x >>= 7) {
        n += 1;
    }

    return n;
}

pub fn varintSize32(v: i32) usize {
    return uvarintSize32(@as(u32, @intCast((v << 1) ^ (v >> 31))));
}

pub fn varintSize64(v: i64) usize {
    return uvarintSize64(@as(u64, @intCast((v << 1) ^ (v >> 63))));
}

pub fn stringSize(v: ?[]const u8) usize {
    if (v) |s| {
        return 2 + s.len;
    }

    return 2; // -1
}

pub fn compactStringSize(v: ?[]const u8) usize {
    if (v) |s| {
        return uvarintSize32(@as(u32, @intCast(s.len)) + 1) + s.len;
    }

    return 1; // 0
}

pub fn bytesSize(v: ?[]const u8) usize {
    if (v) |s| {
        return 4 + s.len;
    }

    return 4;
}

pub fn compactBytesSize(v: ?[]const u8) usize {
    return compactStringSize(v);
}

pub const Decoder = struct {
    buf: []const u8,
    pos: usize = 0,
    limits: limits.Limits = .{},

    pub fn init(buf: []const u8) Decoder {
        return .{
            .buf = buf,
        };
    }

    pub fn initWithLimits(buf: []const u8, l: limits.Limits) Decoder {
        return .{
            .buf = buf,
            .limits = l,
        };
    }

    pub fn remaining(self: *const Decoder) usize {
        return self.buf.len - self.pos;
    }

    fn require(self: *const Decoder, n: usize) CodecError!void {
        if (self.remaining() < n) {
            return error.EndOfStream;
        }
    }

    pub fn readBytes(self: *Decoder, n: usize) CodecError![]const u8 {
        try self.require(n);
        const out = self.buf[self.pos .. self.pos + n];
        self.pos += n;
        return out;
    }

    pub fn readI8(self: *Decoder) CodecError!i8 {
        return @bitCast((try self.readBytes(1))[0]);
    }

    pub fn readI16(self: *Decoder) CodecError!i16 {
        return readIntBE(i16, try self.readBytes(2));
    }

    pub fn readI32(self: *Decoder) CodecError!i32 {
        return readIntBE(i32, try self.readBytes(4));
    }

    pub fn readI64(self: *Decoder) CodecError!i64 {
        return readIntBE(i64, try self.readBytes(8));
    }

    pub fn readU32(self: *Decoder) CodecError!u32 {
        return readIntBE(u32, try self.readBytes(4));
    }

    pub fn readUVarint32(self: *Decoder) CodecError!u32 {
        var shift: u6 = 0;
        var out: u32 = 0;
        while (true) {
            if (shift >= 35) {
                return error.InvalidVariant;
            }

            const b = (try self.readBytes(1))[0];
            const byte_val: u32 = b & 0x7f;
            const shift_u5: u5 = @intCast(shift);
            if (byte_val << shift_u5 >> shift_u5 != byte_val) {
                return error.Overflow;
            }

            out |= byte_val << shift_u5;
            if ((b & 0x80) == 0) {
                return out;
            }

            shift += 7;
        }
    }

    pub fn readUVarint64(self: *Decoder) CodecError!u64 {
        var shift: u6 = 0;
        var out: u64 = 0;
        while (true) {
            if (shift >= 70) {
                return error.InvalidVariant;
            }

            const b = (try self.readBytes(1))[0];
            const byte_val: u64 = b & 0x7f;
            const shift_u6: u6 = @intCast(shift);
            if (byte_val << shift_u6 >> shift_u6 != byte_val) {
                return error.Overflow;
            }

            out |= byte_val << shift_u6;
            if ((b & 0x80) == 0) {
                return out;
            }

            shift += 7;
        }
    }

    pub fn readVarint32(self: *Decoder) CodecError!i32 {
        const u = try self.readUVarint32();
        return @as(i32, @intCast(u >> 1)) ^ -@as(i32, @intCast(u & 1));
    }

    pub fn readVarint64(self: *Decoder) CodecError!i64 {
        const u = try self.readUVarint64();
        return @as(i64, @intCast(u >> 1)) ^ -@as(i64, @intCast(u & 1));
    }

    pub fn readFloat64(self: *Decoder) CodecError!f64 {
        const bytes = try self.readBytes(8);
        const u = readIntBE(u64, bytes);
        return @bitCast(u);
    }

    pub fn readBoolean(self: *Decoder) CodecError!bool {
        const b = try self.readI8();
        return b != 0;
    }

    pub fn readUuid(self: *Decoder) CodecError!Uuid {
        const bytes = try self.readBytes(16);
        var u: Uuid = undefined;
        @memcpy(&u, bytes);
        return u;
    }

    // reads i16 length, then bytes, returns slice into buffer
    // returns null if length is -1 and error if less than -1
    pub fn readNullableString(self: *Decoder) CodecError!?[]const u8 {
        const len = try self.readI16();
        if (len == -1) {
            return null;
        }

        if (len < -1) {
            return error.InvalidLength;
        }

        const size = @as(usize, @intCast(len));
        if (size > self.limits.max_string_bytes) {
            return error.LimitExceeded;
        }

        return self.readBytes(size);
    }

    // reads i16 length, then bytes, returns slice into buffer
    pub fn readString(self: *Decoder) CodecError![]const u8 {
        const len = try self.readI16();
        if (len < 0) {
            return error.InvalidLength;
        }

        const size = @as(usize, @intCast(len));
        if (size > self.limits.max_string_bytes) {
            return error.LimitExceeded;
        }

        return self.readBytes(size);
    }

    // reads unsigned varint length n, then then n - 1 bytes
    // returns null if 0, empty string if 1, etc
    pub fn readCompactNullableString(self: *Decoder) CodecError!?[]const u8 {
        const n = try self.readUVarint32();
        if (n == 0) {
            return null;
        }

        const size = n - 1;
        if (size > self.limits.max_string_bytes) {
            return error.LimitExceeded;
        }

        return self.readBytes(size);
    }

    // reads unsigned varint length n, then then n - 1 bytes
    pub fn readCompactString(self: *Decoder) CodecError![]const u8 {
        const n = try self.readUVarint32();
        if (n == 0) {
            return error.InvalidLength;
        }

        const size = n - 1;
        if (size > self.limits.max_string_bytes) {
            return error.LimitExceeded;
        }

        return self.readBytes(size);
    }

    // reads i32 length, then bytes, returns slice into buffer
    // returns null if length is -1 and error if less than -1
    pub fn readNullableBytesArray(self: *Decoder) CodecError!?[]const u8 {
        const len = try self.readI32();
        if (len == -1) {
            return null;
        }

        if (len < -1) {
            return error.InvalidLength;
        }

        const size = @as(usize, @intCast(len));
        if (size > self.limits.max_bytes_field_bytes) {
            return error.LimitExceeded;
        }

        return self.readBytes(size);
    }

    // reads i32 length, then bytes, returns slice into buffer
    pub fn readBytesArray(self: *Decoder) CodecError![]const u8 {
        const len = try self.readI32();
        if (len < 0) {
            return error.InvalidLength;
        }

        const size = @as(usize, @intCast(len));
        if (size > self.limits.max_bytes_field_bytes) {
            return error.LimitExceeded;
        }

        return self.readBytes(size);
    }

    // for consistency
    pub fn readCompactNullableBytesArray(self: *Decoder) CodecError!?[]const u8 {
        return self.readCompactNullableString();
    }

    // for consistency
    pub fn readCompactBytesArray(self: *Decoder) CodecError![]const u8 {
        return self.readCompactString();
    }

    pub fn readNullableArrayLength(self: *Decoder) CodecError!?u32 {
        const len = try self.readI32();
        if (len == -1) {
            return null;
        }

        if (len < -1) {
            return error.InvalidLength;
        }

        const count = @as(u32, @intCast(len));
        if (count > self.limits.max_array_elements) {
            return error.LimitExceeded;
        }

        return count;
    }

    pub fn readArrayLength(self: *Decoder) CodecError!u32 {
        const len = try self.readI32();
        if (len < 0) {
            return error.InvalidLength;
        }

        const count = @as(u32, @intCast(len));
        if (count > self.limits.max_array_elements) {
            return error.LimitExceeded;
        }

        return count;
    }

    pub fn readCompactNullableArrayLength(self: *Decoder) CodecError!?u32 {
        const n = try self.readUVarint32();
        if (n == 0) {
            return null;
        }

        const count = n - 1;
        if (count > self.limits.max_array_elements) {
            return error.LimitExceeded;
        }

        return count;
    }

    pub fn readCompactArrayLength(self: *Decoder) CodecError!u32 {
        const n = try self.readUVarint32();
        if (n == 0) {
            return error.InvalidLength;
        }

        const count = n - 1;
        if (count > self.limits.max_array_elements) {
            return error.LimitExceeded;
        }

        return count;
    }
};

pub const Encoder = struct {
    buf: []u8,
    pos: usize = 0,

    pub fn init(buf: []u8) Encoder {
        return .{
            .buf = buf,
        };
    }

    pub fn written(self: *const Encoder) []const u8 {
        return self.buf[0..self.pos];
    }

    pub fn writeI8(self: *Encoder, v: i8) CodecError!void {
        if (self.pos + 1 > self.buf.len) {
            return error.NoSpace;
        }

        self.buf[self.pos] = @bitCast(v);
        self.pos += 1;
    }

    pub fn writeI16(self: *Encoder, v: i16) CodecError!void {
        if (self.pos + 2 > self.buf.len) {
            return error.NoSpace;
        }

        writeIntBE(i16, self.buf[self.pos .. self.pos + 2], v);
        self.pos += 2;
    }

    pub fn writeI32(self: *Encoder, v: i32) CodecError!void {
        if (self.pos + 4 > self.buf.len) {
            return error.NoSpace;
        }

        writeIntBE(i32, self.buf[self.pos .. self.pos + 4], v);
        self.pos += 4;
    }

    pub fn writeI64(self: *Encoder, v: i64) CodecError!void {
        if (self.pos + 8 > self.buf.len) {
            return error.NoSpace;
        }

        writeIntBE(i64, self.buf[self.pos .. self.pos + 8], v);
        self.pos += 8;
    }

    pub fn writeU32(self: *Encoder, v: u32) CodecError!void {
        if (self.pos + 4 > self.buf.len) {
            return error.NoSpace;
        }

        writeIntBE(u32, self.buf[self.pos .. self.pos + 4], v);
        self.pos += 4;
    }

    pub fn writeUVarint32(self: *Encoder, v: u32) CodecError!void {
        var x = v;
        while (x >= 0x80) : (x >>= 7) {
            if (self.pos >= self.buf.len) {
                return error.NoSpace;
            }

            self.buf[self.pos] = @as(u8, @intCast((x & 0x7f) | 0x80));
            self.pos += 1;
        }

        if (self.pos >= self.buf.len) {
            return error.NoSpace;
        }

        self.buf[self.pos] = @as(u8, @intCast(x));
        self.pos += 1;
    }

    pub fn writeUVarint64(self: *Encoder, v: u64) CodecError!void {
        var x = v;
        while (x >= 0x80) : (x >>= 7) {
            if (self.pos >= self.buf.len) {
                return error.NoSpace;
            }

            self.buf[self.pos] = @as(u8, @intCast((x & 0x7f) | 0x80));
            self.pos += 1;
        }

        if (self.pos >= self.buf.len) {
            return error.NoSpace;
        }

        self.buf[self.pos] = @as(u8, @intCast(x));
        self.pos += 1;
    }

    pub fn writeVarint32(self: *Encoder, v: i32) CodecError!void {
        try self.writeUVarint32(@as(u32, @intCast((v << 1) ^ (v >> 31))));
    }

    pub fn writeVarint64(self: *Encoder, v: i64) CodecError!void {
        try self.writeUVarint64(@as(u64, @intCast((v << 1) ^ (v >> 63))));
    }

    pub fn writeFloat64(self: *Encoder, v: f64) CodecError!void {
        if (self.pos + 8 > self.buf.len) {
            return error.NoSpace;
        }

        const u: u64 = @bitCast(v);
        writeIntBE(u64, self.buf[self.pos .. self.pos + 8], u);
        self.pos += 8;
    }

    pub fn writeBoolean(self: *Encoder, v: bool) CodecError!void {
        try self.writeI8(if (v) 1 else 0);
    }

    pub fn writeUuid(self: *Encoder, v: Uuid) CodecError!void {
        if (self.pos + 16 > self.buf.len) {
            return error.NoSpace;
        }

        @memcpy(self.buf[self.pos .. self.pos + 16], &v);
        self.pos += 16;
    }

    pub fn writeNullableString(self: *Encoder, v: ?[]const u8) CodecError!void {
        if (v) |s| {
            try self.writeString(s);
        } else {
            try self.writeI16(-1);
        }
    }

    pub fn writeString(self: *Encoder, v: []const u8) CodecError!void {
        if (v.len > std.math.maxInt(i16)) {
            return error.Overflow;
        }

        try self.writeI16(@as(i16, @intCast(v.len)));
        if (self.pos + v.len > self.buf.len) {
            return error.NoSpace;
        }

        @memcpy(self.buf[self.pos .. self.pos + v.len], v);
        self.pos += v.len;
    }

    pub fn writeCompactNullableString(self: *Encoder, v: ?[]const u8) CodecError!void {
        if (v) |s| {
            try self.writeCompactString(s);
        } else {
            try self.writeUVarint32(0);
        }
    }

    pub fn writeCompactString(self: *Encoder, v: []const u8) CodecError!void {
        try self.writeUVarint32(@as(u32, @intCast(v.len)) + 1);
        if (self.pos + v.len > self.buf.len) {
            return error.NoSpace;
        }

        @memcpy(self.buf[self.pos .. self.pos + v.len], v);
        self.pos += v.len;
    }

    pub fn writeNullableBytesArray(self: *Encoder, v: ?[]const u8) CodecError!void {
        if (v) |s| {
            try self.writeBytesArray(s);
        } else {
            try self.writeI32(-1);
        }
    }

    pub fn writeBytesArray(self: *Encoder, v: []const u8) CodecError!void {
        if (v.len > std.math.maxInt(i32)) {
            return error.Overflow;
        }

        try self.writeI32(@as(i32, @intCast(v.len)));
        if (self.pos + v.len > self.buf.len) {
            return error.NoSpace;
        }

        @memcpy(self.buf[self.pos .. self.pos + v.len], v);
        self.pos += v.len;
    }

    pub fn writeCompactNullableBytesArray(self: *Encoder, v: ?[]const u8) CodecError!void {
        try self.writeCompactNullableString(v);
    }

    pub fn writeCompactBytesArray(self: *Encoder, v: []const u8) CodecError!void {
        try self.writeCompactString(v);
    }

    pub fn writeNullableArrayLength(self: *Encoder, length: ?usize) CodecError!void {
        if (length) |len| {
            if (len > std.math.maxInt(i32)) {
                return error.Overflow;
            }

            try self.writeI32(@as(i32, @intCast(len)));
        } else {
            try self.writeI32(-1);
        }
    }

    pub fn writeArrayLength(self: *Encoder, length: usize) CodecError!void {
        if (length > std.math.maxInt(i32)) {
            return error.Overflow;
        }

        try self.writeI32(@as(i32, @intCast(length)));
    }

    pub fn writeCompactNullableArray(self: *Encoder, length: ?usize) CodecError!void {
        if (length) |len| {
            try self.writeUVarint32(@as(u32, @intCast(len)) + 1);
        } else {
            try self.writeUVarint32(0);
        }
    }

    pub fn writeCompactArray(self: *Encoder, length: usize) CodecError!void {
        try self.writeUVarint32(@as(u32, @intCast(length)) + 1);
    }
};

const testing = std.testing;

test "Decoder: fixed integers" {
    const buf = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F };
    var d = Decoder.init(&buf);

    try testing.expectEqual(@as(i8, 1), try d.readI8());
    try testing.expectEqual(@as(i16, 0x0203), try d.readI16());
    try testing.expectEqual(@as(i32, 0x04050607), try d.readI32());
    try testing.expectEqual(@as(i64, 0x08090A0B0C0D0E0F), try d.readI64());

    try testing.expectEqual(15, d.pos);
}

test "Encoder: fixed integers" {
    var buf: [16]u8 = undefined;
    var e = Encoder.init(&buf);

    try e.writeI8(10);
    try e.writeI16(20);
    try e.writeI32(30);

    const written_bytes = e.written();
    try testing.expectEqualSlices(u8, &[_]u8{ 10, 0, 20, 0, 0, 0, 30 }, written_bytes);
}

test "Varint: unsigned 32 roundtrip" {
    const cases = [_]u32{ 0, 1, 127, 128, 16383, 16384, std.math.maxInt(u32) };
    var buf: [16]u8 = undefined;

    for (cases) |val| {
        var e = Encoder.init(&buf);
        try e.writeUVarint32(val);

        var d = Decoder.init(e.written());
        const result = try d.readUVarint32();
        try testing.expectEqual(val, result);
    }
}

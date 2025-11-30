const std = @import("std");

pub const CodecError = error{
    EndOfStream,
    NoSpace,
    InvalidLength,
    InvalidVariant,
    Overflow,
    OutOfMemory,
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

pub fn uvarintSize32(v: u32) usize {
    var x = v;
    var n: usize = 1;
    while (x >= 0x80) : (x >>= 7) {
        n += 1;
    }

    return n;
}

pub const Decoder = struct {
    buf: []const u8,
    pos: usize = 0,

    pub fn init(buf: []const u8) Decoder {
        return .{
            .buf = buf,
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

    pub fn readUVarint32(self: *Decoder) CodecError!u32 {
        var shift: u5 = 0;
        var out: u32 = 0;
        while (true) {
            if (shift >= 35) {
                return error.InvalidVarint;
            }

            const b = (try self.readBytes(1))[0];
            out |= (@as(u32, b & 0x7f) << shift);
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

    pub fn writeUVarint32(self: *Encoder, v: u32) CodecError!void {
        var x = v;
        while (x >= 0x80) : (x >>= 7) {
            if (self.pos >= self.buf.len) {
                return error.NoSpace;
            }

            self.buf[self.pos] = @as(u8, @intCast((x & 0x7f) | 0x80));
            self.pos += 1;
        }
    }

    pub fn writeVarint32(self: *Encoder, v: i32) CodecError!void {
        try self.writeUVarint32(@as(u32, @intCast((v << 1) ^ (v >> 31))));
    }
};

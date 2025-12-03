const std = @import("std");
const codec = @import("codec.zig");

pub const TaggedFieldError = error{
    InvalidTagOrder,
    DuplicateTag,
    TagTooLarge,
};

pub fn begin(e: *codec.Encoder, count: u32) codec.CodecError!void {
    try e.writeUVarint32(count);
}

pub fn writeEmpty(e: *codec.Encoder) codec.CodecError!void {
    try e.writeUVarint32(0);
}

pub fn writeRaw(e: *codec.Encoder, tag: u32, payload: []const u8) codec.CodecError!void {
    try e.writeUVarint32(tag);
    try e.writeUVarint32(@as(u32, @intCast(payload.len)));
    if (e.pos + payload.len > e.buf.len) {
        return error.NoSpace;
    }

    @memcpy(e.buf[e.pos .. e.pos + payload.len], payload);
    e.pos += payload.len;
}

pub fn skipAll(d: *codec.Decoder) !void {
    const count = try d.readUVarint32();
    if (count == 0) {
        return;
    }

    var last_tag: ?u32 = null;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const tag = try d.readUVarint32();
        if (last_tag) |prev| {
            if (tag == prev) {
                return error.DuplicateTag;
            } else if (tag < prev) {
                return error.InvalidTagOrder;
            }
        }

        last_tag = tag;
        const size_u32 = try d.readUVarint32();
        const size = @as(usize, @intCast(size_u32));
        if (size > d.limits.max_tagged_field_bytes) {
            return error.TagTooLarge;
        }

        _ = try d.readBytes(size);
    }

    return;
}

const testing = std.testing;

test "TaggedFields: writeEmpty" {
    var buf: [16]u8 = undefined;
    var e = codec.Encoder.init(&buf);

    try writeEmpty(&e);

    try testing.expectEqualSlices(u8, &[_]u8{0}, e.written());
}

test "TaggedFields: skipAll handles empty" {
    var buf: [16]u8 = undefined;
    var e = codec.Encoder.init(&buf);

    try writeEmpty(&e);

    var d = codec.Decoder.init(e.written());
    try skipAll(&d);
    try testing.expectEqual(0, d.remaining());
}

test "TaggedFields: skipAll skips unknown tags" {
    var buf: [64]u8 = undefined;
    var e = codec.Encoder.init(&buf);

    try e.writeUVarint32(2);

    try e.writeUVarint32(100);
    try e.writeUVarint32(2);
    try e.writeI8('A');
    try e.writeI8('B');

    try e.writeUVarint32(200);
    try e.writeUVarint32(1);
    try e.writeI8('C');

    var d = codec.Decoder.init(e.written());
    try skipAll(&d);
    try testing.expectEqual(0, d.remaining());
}

test "TaggedFields: skipAll rejects out of order" {
    var buf: [64]u8 = undefined;
    var e = codec.Encoder.init(&buf);

    try e.writeUVarint32(2);

    try e.writeUVarint32(200);
    try e.writeUVarint32(1);
    try e.writeI8('C');

    try e.writeUVarint32(100);
    try e.writeUVarint32(2);
    try e.writeI8('A');
    try e.writeI8('B');

    var d = codec.Decoder.init(e.written());
    const err = skipAll(&d);
    try testing.expectError(error.InvalidTagOrder, err);
}

test "TaggedFileds: skipAll rejects duplicates" {
    var buf: [64]u8 = undefined;
    var e = codec.Encoder.init(&buf);

    try e.writeUVarint32(2);

    try e.writeUVarint32(100);
    try e.writeUVarint32(1);
    try e.writeI8('A');

    try e.writeUVarint32(100);
    try e.writeUVarint32(1);
    try e.writeI8('B');

    var d = codec.Decoder.init(e.written());
    const err = skipAll(&d);
    try testing.expectError(error.DuplicateTag, err);
}

test "TaggedFields: skipAll enforces max size" {
    var buf: [64]u8 = undefined;
    var e = codec.Encoder.init(&buf);

    try e.writeUVarint32(1);

    try e.writeUVarint32(1);
    try e.writeUVarint32(10);

    var d = codec.Decoder.init(e.written());
    d.limits.max_tagged_field_bytes = 5;

    const err = skipAll(&d);
    try testing.expectError(error.TagTooLarge, err);
}

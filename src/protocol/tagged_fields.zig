const std = @import("std");
const codec = @import("codec.zig");

pub fn writeEmpty(e: *codec.Encoder) codec.CodecError!void {
    try e.writeUVarint32(0);
}

pub fn skipAll(d: *codec.Decoder) !void {
    const count = try d.readUVarint32();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        _ = try d.readUVarint32();
        const size = try d.readUVarint32();
        _ = try d.readBytes(size);
    }
}

const testing = std.testing;

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

    var d = codec.Decoder.init(e.written());
    try skipAll(&d);
    try testing.expectEqual(0, d.remaining());
}

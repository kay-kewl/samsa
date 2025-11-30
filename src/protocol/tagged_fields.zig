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
        const size = try .readUVarint32();
        _ = try d.readBytes(size);
    }
}

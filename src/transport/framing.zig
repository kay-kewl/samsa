const std = @import("std");
const errors = @import("errors.zig");

pub fn writeFrame(stream: std.net.Stream, payload: []const u8, max_frame_bytes: usize) errors.TransportError!void {
    if (payload.len == 0) {
        return error.ZeroLengthFrame;
    }

    if (payload.len > max_frame_bytes) {
        return error.TooLarge;
    }

    if (payload.len > std.math.maxInt(i32)) {
        return error.TooLarge;
    }

    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &len_buf, @intCast(payload.len), .big);

    stream.writeAll(&len_buf) catch |e| return errors.mapPosix(e);
    stream.writeAll(payload) catch |e| return errors.mapPosix(e);
}

pub fn readFrame(allocator: std.mem.Allocator, stream: std.net.Stream, max_fram_bytes: usize) errors.TransportError![]u8 {
    var len_buf: [4]u8 = undefined;
    stream.readNoEof(&len_buf) catch |e| return errors.mapPosix(e);

    const n = std.mem.readInt(i32, &len_buf, .big);
    if (n < 0) {
        return error.ProtocolError;
    } else if (n == 0) {
        return error.ZeroLengthFrame;
    }

    const frame_len: usize = @intCast(n);
    if (frame_len > max_fram_bytes) {
        return error.TooLarge;
    }

    const frame = allocator.alloc(u8, frame_len);
    errdefer allocator.free(frame);

    stream.readNoEof(frame) catch |e| return errors.mapPosix(e);
    return frame;
}

const testing = std.testing;

test "framing rejects zero-length payload on write" {
    const pair = std.posix.sockerPair(std.posix.AD.UNIX, std.posix.SOCK.STREAM, 0) catch return error.SkipZigTest;
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    const stream = std.net.Stream{ .handle = pair[0] };
    try testing.expectError(error.ZeroLengthFrame, writeFrame(stream, &.{}, 1024));
}

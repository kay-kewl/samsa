const std = @import("std");
const builtin = @import("builtin");
const errors = @import("errors.zig");

fn socketPairStream() ![2]std.posix.fd_t {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    var fds: [2]i32 = undefined;
    const raw_syscall = std.os.linux.socketpair(@as(i32, std.posix.AF.UNIX), @as(i32, std.posix.SOCK.STREAM), 0, &fds);

    switch (std.posix.errno(raw_syscall)) {
        .SUCCESS => return .{
            @as(std.posix.fd_t, @intCast(fds[0])),
            @as(std.posix.fd_t, @intCast(fds[1])),
        },
        else => return error.SkipZigTest,
    }
}

fn readExact(stream: std.net.Stream, buf: []u8) errors.TransportError!void {
    var offset: usize = 0;
    while (offset < buf.len) {
        const n = stream.read(buf[offset..]) catch |e| return errors.mapPosix(e);
        if (n == 0) {
            return error.EndOfStream;
        }

        offset += n;
    }
}

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

pub fn readFrame(allocator: std.mem.Allocator, stream: std.net.Stream, max_frame_bytes: usize) errors.TransportError![]u8 {
    var len_buf: [4]u8 = undefined;
    try readExact(stream, &len_buf);

    const n = std.mem.readInt(i32, &len_buf, .big);
    if (n < 0) {
        return error.ProtocolError;
    } else if (n == 0) {
        return error.ZeroLengthFrame;
    }

    const frame_len: usize = @intCast(n);
    if (frame_len > max_frame_bytes) {
        return error.TooLarge;
    }

    const frame = allocator.alloc(u8, frame_len) catch return error.Unexpected;
    errdefer allocator.free(frame);

    try readExact(stream, frame);
    return frame;
}

const testing = std.testing;

test "framing rejects zero-length payload on write" {
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    const stream = std.net.Stream{ .handle = pair[0] };
    try testing.expectError(error.ZeroLengthFrame, writeFrame(stream, &.{}, 1024));
}

test "framing read rejects zero-length frame" {
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    const reader = std.net.Stream{ .handle = pair[0] };
    const writer = std.net.Stream{ .handle = pair[1] };

    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &len_buf, 0, .big);
    writer.writeAll(&len_buf) catch return error.SkipZigTest;

    try testing.expectError(error.ZeroLengthFrame, readFrame(testing.allocator, reader, 1024));
}

test "framing read rejects negative-length frame" {
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    const reader = std.net.Stream{ .handle = pair[0] };
    const writer = std.net.Stream{ .handle = pair[1] };

    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &len_buf, -1, .big);
    writer.writeAll(&len_buf) catch return error.SkipZigTest;

    try testing.expectError(error.ProtocolError, readFrame(testing.allocator, reader, 1024));
}

test "framing read rejects oversized frame" {
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    const reader = std.net.Stream{ .handle = pair[0] };
    const writer = std.net.Stream{ .handle = pair[1] };

    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &len_buf, 2048, .big);
    writer.writeAll(&len_buf) catch return error.SkipZigTest;

    try testing.expectError(error.TooLarge, readFrame(testing.allocator, reader, 1024));
}

test "framing read returns EndOfStream on truncated body" {
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    const reader = std.net.Stream{ .handle = pair[0] };
    const writer = std.net.Stream{ .handle = pair[1] };

    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &len_buf, 4, .big);
    writer.writeAll(&len_buf) catch return error.SkipZigTest;
    writer.writeAll("ab") catch return error.SkipZigTest;
    try std.posix.shutdown(pair[1], .send);

    try testing.expectError(error.EndOfStream, readFrame(testing.allocator, reader, 1024));
}

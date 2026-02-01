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

fn setNonBlocking(fd: std.posix.fd_t) !void {
    // test helper only
    const flags = try std.posix.fcntl(fd, std.posix.F.GETFL, 0);
    _ = try std.posix.fcntl(fd, std.posix.F.SETFL, flags | @as(i32, 0x800));
}

fn waitReadable(fd: std.posix.fd_t, timeout_ms: i32) errors.TransportError!void {
    var pfd = [_]std.posix.pollfd{.{
        .fd = fd,
        .events = std.posix.POLL.IN,
        .revents = 0,
    }};

    while (true) {
        const n = std.posix.poll(&pfd, timeout_ms) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return error.Unexpected,
        };

        if (n == 0) {
            return error.Timeout;
        }

        const revents = pfd[0].revents;
        if ((revents & std.posix.POLL.NVAL) != 0) {
            return error.Unexpected;
        }
        if ((revents & std.posix.POLL.ERR) != 0) {
            return error.ConnectionReset;
        }
        if ((revents & (std.posix.POLL.IN | std.posix.POLL.HUP)) == 0) {
            return error.Unexpected;
        }

        return;
    }
}

fn readExact(fd: std.posix.fd_t, buf: []u8, deadline_ms: i64) errors.TransportError!void {
    var offset: usize = 0;
    while (offset < buf.len) {
        const remaining = @as(i32, @intCast(@max(@as(i64, 0), deadline_ms - std.time.milliTimestamp())));
        if (remaining == 0) {
            return error.Timeout;
        }

        const n = std.posix.recv(fd, buf[offset..], 0) catch |e| switch (e) {
            error.Interrupted => continue,
            error.WouldBlock => {
                try waitReadable(fd, remaining);
                continue;
            },
            else => return errors.mapPosix(e),
        };

        if (n == 0) {
            return error.EndOfStream;
        }

        offset += n;
    }
}

fn waitWritable(fd: std.posix.fd_t, timeout_ms: i32) errors.TransportError!void {
    var pfd = [_]std.posix.pollfd{
        .{ .fd = fd, .events = std.posix.POLL.OUT, .revents = 0 },
    };

    while (true) {
        const n = std.posix.poll(&pfd, timeout_ms) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return error.Unexpected,
        };

        if (n == 0) {
            return error.Timeout;
        }

        const revents = pfd[0].revents;
        if ((revents & std.posix.POLL.NVAL) != 0) {
            return error.Unexpected;
        }
        if ((revents & (std.posix.POLL.ERR | std.posix.POLL.HUP)) != 0) {
            return error.ConnectionReset;
        }
        if ((revents & std.posix.POLL.OUT) == 0) {
            return error.Unexpected;
        }

        return;
    }
}

fn writeAllNoSignal(fd: std.posix.fd_t, bytes: []const u8, deadline_ms: i64) errors.TransportError!void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        const remaining = @as(i32, @intCast(@max(@as(i64, 0), deadline_ms - std.time.milliTimestamp())));
        if (remaining == 0) {
            return error.Timeout;
        }

        const flags: u32 = if (builtin.os.tag == .linux) std.posix.MSG.NOSIGNAL else 0;
        const n = std.posix.send(fd, bytes[offset..], flags) catch |e| switch (e) {
            error.Interrupted => continue,
            error.WouldBlock => {
                try waitWritable(fd, remaining);
                continue;
            },
            else => return errors.mapPosix(e),
        };

        if (n == 0) {
            return error.ConnectionReset;
        }

        offset += n;
    }
}

pub fn writeFrame(stream: std.net.Stream, payload: []const u8, max_frame_bytes: usize, deadline_ms: i64) errors.TransportError!void {
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

    try writeAllNoSignal(stream.handle, &len_buf, deadline_ms);
    try writeAllNoSignal(stream.handle, payload, deadline_ms);
}

pub fn readFrame(allocator: std.mem.Allocator, stream: std.net.Stream, max_frame_bytes: usize, deadline_ms: i64) errors.TransportError![]u8 {
    var len_buf: [4]u8 = undefined;
    try readExact(stream.handle, &len_buf, deadline_ms);

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

    try readExact(stream.handle, frame, deadline_ms);
    return frame;
}

const testing = std.testing;

fn testDeadlineMs() i64 {
    return std.time.milliTimestamp() + 100;
}

fn shortDeadlineMs() i64 {
    return std.time.milliTimestamp() + 20;
}

test "framing rejects zero-length payload on write" {
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    const stream = std.net.Stream{ .handle = pair[0] };
    try testing.expectError(error.ZeroLengthFrame, writeFrame(stream, &.{}, 1024, testDeadlineMs()));
}

test "framing rejects oversized payload on write" {
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    const stream = std.net.Stream{ .handle = pair[0] };

    const payload = [_]u8{0} ** 16;
    try testing.expectError(error.TooLarge, writeFrame(stream, &payload, 8, testDeadlineMs()));
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

    try testing.expectError(error.ZeroLengthFrame, readFrame(testing.allocator, reader, 1024, testDeadlineMs()));
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

    try testing.expectError(error.ProtocolError, readFrame(testing.allocator, reader, 1024, testDeadlineMs()));
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

    try testing.expectError(error.TooLarge, readFrame(testing.allocator, reader, 1024, testDeadlineMs()));
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

    try testing.expectError(error.EndOfStream, readFrame(testing.allocator, reader, 1024, testDeadlineMs()));
}

test "framing write/read roundtrip succeeds" {
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    const reader = std.net.Stream{ .handle = pair[0] };
    const writer = std.net.Stream{ .handle = pair[1] };

    try writeFrame(writer, "ping", 1024, testDeadlineMs());
    const frame = try readFrame(testing.allocator, reader, 1024, testDeadlineMs());
    defer testing.allocator.free(frame);

    try testing.expectEqualStrings("ping", frame);
}

test "framing nonblocking read respects timeout path" {
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    try setNonBlocking(pair[0]);
    const reader = std.net.Stream{ .handle = pair[0] };

    try std.testing.expectError(
        error.Timeout,
        readFrame(std.testing.allocator, reader, 1024, shortDeadlineMs()),
    );
}

test "framing read waits and then succeeds when data arrives later" {
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer std.posix.close(pair[1]);

    try setNonBlocking(pair[0]);
    const reader = std.net.Stream{ .handle = pair[0] };
    const writer = std.net.Stream{ .handle = pair[1] };

    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &len_buf, 4, .big);
    try writer.writeAll(&len_buf);
    try writer.writeAll("ping");

    const frame = try readFrame(testing.allocator, reader, 1024, testDeadlineMs());
    defer testing.allocator.free(frame);
    try testing.expectEqualStrings("ping", frame);
}

test "framing write returns connection error when peer is closed" {
    var open = true;
    const pair = try socketPairStream();
    defer std.posix.close(pair[0]);
    defer if (open) std.posix.close(pair[1]);

    const writer = std.net.Stream{ .handle = pair[0] };
    try std.posix.shutdown(pair[1], .both);
    std.posix.close(pair[1]);
    open = false;

    const result = writeFrame(writer, "ping", 1024, testDeadlineMs());
    if (result) |_| {
        return error.ExpectedWriteFailure;
    } else |err| switch (err) {
        error.BrokenPipe, error.ConnectionReset, error.Timeout, error.Unexpected => {},
        else => return err,
    }
}

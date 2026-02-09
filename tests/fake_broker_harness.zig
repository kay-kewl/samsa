const std = @import("std");
const builtin = @import("builtin");

pub const ScriptedExchange = struct {
    response_frame: []const u8,
    close_after_write: bool = false,
};

pub const CapturedRequest = struct {
    frame: []u8,
};

fn socketPairStream() ![2]std.posix.fd_t {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    var fds: [2]i32 = undefined;
    const raw = std.os.linux.socketpair(
        @as(i32, std.posix.AF.UNIX),
        @as(i32, std.posix.SOCK.STREAM),
        0,
        &fds,
    );

    switch (std.posix.errno(raw)) {
        .SUCCESS => return .{
            @as(std.posix.fd_t, @intCast(fds[0])),
            @as(std.posix.fd_t, @intCast(fds[1])),
        },
        else => return error.SkipZigTest,
    }
}

pub const Harness = struct {
    allocator: std.mem.Allocator,
    exchanges: []const ScriptedExchange,
    captures: std.ArrayList(CapturedRequest),

    pub fn init(allocator: std.mem.Allocator, exchanges: []const ScriptedExchange) Harness {
        return .{
            .allocator = allocator,
            .exchanges = exchanges,
            .captures = .empty,
        };
    }

    pub fn deinit(self: *Harness) void {
        for (self.captures.items) |c| {
            self.allocator.free(c.frame);
        }

        self.captures.deinit(self.allocator);
    }

    pub fn runOnAcceptedStream(self: *Harness, stream: std.net.Stream) !void {
        defer stream.close();

        for (self.exchanges) |ex| {
            var len_buf: [4]u8 = undefined;
            const header_n = try stream.readAtLeast(&len_buf, len_buf.len);
            if (header_n != len_buf.len) {
                return error.EndOfStream;
            }

            const frame_len_i32 = std.mem.readInt(i32, &len_buf, .big);
            if (frame_len_i32 <= 0) {
                return error.InvalidLength;
            }

            const frame_len: usize = @intCast(frame_len_i32);
            const request = try self.allocator.alloc(u8, frame_len);
            errdefer self.allocator.free(request);

            const body_n = try stream.readAtLeast(request, frame_len);
            if (body_n != frame_len) {
                return error.EndOfStream;
            }

            try self.captures.append(self.allocator, .{
                .frame = request,
            });

            var out_len_buf: [4]u8 = undefined;
            std.mem.writeInt(i32, &out_len_buf, @intCast(ex.response_frame.len), .big);
            try stream.writeAll(&out_len_buf);
            try stream.writeAll(ex.response_frame);

            if (ex.close_after_write) {
                return;
            }
        }
    }
};

test "fake broker harness capture a request and returns scripted response" {
    const pair = try socketPairStream();

    var h = Harness.init(std.testing.allocator, &[_]ScriptedExchange{
        .{
            .response_frame = "pong",
        },
    });
    defer h.deinit();

    const t = try std.Thread.spawn(.{}, struct {
        fn run(hh: *Harness, fd: std.posix.fd_t) void {
            hh.runOnAcceptedStream(.{
                .handle = fd,
            }) catch {};
        }
    }.run, .{ &h, pair[0] });

    var client = std.net.Stream{ .handle = pair[1] };
    var request_len: [4]u8 = undefined;
    std.mem.writeInt(i32, &request_len, 4, .big);
    try client.writeAll(&request_len);
    try client.writeAll("ping");

    var response_len_buf: [4]u8 = undefined;
    const hlen = try client.readAtLeast(&response_len_buf, response_len_buf.len);
    try std.testing.expectEqual(@as(usize, 4), hlen);
    const response_len = std.mem.readInt(i32, &response_len_buf, .big);
    try std.testing.expectEqual(@as(i32, 4), response_len);

    var response: [4]u8 = undefined;
    const rlen = try client.readAtLeast(&response, response.len);
    try std.testing.expectEqual(@as(usize, 4), rlen);
    try std.testing.expectEqualStrings("pong", response[0..]);

    t.join();

    try std.testing.expectEqual(@as(usize, 1), h.captures.items.len);
    try std.testing.expectEqualStrings("ping", h.captures.items[0].frame);
}

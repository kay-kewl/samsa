const std = @import("std");

pub const ScriptedExchange = struct {
    response_frame: []const u8,
    close_after_write: bool = false,
};

pub const CapturedRequest = struct {
    frame: []u8,
};

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

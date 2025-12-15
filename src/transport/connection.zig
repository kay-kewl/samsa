const std = @import("std");
const errors = @import("errors.zig");
const framing = @import("framing.zig");
const header = @import("../protocol/header.zig");
const codec = @import("../protocol/codec.zig");
const types = @import("../protocol/types.zig");
const api_versions = @import("../generated/api_versions.zig");

fn deadlineMsFromNow(timeout_ms: i32) i64 {
    return std.time.milliTimestamp() + timeout_ms;
}

fn remainingMs(deadline_ms: i64) i32 {
    const now = std.time.milliTimestamp();
    const remaining = deadline_ms - now;
    if (remaining <= 0) {
        return 0;
    } else if (remaining > std.math.maxInt(i32)) {
        return std.math.maxInt(i32);
    }

    return @intCast(remaining);
}

fn waitFd(fd: std.posix.fd_t, events: i16, timeout_ms: i32) errors.TransportError!void {
    var pfd = [_]std.posix.pollfd{
        .{
            .fd = fd,
            .events = events,
            .revents = 0,
        },
    };

    const n = std.posix.poll(&pfd, timeout_ms) catch return error.Unexpected;
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

    if ((revents & std.posix.POLL.HUP) != 0 and (events & std.posix.POLL.IN) == 0) {
        return error.ConnectionReset;
    }
}

pub const State = enum {
    Disconnected,
    Connecting,
    Handshaking,
    Ready,
    Dead,
};

pub const Config = struct {
    host: []const u8,
    port: u16,
    connect_timeout_ms: i32 = 10_000,
    request_timeout_ms: i32 = 30_000,
    max_frame_bytes: usize = 16 * 1024 * 1024,
};

pub const Connection = struct {
    allocator: std.mem.Allocator,
    config: Config,
    stream: ?std.net.Stream = null,
    state: State = .Disconnected,
    correlation_id: i32 = 1,

    pub fn init(allocator: std.mem.Allocator, config: Config) Connection {
        return .{
            .allocator = allocator,
            .config = config,
        };
    }

    pub fn deinit(self: *Connection) void {
        if (self.stream) |s| {
            s.close();
        }

        self.stream = null;
        self.state = .Disconnected;
    }

    fn writeFrameWithDeadline(self: *Connection, payload: []const u8, deadline_ms: i64) errors.TransportError!void {
        const s = self.stream orelse return error.Unexpected;
        while (true) {
            const remaining = remainingMs(deadline_ms);
            if (remaining == 0) {
                return error.Timeout;
            }

            try waitFd(s.handle, std.posix.POLL.OUT, remaining);

            const result = framing.writeFrame(s, payload, self.config.max_frame_bytes);
            if (result) |_| return else |err| switch (err) {
                error.BrokenPipe, error.ConnectionReset, error.NetworkUnreachable => return err,
                else => return err,
            }
        }
    }

    fn readFrameWithDeadline(self: *Connection, deadline_ms: i64) errors.TransportError![]u8 {
        const s = self.stream orelse return error.Unexpected;
        while (true) {
            const remaining = remainingMs(deadline_ms);
            if (remaining == 0) {
                return error.Timeout;
            }

            try waitFd(s.handle, std.posix.POLL.IN, remaining);

            const result = framing.readFrame(self.allocator, s, self.config.max_frame_bytes);
            if (result) |frame| return frame else |err| switch (err) {
                error.EndOfStream, error.ConnectionReset => return err,
                else => return err,
            }
        }
    }

    fn handshakeApiVersions(self: *Connection) errors.TransportError!void {
        var buf: [2048]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        const request_header = header.RequestHeaderV2{
            .api_key = @intFromEnum(api_versions.api_key),
            .api_version = 4,
            .correlation_id = self.correlation_id,
            .client_id = "samsa",
        };
        request_header.encode(&e) catch return error.ProtocolError;

        const request = api_versions.Request{
            .client_software_name = "samsa",
            .client_software_version = "0.1.0",
        };
        request.encode(&e, 4) catch return error.ProtocolError;

        const deadline_ms = deadlineMsFromNow(self.config.request_timeout_ms);
        try self.writeFrameWithDeadline(e.written(), deadline_ms);

        const frame = try self.readFrameWithDeadline(deadline_ms);
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
        const response_header = header.ResponseHeaderV0.decode(&d) catch return error.ProtocolError;
        if (response_header.correlation_id != self.correlation_id) {
            return error.ProtocolError;
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        _ = api_versions.Response.decode(arena.allocator(), &d, 4) catch return error.ProtocolError;

        self.correlation_id +%= 1;
    }

    pub fn connect(self: *Connection) errors.TransportError!void {
        if (self.state == .Ready) {
            return;
        }

        self.state = .Connecting;
        const address = std.net.Address.parseIp(self.config.host, self.config.port) catch return error.Unexpected;
        const stream = std.net.tcpConnectToAddress(address) catch |e| {
            self.state = .Dead;
            return errors.mapPosix(e);
        };

        self.stream = stream;
        self.state = .Handshaking;

        self.handshakeApiVersions() catch |err| {
            self.state = .Dead;
            return err;
        };

        self.state = .Ready;
    }

    fn ensureReady(self: *Connection) errors.TransportError!void {
        if (self.state == .Ready) {
            return;
        }

        try self.connect();

        if (self.state != .Ready) {
            return error.Unexpected;
        }
    }

    pub fn call(self: *Connection, api_key: types.ApiKey, is_flexible: bool, payload: []const u8) errors.TransportError![]u8 {
        try self.ensureReady();

        const expected_correlation_id = self.correlation_id;
        const deadline_ms = deadlineMsFromNow(self.config.request_timeout_ms);

        try self.writeFrameWithDeadline(payload, deadline_ms);
        const response = try self.readFrameWithDeadline(deadline_ms);

        var d = codec.Decoder.init(response);
        const header_version = header.responseHeaderVersion(api_key, is_flexible);
        const result_correlation_id: i32 = switch (header_version) {
            .v0 => (header.ResponseHeaderV0.decode(&d) catch {
                self.state = .Dead;
                return error.ProtocolError;
            }).correlation_id,
            .v1 => (header.ResponseHeaderV1.decode(&d) catch {
                self.state = .Dead;
                return error.ProtocolError;
            }).correlation_id,
            else => {
                self.state = .Dead;
                return error.ProtocolError;
            },
        };

        if (result_correlation_id != expected_correlation_id) {
            self.state = .Dead;
            return error.ProtocolError;
        }

        self.correlation_id +%= 1;
        return response;
    }

    pub fn callNoResponse(self: *Connection, payload: []const u8) errors.TransportError!void {
        try self.ensureReady();

        const deadline_ms = deadlineMsFromNow(self.config.request_timeout_ms);
        try self.writeFrameWithDeadline(payload, deadline_ms) catch |err| {
            self.state = .Dead;
            return err;
        };

        self.correlation_id +%= 1;
    }
};

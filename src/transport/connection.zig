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

    if ((revents & events) == 0) {
        return error.Unexpected;
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

    pub fn validate(self: @This()) errors.TransportError!void {
        if (self.host.len == 0 or self.port == 0) {
            return error.ProtocolError;
        }

        if (self.connect_timeout_ms <= 0 or self.request_timeout_ms <= 0) {
            return error.Timeout;
        }

        if (self.max_frame_bytes == 0 or self.max_frame_bytes > std.math.maxInt(i32)) {
            return error.TooLarge;
        }
    }
};

pub const Statistics = struct {
    frames_written: u64 = 0,
    frames_read: u64 = 0,
    timeouts: u64 = 0,
    protocol_errors: u64 = 0,
};

pub const Connection = struct {
    allocator: std.mem.Allocator,
    config: Config,
    stream: ?std.net.Stream = null,
    state: State = .Disconnected,
    correlation_id: i32 = 1,
    statistics: Statistics = .{},

    fn failDead(self: *Connection, err: errors.TransportError) errors.TransportError {
        self.state = .Dead;
        return err;
    }

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

    pub fn getStatistics(self: *const Connection) Statistics {
        return self.statistics;
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
            if (result) |_| {
                self.statistics.frames_written += 1;
                return;
            } else |err| switch (err) {
                error.Timeout => {
                    self.statistics.timeouts += 1;
                    return err;
                },
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
            if (result) |frame| {
                self.statistics.frames_read += 1;
                return frame;
            } else |err| switch (err) {
                error.Timeout => {
                    self.statistics.timeouts += 1;
                    return err;
                },
                else => return err,
            }
        }
    }

    fn decodeApiVersionsBodyWithFallback(self: *Connection, frame: []const u8, request_version: i16) errors.TransportError!api_versions.Response {
        var d = codec.Decoder.init(frame);
        const response_header = header.ResponseHeaderV0.decode(&d) catch return error.ProtocolError;
        if (response_header.correlation_id != self.correlation_id) {
            return error.ProtocolError;
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const direct = api_versions.Response.decode(arena.allocator(), &d, request_version);
        if (direct) |r| return r else |_| {
            var d0 = codec.Decoder.init(frame);
            const response_header0 = header.ResponseHeaderV0.decode(&d0) catch return error.ProtocolError;
            if (response_header0.correlation_id != self.correlation_id) {
                return error.ProtocolError;
            }

            var arena0 = std.heap.ArenaAllocator.init(self.allocator);
            defer arena0.deinit();

            return api_versions.Response.decode(arena0.allocator(), &d0, 0) catch return error.ProtocolError;
        }
    }

    fn sendApiVersionsOnce(self: *Connection, request_version: i16, deadline_ms: i64) errors.TransportError!api_versions.Response {
        var buf: [2048]u8 = undefined;
        var e = codec.Encoder.init(&buf);

        if (request_version >= 3) {
            const request_header = header.RequestHeaderV2{
                .api_key = @intFromEnum(api_versions.api_key),
                .api_version = request_version,
                .correlation_id = self.correlation_id,
                .client_id = "samsa",
            };
            request_header.encode(&e) catch return error.ProtocolError;
        } else {
            const request_header = header.RequestHeaderV1{
                .api_key = @intFromEnum(api_versions.api_key),
                .api_version = request_version,
                .correlation_id = self.correlation_id,
                .client_id = "samsa",
            };
            request_header.encode(&e) catch return error.ProtocolError;
        }

        const request = api_versions.Request{
            .client_software_name = "samsa",
            .client_software_version = "0.1.0",
        };
        request.encode(&e, request_version) catch return error.ProtocolError;

        try self.writeFrameWithDeadline(e.written(), deadline_ms);
        const frame = try self.readFrameWithDeadline(deadline_ms);
        defer self.allocator.free(frame);

        return try self.decodeApiVersionsBodyWithFallback(frame, request_version);
    }

    fn handshakeApiVersions(self: *Connection) errors.TransportError!void {
        const deadline_ms = deadlineMsFromNow(self.config.request_timeout_ms);

        var response = try sendApiVersionsOnce(self, 4, deadline_ms);
        if (response.error_code == 35) {
            response = try self.sendApiVersionsOnce(2, deadline_ms);
            if (response.error_code == 35) {
                return error.ProtocolError;
            }
        } else if (response.error_code != 0) {
            return error.ProtocolError;
        }

        self.correlation_id +%= 1;
    }

    pub fn connect(self: *Connection) errors.TransportError!void {
        if (self.state == .Ready) {
            return;
        }

        try self.config.validate();
        self.state = .Connecting;
        const address = std.net.Address.parseIp(self.config.host, self.config.port) catch return error.Unexpected;
        const stream = std.net.tcpConnectToAddress(address) catch |e| {
            self.state = .Dead;
            return errors.mapPosix(e);
        };

        self.stream = stream;
        self.state = .Handshaking;

        self.handshakeApiVersions() catch |err| {
            if (self.stream) |s| {
                s.close();
            }

            self.stream = null;
            self.statistics.protocol_errors += 1;
            return self.failDead(err);
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
        const response = self.readFrameWithDeadline(deadline_ms) catch |err| {
            self.statistics.protocol_errors += 1;
            return self.failDead(err);
        };
        var d = codec.Decoder.init(response);
        const header_version = header.responseHeaderVersion(api_key, is_flexible);
        const result_correlation_id: i32 = switch (header_version) {
            .v0 => (header.ResponseHeaderV0.decode(&d) catch {
                self.statistics.protocol_errors += 1;
                return self.failDead(error.ProtocolError);
            }).correlation_id,
            .v1 => (header.ResponseHeaderV1.decode(&d) catch {
                self.statistics.protocol_errors += 1;
                return self.failDead(error.ProtocolError);
            }).correlation_id,
            else => {
                self.statistics.protocol_errors += 1;
                return self.failDead(error.ProtocolError);
            },
        };

        if (result_correlation_id != expected_correlation_id) {
            self.statistics.protocol_errors += 1;
            return self.failDead(error.ProtocolError);
        }

        self.correlation_id +%= 1;
        return response;
    }

    pub fn callNoResponse(self: *Connection, payload: []const u8) errors.TransportError!void {
        try self.ensureReady();

        const deadline_ms = deadlineMsFromNow(self.config.request_timeout_ms);
        try self.writeFrameWithDeadline(payload, deadline_ms) catch |err| {
            self.statistics.protocol_errors += 1;
            return self.failDead(err);
        };

        self.correlation_id +%= 1;
    }
};

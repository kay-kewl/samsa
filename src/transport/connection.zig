const std = @import("std");
const errors = @import("errors.zig");
const framing = @import("framing.zig");
const stream_mod = @import("stream.zig");
const header = @import("../protocol/header.zig");
const codec = @import("../protocol/codec.zig");
const types = @import("../protocol/types.zig");
const api_versions = @import("../generated/api_versions.zig");
const protocol_limits = @import("../protocol/limits.zig");
const compat = @import("../compat.zig");

fn deadlineMsFromNow(timeout_ms: i32) i64 {
    return compat.milliTimestamp() + timeout_ms;
}

fn remainingMs(deadline_ms: i64) i32 {
    const now = compat.milliTimestamp();
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

    if ((revents & std.posix.POLL.HUP) != 0) {
        if ((events & std.posix.POLL.IN) != 0) {
            // let the caller read and get EndOfStream if needed
        } else {
            return error.ConnectionReset;
        }
    }

    if ((revents & events) == 0 and (revents & std.posix.POLL.HUP) == 0) {
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
    tcp_nodelay: bool = false,
    enable_tcp_keepalive: bool = false,
    decoder_limits: protocol_limits.Limits = .{},

    client_id: []const u8 = "samsa",
    client_software_name: []const u8 = "samsa",
    client_software_version: []const u8 = "0.1.0",

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
    stream: ?stream_mod.Stream = null,
    state: State = .Disconnected,
    correlation_id: i32 = 1,
    statistics: Statistics = .{},

    const ApiVersionsSummary = struct {
        error_code: i16,
    };

    fn failDead(self: *Connection, err: errors.TransportError) errors.TransportError {
        if (self.stream) |s| {
            s.close();
        }
        self.stream = null;

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

        const remaining = remainingMs(deadline_ms);
        if (remaining == 0) {
            self.statistics.timeouts += 1;
            return error.Timeout;
        }

        framing.writeFrame(s, payload, self.config.max_frame_bytes, deadline_ms) catch |err| switch (err) {
            error.Timeout => {
                self.statistics.timeouts += 1;
                return err;
            },
            else => return err,
        };

        self.statistics.frames_written += 1;
        return;
    }

    fn readFrameWithDeadline(self: *Connection, deadline_ms: i64) errors.TransportError![]u8 {
        const s = self.stream orelse return error.Unexpected;

        const remaining = remainingMs(deadline_ms);
        if (remaining == 0) {
            self.statistics.timeouts += 1;
            return error.Timeout;
        }

        const frame = framing.readFrame(self.allocator, s, self.config.max_frame_bytes, deadline_ms) catch |err| switch (err) {
            error.Timeout => {
                self.statistics.timeouts += 1;
                return err;
            },
            else => return err,
        };

        self.statistics.frames_read += 1;
        return frame;
    }

    pub fn decodeApiVersionsBodyWithFallback(self: *Connection, frame: []const u8, request_version: i16) errors.TransportError!ApiVersionsSummary {
        var d = codec.Decoder.initWithLimits(frame, self.config.decoder_limits);
        const response_header = header.ResponseHeaderV0.decode(&d) catch {
            self.statistics.protocol_errors += 1;
            return error.ProtocolError;
        };
        if (response_header.correlation_id != self.correlation_id) {
            self.statistics.protocol_errors += 1;
            return error.ProtocolError;
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const direct = api_versions.Response.decode(arena.allocator(), &d, request_version);
        if (direct) |r| {
            if (d.remaining() != 0) {
                self.statistics.protocol_errors += 1;
                return error.ProtocolError;
            }

            return .{ .error_code = r.error_code };
        } else |err| switch (err) {
            error.EndOfStream, error.InvalidLength, error.Overflow, error.InvalidVariant => {
                var d0 = codec.Decoder.initWithLimits(frame, self.config.decoder_limits);
                const response_header0 = header.ResponseHeaderV0.decode(&d0) catch {
                    self.statistics.protocol_errors += 1;
                    return error.ProtocolError;
                };
                if (response_header0.correlation_id != self.correlation_id) {
                    self.statistics.protocol_errors += 1;
                    return error.ProtocolError;
                }

                if (d0.remaining() == 0) {
                    self.statistics.protocol_errors += 1;
                    return error.ProtocolError;
                }

                var arena0 = std.heap.ArenaAllocator.init(self.allocator);
                defer arena0.deinit();

                const fallback = api_versions.Response.decode(arena0.allocator(), &d0, 0) catch {
                    self.statistics.protocol_errors += 1;
                    return error.ProtocolError;
                };
                if (d0.remaining() != 0) {
                    self.statistics.protocol_errors += 1;
                    return error.ProtocolError;
                }
                if (fallback.error_code != 35) {
                    self.statistics.protocol_errors += 1;
                    return error.ProtocolError;
                }

                return .{ .error_code = 35 };
            },
            else => {
                self.statistics.protocol_errors += 1;
                return error.ProtocolError;
            },
        }
    }

    fn encodeApiVersionsRequest(self: *Connection, request_version: i16, out: []u8) errors.TransportError![]const u8 {
        var e = codec.Encoder.init(out);

        if (request_version >= 3) {
            const request_header = header.RequestHeaderV2{
                .api_key = @intFromEnum(api_versions.api_key),
                .api_version = request_version,
                .correlation_id = self.correlation_id,
                .client_id = self.config.client_id,
            };
            request_header.encode(&e) catch |err| switch (err) {
                error.NoSpace => return error.TooLarge,
                else => return error.ProtocolError,
            };
        } else {
            const request_header = header.RequestHeaderV1{
                .api_key = @intFromEnum(api_versions.api_key),
                .api_version = request_version,
                .correlation_id = self.correlation_id,
                .client_id = self.config.client_id,
            };
            request_header.encode(&e) catch return error.ProtocolError;
        }

        const request = api_versions.Request{
            .client_software_name = self.config.client_software_name,
            .client_software_version = self.config.client_software_version,
        };
        request.encode(&e, request_version) catch return error.ProtocolError;

        return e.written();
    }

    fn sendApiVersionsOnce(self: *Connection, request_version: i16, deadline_ms: i64) errors.TransportError!ApiVersionsSummary {
        const request_buf = self.allocator.alloc(u8, self.config.max_frame_bytes) catch return error.Unexpected;
        defer self.allocator.free(request_buf);
        const payload = try self.encodeApiVersionsRequest(request_version, request_buf);

        try self.writeFrameWithDeadline(payload, deadline_ms);
        const frame = try self.readFrameWithDeadline(deadline_ms);
        defer self.allocator.free(frame);

        const summary = try self.decodeApiVersionsBodyWithFallback(frame, request_version);
        self.correlation_id +%= 1;
        return summary;
    }

    fn handshakeApiVersions(self: *Connection, deadline_ms: i64) errors.TransportError!void {
        var response = try self.sendApiVersionsOnce(4, deadline_ms);
        if (response.error_code == 35) {
            response = try self.sendApiVersionsOnce(2, deadline_ms);
            if (response.error_code == 35) {
                return error.ProtocolError;
            }
        } else if (response.error_code != 0) {
            return error.ProtocolError;
        }
    }

    fn openConnectedStreamWithDeadline(self: *Connection, deadline_ms: i64) errors.TransportError!stream_mod.Stream {
        var last_err: errors.TransportError = error.Unexpected;
        var address = stream_mod.parseIpAddress(self.config.host, self.config.port) catch {
            return error.NetworkUnreachable;
        };
        var storage: std.Io.Threaded.PosixAddress = undefined;
        const sock_len = std.Io.Threaded.addressToPosix(&address, &storage);

        {
            const remaining = remainingMs(deadline_ms);
            if (remaining == 0) {
                return error.Timeout;
            }

            const sock = stream_mod.socketTcp(storage.any.family, true) catch |e| {
                last_err = errors.mapPosix(e);
                return last_err;
            };

            var one: i32 = 1;
            if (self.config.tcp_nodelay) {
                stream_mod.setSockOpt(sock, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&one)) catch {};
            }

            if (self.config.enable_tcp_keepalive) {
                stream_mod.setSockOpt(sock, std.posix.SOL.SOCKET, std.posix.SO.KEEPALIVE, std.mem.asBytes(&one)) catch {};
            }

            var keep_sock = false;
            defer if (!keep_sock) stream_mod.closeFd(sock);

            stream_mod.connectFd(sock, &storage.any, sock_len) catch |e| switch (e) {
                error.WouldBlock, error.ConnectionPending => {},
                else => {
                    last_err = errors.mapPosix(e);
                    return last_err;
                },
            };

            waitFd(sock, std.posix.POLL.OUT, remaining) catch |e| {
                last_err = e;
                return last_err;
            };

            var so_error: i32 = 0;
            stream_mod.getSockOpt(sock, std.posix.SOL.SOCKET, std.posix.SO.ERROR, std.mem.asBytes(&so_error)) catch {
                return error.Unexpected;
            };
            if (so_error != 0) {
                last_err = errors.mapErrnoCode(so_error);
                return last_err;
            }

            keep_sock = true;
            return .{ .handle = sock };
        }

        return last_err;
    }

    pub fn connectWithDeadline(self: *Connection, deadline_ms: i64) errors.TransportError!void {
        if (self.state == .Ready) {
            return;
        } else if (self.state == .Dead and self.stream != null) {
            if (self.stream) |s| {
                s.close();
            }

            self.stream = null;
        }

        try self.config.validate();
        self.state = .Connecting;

        const stream = self.openConnectedStreamWithDeadline(deadline_ms) catch |err| {
            self.stream = null;
            self.state = .Dead;
            return err;
        };

        self.stream = stream;
        self.state = .Handshaking;

        const request_deadline = deadlineMsFromNow(self.config.request_timeout_ms);
        const handshake_deadline = @min(deadline_ms, request_deadline);

        self.handshakeApiVersions(handshake_deadline) catch |err| {
            self.statistics.protocol_errors += 1;
            return self.failDead(err);
        };

        self.state = .Ready;
    }

    pub fn connect(self: *Connection) errors.TransportError!void {
        const deadline_ms = deadlineMsFromNow(self.config.connect_timeout_ms);
        return self.connectWithDeadline(deadline_ms);
    }

    fn ensureReady(self: *Connection, deadline_ms: i64) errors.TransportError!void {
        if (self.state == .Ready) {
            return;
        }

        try self.connectWithDeadline(deadline_ms);

        if (self.state != .Ready) {
            return error.Unexpected;
        }
    }

    pub fn callWithDeadline(self: *Connection, api_key: types.ApiKey, api_version: i16, payload: []const u8, deadline_ms: i64) errors.TransportError![]u8 {
        try self.ensureReady(deadline_ms);

        const expected_correlation_id = self.correlation_id;

        self.writeFrameWithDeadline(payload, deadline_ms) catch |err| {
            self.statistics.protocol_errors += 1;
            return self.failDead(err);
        };
        const response = self.readFrameWithDeadline(deadline_ms) catch |err| {
            self.statistics.protocol_errors += 1;
            return self.failDead(err);
        };
        var d = codec.Decoder.initWithLimits(response, self.config.decoder_limits);
        const header_version = header.responseHeaderVersionForApiVersion(api_key, api_version);
        const result_correlation_id: i32 = switch (header_version) {
            .v0 => (header.ResponseHeaderV0.decode(&d) catch {
                self.statistics.protocol_errors += 1;
                return self.failDead(error.ProtocolError);
            }).correlation_id,
            .v1, .v2 => (header.ResponseHeaderV1.decode(&d) catch {
                self.statistics.protocol_errors += 1;
                return self.failDead(error.ProtocolError);
            }).correlation_id,
        };

        if (result_correlation_id != expected_correlation_id) {
            self.statistics.protocol_errors += 1;
            return self.failDead(error.ProtocolError);
        }

        self.correlation_id +%= 1;
        return response;
    }

    pub fn call(self: *Connection, api_key: types.ApiKey, api_version: i16, payload: []const u8) errors.TransportError![]u8 {
        return self.callWithDeadline(api_key, api_version, payload, deadlineMsFromNow(self.config.request_timeout_ms));
    }

    pub fn callNoResponseWithDeadline(self: *Connection, payload: []const u8, deadline_ms: i64) errors.TransportError!void {
        try self.ensureReady(deadline_ms);

        self.writeFrameWithDeadline(payload, deadline_ms) catch |err| {
            self.statistics.protocol_errors += 1;
            return self.failDead(err);
        };

        self.correlation_id +%= 1;

        if (self.stream) |s| {
            var pfd = [_]std.posix.pollfd{
                .{
                    .fd = s.handle,
                    .events = std.posix.POLL.IN,
                    .revents = 0,
                },
            };

            const n = std.posix.poll(&pfd, 0) catch return error.Unexpected;
            if (n > 0) {
                const revents = pfd[0].revents;
                if ((revents & (std.posix.POLL.ERR | std.posix.POLL.HUP | std.posix.POLL.NVAL)) != 0) {
                    return self.failDead(error.ConnectionReset);
                }
                if ((revents & std.posix.POLL.IN) != 0) {
                    var peek_byte: [1]u8 = undefined;
                    const peek_flags: u32 = std.posix.MSG.PEEK | std.posix.MSG.DONTWAIT;
                    const peek_n = stream_mod.recvFd(s.handle, &peek_byte, peek_flags) catch |e| switch (e) {
                        error.WouldBlock => 1,
                        else => return self.failDead(errors.mapPosix(e)),
                    };

                    if (peek_n == 0) {
                        return self.failDead(error.ConnectionReset);
                    }
                }
            }
        }
    }

    pub fn callNoResponse(self: *Connection, payload: []const u8) errors.TransportError!void {
        return self.callNoResponseWithDeadline(payload, deadlineMsFromNow(self.config.request_timeout_ms));
    }
};

const testing = std.testing;

fn encodeV0HeaderAndBody(correlation_id: i32, body: []const u8, out: []u8) ![]const u8 {
    var e = codec.Encoder.init(out);
    try e.writeI32(correlation_id);

    if (e.pos + body.len > e.buf.len) {
        return error.NoSpace;
    }

    @memcpy(e.buf[e.pos .. e.pos + body.len], body);
    e.pos += body.len;

    return e.written();
}

test "decodeApiVersionsBodyWithFallback accepts v0 unsupported-version response" {
    var c = Connection.init(testing.allocator, .{
        .host = "127.0.0.1",
        .port = 1,
    });
    defer c.deinit();
    c.correlation_id = 42;

    const v0_body = [_]u8{
        0x00, 0x23,
        0x00, 0x00,
        0x00, 0x01,
        0x00, 0x12,
        0x00, 0x02,
        0x00, 0x04,
    };

    var frame_buf: [64]u8 = undefined;
    const frame = try encodeV0HeaderAndBody(c.correlation_id, &v0_body, &frame_buf);

    const summary = try c.decodeApiVersionsBodyWithFallback(frame, 4);
    try testing.expectEqual(@as(i16, 35), summary.error_code);
}

test "decodeApiVersionsBodyWithFallback rejects mismatched coorelation_id" {
    var c = Connection.init(testing.allocator, .{
        .host = "127.0.0.1",
        .port = 1,
    });
    defer c.deinit();
    c.correlation_id = 42;

    const v0_body = [_]u8{
        0x00, 0x23,
        0x00, 0x00,
        0x00, 0x01,
        0x00, 0x12,
        0x00, 0x02,
        0x00, 0x04,
    };

    var frame_buf: [64]u8 = undefined;
    const frame = try encodeV0HeaderAndBody(c.correlation_id + 1, &v0_body, &frame_buf);
    try testing.expectError(error.ProtocolError, c.decodeApiVersionsBodyWithFallback(frame, 4));
    try testing.expectEqual(@as(u64, 1), c.statistics.protocol_errors);
}

test "encodeApiVersionsRequest uses configured identity and software fields" {
    var c = Connection.init(testing.allocator, .{
        .host = "127.0.0.1",
        .port = 1,
        .client_id = "cid-42",
        .client_software_name = "samsa-it",
        .client_software_version = "9.9.9",
    });
    defer c.deinit();

    c.correlation_id = 123;

    var buf: [256]u8 = undefined;
    const payload = try c.encodeApiVersionsRequest(4, &buf);

    var d = codec.Decoder.init(payload);
    try testing.expectEqual(@as(i16, @intFromEnum(api_versions.api_key)), try d.readI16());
    try testing.expectEqual(@as(i16, 4), try d.readI16());
    try testing.expectEqual(@as(i32, 123), try d.readI32());

    const client_id = (try d.readNullableString()).?;
    try testing.expectEqualStrings("cid-42", client_id);
    try testing.expectEqual(@as(u32, 0), try d.readUVarint32());

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const request = try api_versions.Request.decode(arena.allocator(), &d, 4);
    try testing.expectEqualStrings("samsa-it", request.client_software_name);
    try testing.expectEqualStrings("9.9.9", request.client_software_version);
    try testing.expectEqual(@as(usize, 0), d.remaining());
}

const std = @import("std");
const errors = @import("errors.zig");
const framing = @import("framing.zig");
const header = @import("../protocol/header.zig");
const codec = @import("../protocol/codec.zig");
const api_versions = @import("../generated/api_versions.zig");

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

    fn handshakeApiVersions(self: *Connection) errors.TransportError!void {
        const s = self.stream orelse return error.Unexpected;

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

        try framing.writeFrame(s, e.written(), self.config.max_frame_bytes);

        const frame = try framing.readFrame(self.allocator, s, self.config.max_frame_bytes);
        defer self.allocator.free(frame);

        var d = codec.Decoder.init(frame);
        const response_header = header.RequestHeaderV0.decode(&d) catch return error.ProtocolError;
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
        const stream = std.net.tcpConnectToAddress(address) catch |e| return errors.mapPosix(e);

        self.stream = stream;
        self.state = .Handshaking;

        try self.handshakeApiVersions();

        self.state = .Ready;
    }
};

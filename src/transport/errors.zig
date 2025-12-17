const std = @import("std");

pub const TransportError = error{
    Timeout,
    TooLarge,
    ProtocolError,
    ZeroLengthFrame,
    BrokenPipe,
    ConnectionRefused,
    NetworkUnreachable,
    ConnectionReset,
    EndOfStream,
    PoolExhausted,
    Unexpected,
};

pub fn mapPosix(err: anyerror) TransportError {
    return switch (err) {
        error.BrokenPipe => error.BrokenPipe,
        error.ConnectionRefused => error.ConnectionRefused,
        error.NetworkUnreachable => error.NetworkUnreachable,
        error.ConnectionResetByPeer => error.ConnectionReset,
        error.ConnectionTimedOut => error.Timeout,
        error.TimedOut => error.Timeout,
        error.WouldBlock => error.Timeout,
        error.EndOfStream => error.EndOfStream,
        else => error.Unexpected,
    };
}

pub fn mapErrnoCode(errno_code: i32) TransportError {
    const e: std.posix.E = @enumFromInt(@as(u16, @intCast(errno_code)));
    return switch (e) {
        .CONNREFUSED => error.ConnectionRefused,
        .NETUNREACH => error.NetworkUnreachable,
        .CONNRESET => error.ConnectionReset,
        .TIMEDOUT => error.Timeout,
        .PIPE => error.BrokenPipe,
        else => error.Unexpected,
    };
}

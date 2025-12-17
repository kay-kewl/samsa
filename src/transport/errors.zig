pub const TransportError = error{
    Timeout,
    EOF,
    TooLarge,
    ProtocolError,
    ZeroLengthFrame,
    BrokenPipe,
    ConnectionRefused,
    NetworkUnreachable,
    ConnectionReset,
    EndOfStream,
    Interrupted,
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

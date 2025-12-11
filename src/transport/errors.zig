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
    Unexpected,
};

pub fn mapPosix(err: anyerror) TransportError {
    return switch (err) {
        error.BrokenPipe => error.BrokenPipe,
        error.ConnectionRefused => error.ConnectionRefused,
        error.NetworkUnreachable => error.NetworkUnreachable,
        error.ConnectionResetByPeer => error.ConnectionReset,
        error.EndOfStream => error.EndOfStream,
        else => error.Unexpected,
    };
}

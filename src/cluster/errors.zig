pub const ClusterError = error{
    NoBrokers,
    NoLeader,
    UnknownTopic,
    UnknownPartition,
    VersionNotNegotiated,
    FrameTooLarge,
    InvalidFrame,
    ProtocolError,
    StaleMetadata,
    MetadataUnavailable,
    Timeout,
    ConnectionRefused,
    ConnectionReset,
    NetworkUnreachable,
    Unexpected,
};

pub fn mapTransportError(err: anyerror) ClusterError {
    return switch (err) {
        error.Timeout,
        error.TimedOut,
        error.ConnectionTimedOut,
        error.WouldBlock,
        => error.Timeout,

        error.TooLarge => error.FrameTooLarge,
        error.ZeroLengthFrame => error.InvalidFrame,

        error.ProtocolError => error.ProtocolError,

        error.ConnectionRefused => error.ConnectionRefused,

        error.ConnectionReset,
        error.ConnectionResetByPeer,
        error.BrokenPipe,
        error.EndOfStream,
        => error.ConnectionReset,

        error.NetworkUnreachable => error.NetworkUnreachable,

        else => error.Unexpected,
    };
}

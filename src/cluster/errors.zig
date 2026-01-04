pub const ClusterError = error{
    NoBrokers,
    NoLeader,
    UnknownTopic,
    UnknownPartition,
    VersionNotNegotiated,
    ProtocolError,
    Timeout,
    ConnectionRefused,
    ConnectionReset,
    NetworkUnreachable,
    Unexpected,
};

pub fn mapTransportError(err: anyerror) ClusterError {
    return switch (err) {
        error.Timeout => error.Timeout,
        error.ProtocolError => error.ProtocolError,
        error.ConnectionRefused => error.ConnectionRefused,
        error.ConnectionReset => error.ConnectionReset,
        error.NetworkUnreachable => error.NetworkUnreachable,
        else => error.Unexpected,
    };
}

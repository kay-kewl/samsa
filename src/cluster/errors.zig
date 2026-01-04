pub const ClusterError = error{
    NoBrokers,
    NoLeader,
    UnknownTopic,
    UnknownPartition,
    VersionNotNegotiated,
    ProtocolError,
    Timeout,
    Unexpected,
};

pub fn mapTransportError(err: anyerror) ClusterError {
    return switch (err) {
        error.Timeout => error.Timeout,
        error.ProtocolError => error.ProtocolError,
        else => error.Unexpected,
    };
}

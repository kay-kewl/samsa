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

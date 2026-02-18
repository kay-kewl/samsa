pub const Uuid = @import("codec.zig").Uuid;

pub const BrokerErrorCode = enum(i16) {
    NONE = 0,
    OFFSET_OUT_OF_RANGE = 1,
    UNKNOWN_TOPIC_OR_PARTITION = 3,
    LEADER_NOT_AVAILABLE = 5,
    NOT_LEADER_OR_FOLLOWER = 6,
    REQUEST_TIMED_OUT = 7,
    MESSAGE_TOO_LARGE = 10,
    NOT_ENOUGH_REPLICAS = 19,
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
    UNSUPPORTED_VERSION = 35,
    KAFKA_STORAGE_ERROR = 56,
    FENCED_LEADER_EPOCH = 74,
    UNKNOWN_LEADER_EPOCH = 75,
    REBOOTSTRAP_REQUIRED = 129,
};

pub const ApiKey = enum(i16) {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    ApiVersions = 18,
    _,
};

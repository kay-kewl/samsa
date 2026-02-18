const std = @import("std");

pub const Broker = struct {
    node_id: i32,
    host: []const u8,
    port: u16,
    owns_host: bool = false,
};

pub const TopicPartition = struct {
    topic: []const u8,
    partition: i32,
};

pub const PartitionLeader = struct {
    topic: []const u8,
    partition: i32,
    leader_id: i32,
};

pub const PartitionState = struct {
    error_code: i16,
    leader_id: ?i32,
    leader_epoch: ?i32,
    replica_ids: []const i32,
    isr_ids: []const i32,
    offline_replica_ids: []const i32,
};

pub const ClusterStatistics = struct {
    broker_count: usize,
    topic_count: usize,
    metadata_age_ms: i64,
    controller_id: i32,
    has_cluster_id: bool,
    next_metadata_retry_in_ms: i64,
    metadata_refresh_inflight: bool,
    metadata_refresh_attempts: u64,
    metadata_refresh_failures: u64,
    metadata_rebootstrap_count: u64,
    metadata_oversize_rejections: u64,
    metadata_refresh_blocked_inflight: u64,
    metadata_refresh_blocked_backoff: u64,
};

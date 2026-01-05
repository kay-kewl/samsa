const std = @import("std");

pub const Broker = struct {
    node_id: i32,
    host: []const u8,
    port: u16,
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

pub const ClusterStatistics = struct {
    broker_count: usize,
    topic_count: usize,
    metadata_age_ms: i64,
};

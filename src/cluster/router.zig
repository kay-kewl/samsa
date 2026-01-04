const std = @import("std");
const metadata_cache = @import("metadata_cache.zig");
const model = @import("model.zig");
const errors = @import("errors.zig");

pub fn leaderFor(cache: *const metadata_cache.Cache, topic: []const u8, partition: i32) errors.ClusterError!i32 {
    const parts = cache.leaders.get(topic) orelse return error.UnknownTopic;
    return parts.get(partition) orelse error.UnknownPartition;
}

pub fn anyBroker(cache: *const metadata_cache.Cache) errors.ClusterError!i32 {
    var it = cache.brokers.iterator();
    if (it.next()) |entry| {
        return entry.key_ptr.*;
    }

    return error.NoBrokers;
}

pub fn brokerFor(cache: *const metadata_cache.Cache, topic: []const u8, partition: i32) errors.ClusterError!model.Broker {
    const leader_id = try leaderFor(cache, topic, partition);
    return cache.brokers.get(leader_id) orelse error.NoLeader;
}

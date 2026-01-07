const std = @import("std");
const metadata_cache = @import("metadata_cache.zig");
const model = @import("model.zig");
const errors = @import("errors.zig");

pub fn leaderFor(cache: *const metadata_cache.Cache, topic: []const u8, partition: i32) errors.ClusterError!i32 {
    const by_partition = cache.partition_state.get(topic) orelse return error.UnknownTopic;
    const state = by_partition.get(partition) orelse return error.UnknownPartition;
    return state.leader_id orelse error.NoLeader;
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

const testing = std.testing;

test "router leaderFor and anyBroker behaviour" {
    var cache = metadata_cache.Cache.init(testing.allocator);
    defer cache.deinit();

    try cache.brokers.put(1, .{
        .node_id = 1,
        .host = "127.0.0.1",
        .port = 9092,
    });

    const topic_name = try testing.allocator.dupe(u8, "t1");
    var parts = std.AutoHashMap(i32, i32).init(testing.allocator);
    try parts.put(0, 1);
    try cache.leaders.put(topic_name, parts);

    try testing.expectEqual(@as(i32, 1), try leaderFor(&cache, "t1", 0));
    const b = try brokerFor(&cache, "t1", 0);
    try testing.expectEqual(@as(i32, 1), b.node_id);

    try testing.expectError(error.UnknownTopic, leaderFor(&cache, "missing", 0));
    try testing.expectError(error.UnknownPartition, leaderFor(&cache, "t1", 99));
}

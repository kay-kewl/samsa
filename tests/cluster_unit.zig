const std = @import("std");
const kafka = @import("kafka");

test "cluster versions registry behavior" {
    var registry = kafka.cluster.versions.Registry.init(std.testing.allocator);
    defer registry.deinit();

    try registry.by_api_key.put(@intFromEnum(kafka.protocol.types.ApiKey.Metadata), .{ .min = 4, .max = 12 });
    try std.testing.expectEqual(@as(i16, 4), try registry.choose(.Metadata, 1));
    try std.testing.expectEqual(@as(i16, 12), try registry.choose(.Metadata, 100));
    try std.testing.expectError(error.VersionNotNegotiated, registry.choose(.Fetch, 1));
}

test "metadata cache apply skips bad broker ports" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    const response = kafka.generated.metadata.Response{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
            .{
                .node_id = 2,
                .host = "b",
                .port = -1,
            },
        },
        .topics = &.{},
    };

    try cache.apply(response);
    try std.testing.expectEqual(@as(usize, 1), cache.brokers.count());
}

test "topic-only metadata refresh does not wipe other topic leaders" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    const full = kafka.generated.metadata.Response{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
        },
        .topics = &.{.{
            .error_code = 0,
            .name = "a",
            .partitions = &.{.{ .error_code = 0, .partition_index = 0, .leader_id = 1 }},
        }},
    };
    try cache.apply(full);

    const partial = kafka.generated.metadata.Response{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
        },
        .topics = &.{.{
            .error_code = 0,
            .name = "b",
            .partitions = &.{.{ .error_code = 0, .partition_index = 0, .leader_id = 1 }},
        }},
    };
    try cache.applyTopicOnly(partial);

    try std.testing.expect(cache.leaders.get("a") != null);
    try std.testing.expect(cache.leaders.get("b") != null);
}

test "cluster statistics report metadata age semantics" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{});
    defer c.deinit();

    const s0 = c.statistics();
    try std.testing.expectEqual(@as(i64, -1), s0.metadata_age_ms);

    c.metadata_epoch_ms = std.time.milliTimestamp();
    const s1 = c.statistics();
    try std.testing.expect(s1.metadata_age_ms >= 0);
}

test "metadata invalidation clears cache and epoch" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{});
    defer c.deinit();

    try c.cache.brokers.put(1, .{
        .node_id = 1,
        .host = "h",
        .port = 9092,
    });

    c.metadata_epoch_ms = std.time.milliTimestamp();
    c.invalidateMetadata();

    try std.testing.expectEqual(@as(usize, 0), c.cache.brokers.count());
    try std.testing.expectEqual(@as(i64, 0), c.metadata_epoch_ms);
}

test "metadata cache stores controller cluster and partition detail" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    const response = kafka.generated.metadata.Response{
        .cluster_id = "cluster-a",
        .controller_id = 7,
        .brokers = &.{
            .{
                .node_id = 7,
                .host = "a",
                .port = 9092,
            },
        },
        .topics = &.{
            .{
                .error_code = 0,
                .name = "t1",
                .topic_id = .{1} ** 16,
                .partitions = &.{
                    .{
                        .error_code = 0,
                        .partition_index = 0,
                        .leader_id = 7,
                        .leader_epoch = 22,
                        .replica_nodes = &.{ 7, 8 },
                        .isr_nodes = &.{7},
                        .offline_replicas = &.{8},
                    },
                },
            },
        },
    };

    try cache.apply(response);

    try std.testing.expectEqual(@as(i32, 7), cache.controller_id);
    try std.testing.expect(cache.cluster_id != null);

    const states = cache.partition_state.get("t1") orelse return error.TestUnexpectedResult;
    const p0 = states.get(0) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqual(@as(i16, 0), p0.error_code);
    try std.testing.expectEqual(@as(?i32, 7), p0.leader_id);
    try std.testing.expectEqual(@as(?i32, 22), p0.leader_epoch);
    try std.testing.expectEqual(@as(usize, 2), p0.replica_ids.len);
    try std.testing.expectEqual(@as(usize, 1), p0.isr_ids.len);
    try std.testing.expectEqual(@as(usize, 1), p0.offline_replica_ids.len);
}

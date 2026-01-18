const std = @import("std");
const kafka = @import("kafka");

test "cluster versions registry behavior" {
    var registry = kafka.cluster.versions.Registry.init(std.testing.allocator);
    defer registry.deinit();

    try registry.by_api_key.put(@intFromEnum(kafka.protocol.types.ApiKey.Metadata), .{ .min = 4, .max = 12 });
    try std.testing.expectEqual(@as(i16, 12), try registry.choose(.Metadata));

    try registry.by_api_key.put(@intFromEnum(kafka.protocol.types.ApiKey.Metadata), .{ .min = 4, .max = 10 });
    try std.testing.expectError(error.NoSupportedVersion, registry.choose(.Metadata));

    try std.testing.expectError(error.VersionNotNegotiated, registry.choose(.Fetch));
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
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{ .request_timeout_ms = 250 });
    defer c.deinit();

    const s0 = c.statistics();
    try std.testing.expectEqual(@as(i64, -1), s0.metadata_age_ms);

    c.metadata_epoch_ms = std.time.milliTimestamp();
    const s1 = c.statistics();
    try std.testing.expect(s1.metadata_age_ms >= 0);
}

test "metadata invalidation clears cache and epoch" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{ .request_timeout_ms = 250 });
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

test "cluster statistics expose retry and identity fields" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{ .request_timeout_ms = 250 });
    defer c.deinit();

    c.next_metadata_retry_ms = std.time.milliTimestamp() + 200;
    c.metadata_refresh_inflight = true;
    c.cache.controller_id = 11;
    c.cache.cluster_id = try std.testing.allocator.dupe(u8, "cid");

    defer {
        if (c.cache.cluster_id) |cid| {
            std.testing.allocator.free(cid);
        }

        c.cache.cluster_id = null;
    }

    const stats = c.statistics();
    try std.testing.expectEqual(@as(i32, 11), stats.controller_id);
    try std.testing.expect(stats.has_cluster_id);
    try std.testing.expect(stats.next_metadata_retry_in_ms >= 0);
    try std.testing.expect(stats.metadata_refresh_inflight);
}

test "topic-only apply preserves topic map replacement with same topic id" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    const first = kafka.generated.metadata.Response{
        .topics = &.{
            .{
                .error_code = 0,
                .name = "t1",
                .topic_id = .{2} ** 16,
                .partitions = &.{
                    .{
                        .error_code = 0,
                        .partition_index = 0,
                        .leader_id = 1,
                    },
                },
            },
        },
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
        },
    };

    try cache.apply(first);
    const second = kafka.generated.metadata.Response{
        .topics = &.{
            .{
                .error_code = 0,
                .name = "t1",
                .topic_id = .{2} ** 16,
                .partitions = &.{
                    .{
                        .error_code = 0,
                        .partition_index = 1,
                        .leader_id = 1,
                    },
                },
            },
        },
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
        },
    };

    try cache.applyTopicOnly(second);

    const leaders = cache.leaders.get("t1") orelse return error.TestUnexpectedResult;
    try std.testing.expect(leaders.get(1) != null);
    try std.testing.expect(leaders.get(0) == null);
}

test "refreshMetadataNow returns MetadataUnavailable while refresh in progress" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{ .request_timeout_ms = 250 });
    defer c.deinit();

    c.metadata_refresh_inflight = true;
    try std.testing.expectError(error.MetadataUnavailable, c.refreshMetadataNow());
}

test "brokerForTopicPartition uses existing cache when refresh is temporarily unavailable" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{ .request_timeout_ms = 250 });
    defer c.deinit();

    try c.cache.brokers.put(1, .{
        .node_id = 1,
        .host = "127.0.0.1",
        .port = 9092,
    });

    const topic_name = try std.testing.allocator.dupe(u8, "topic-a");
    var parts = std.AutoHashMap(i32, kafka.cluster.model.PartitionState).init(std.testing.allocator);
    try parts.put(0, .{
        .error_code = 0,
        .leader_id = 1,
        .leader_epoch = null,
        .replica_ids = &.{},
        .isr_ids = &.{},
        .offline_replica_ids = &.{},
    });
    try c.cache.partition_state.put(topic_name, parts);

    c.metadata_epoch_ms = 0;
    c.metadata_refresh_inflight = true;

    const b = try c.brokerForTopicPartition("topic-a", 0);
    try std.testing.expectEqual(@as(i32, 1), b.node_id);
}

test "topic generation increments when topic id changes in full apply" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    try cache.apply(.{
        .brokers = &.{},
        .topics = &.{.{
            .error_code = 0,
            .name = "tgen",
            .topic_id = .{1} ** 16,
            .partitions = &.{},
        }},
    });

    try cache.apply(.{
        .brokers = &.{},
        .topics = &.{.{
            .error_code = 0,
            .name = "tgen",
            .topic_id = .{2} ** 16,
            .partitions = &.{},
        }},
    });

    try std.testing.expect((cache.topicGeneration("tgen") orelse 0) > 0);
}

test "topic generation increments when topic id changes in topic-only apply" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    try cache.apply(.{
        .brokers = &.{},
        .topics = &.{.{
            .error_code = 0,
            .name = "ttopic",
            .topic_id = .{3} ** 16,
            .partitions = &.{},
        }},
    });

    try cache.applyTopicOnly(.{
        .brokers = &.{},
        .topics = &.{.{
            .error_code = 0,
            .name = "ttopic",
            .topic_id = .{4} ** 16,
            .partitions = &.{},
        }},
    });

    try std.testing.expect((cache.topicGeneration("ttopic") orelse 0) > 0);
}

test "metadata cache applyBrokersOnly preserves topic maps" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    try cache.apply(.{
        .brokers = &.{.{
            .node_id = 1,
            .host = "a",
            .port = 9092,
        }},
        .topics = &.{.{
            .error_code = 0,
            .name = "topic-a",
            .topic_id = .{1} ** 16,
            .partitions = &.{.{
                .error_code = 0,
                .partition_index = 0,
                .leader_id = 1,
            }},
        }},
    });

    try cache.applyBrokersOnly(.{
        .controller_id = 2,
        .brokers = &.{.{
            .node_id = 2,
            .host = "b",
            .port = 9093,
        }},
        .topics = &.{},
    });

    try std.testing.expectEqual(@as(usize, 1), cache.brokers.count());
    try std.testing.expect(cache.leaders.get("topic-a") != null);
    try std.testing.expect(cache.topic_ids.get("topic-a") != null);
}

test "topic generation tracks all recreated topics in one full apply" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    try cache.apply(.{
        .brokers = &.{},
        .topics = &.{
            .{ .error_code = 0, .name = "a", .topic_id = .{1} ** 16, .partitions = &.{} },
            .{ .error_code = 0, .name = "b", .topic_id = .{1} ** 16, .partitions = &.{} },
        },
    });

    try cache.apply(.{
        .brokers = &.{},
        .topics = &.{
            .{ .error_code = 0, .name = "a", .topic_id = .{2} ** 16, .partitions = &.{} },
            .{ .error_code = 0, .name = "b", .topic_id = .{2} ** 16, .partitions = &.{} },
        },
    });

    try std.testing.expect((cache.topicGeneration("a") orelse 0) > 0);
    try std.testing.expect((cache.topicGeneration("b") orelse 0) > 0);
}

test "topic generation prunes when full apply has no topics" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    try cache.apply(.{
        .brokers = &.{},
        .topics = &.{.{ .error_code = 0, .name = "prune-me", .topic_id = .{1} ** 16, .partitions = &.{} }},
    });

    try cache.apply(.{
        .brokers = &.{},
        .topics = &.{.{ .error_code = 0, .name = "prune-me", .topic_id = .{2} ** 16, .partitions = &.{} }},
    });

    try std.testing.expect((cache.topicGeneration("prune-me") orelse 0) > 0);

    try cache.apply(.{
        .brokers = &.{},
        .topics = &.{},
    });

    try std.testing.expect(cache.topicGeneration("prune-me") == null);
}

test "triggerRebootstrap resets cluster runtime state" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{ .request_timeout_ms = 250 });
    defer c.deinit();

    c.metadata_epoch_ms = std.time.milliTimestamp();
    c.next_metadata_retry_ms = std.time.milliTimestamp() + 1000;
    c.metadata_refresh_not_before_ms = std.time.milliTimestamp() + 1000;
    c.metadata_retry_backoff_ms = 1000;

    try c.cache.brokers.put(1, .{
        .node_id = 1,
        .host = "127.0.0.1",
        .port = 9092,
    });

    c.triggerRebootstrap();

    try std.testing.expectEqual(@as(usize, 0), c.cache.brokers.count());
    try std.testing.expectEqual(@as(i64, 0), c.metadata_epoch_ms);
    try std.testing.expectEqual(@as(i32, 200), c.metadata_retry_backoff_ms);
    try std.testing.expectEqual(@as(i64, 0), c.next_metadata_retry_ms);
    try std.testing.expectEqual(@as(i64, 0), c.metadata_refresh_not_before_ms);
}

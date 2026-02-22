const std = @import("std");
const kafka = @import("kafka");

fn expectPreferredWithinBounds(
    preferred: []const i16,
    request_min: i16,
    request_max: i16,
    response_min: i16,
    response_max: i16,
) !void {
    try std.testing.expect(preferred.len > 0);
    for (preferred) |v| {
        try std.testing.expect(v >= request_min and v <= request_max);
        try std.testing.expect(v >= response_min and v <= response_max);
    }
}

test "preferred versions stay within generated request/response bounds" {
    const preferred = kafka.cluster.versions.Registry.preferredVersions;

    try expectPreferredWithinBounds(
        preferred(.Fetch),
        kafka.generated.fetch.request_min_version,
        kafka.generated.fetch.request_max_version,
        kafka.generated.fetch.response_min_version,
        kafka.generated.fetch.response_max_version,
    );
    try expectPreferredWithinBounds(
        preferred(.ListOffsets),
        kafka.generated.list_offsets.request_min_version,
        kafka.generated.list_offsets.request_max_version,
        kafka.generated.list_offsets.response_min_version,
        kafka.generated.list_offsets.response_max_version,
    );
    try expectPreferredWithinBounds(
        preferred(.Metadata),
        kafka.generated.metadata.request_min_version,
        kafka.generated.metadata.request_max_version,
        kafka.generated.metadata.response_min_version,
        kafka.generated.metadata.response_max_version,
    );
    try expectPreferredWithinBounds(
        preferred(.Produce),
        kafka.generated.produce.request_min_version,
        kafka.generated.produce.request_max_version,
        kafka.generated.produce.response_min_version,
        kafka.generated.produce.response_max_version,
    );
}

test "runtime preferred versions are pinned to v1 profile" {
    const preferred = kafka.cluster.versions.Registry.preferredVersions;
    try std.testing.expectEqualSlices(i16, &.{ 4, 2 }, preferred(.ApiVersions));
    try std.testing.expectEqualSlices(i16, &.{12}, preferred(.Metadata));
    try std.testing.expectEqualSlices(i16, &.{12}, preferred(.Produce));
    try std.testing.expectEqualSlices(i16, &.{12}, preferred(.Fetch));
    try std.testing.expectEqualSlices(i16, &.{10}, preferred(.ListOffsets));
}

test "runtime supported-version helper rejects out-of-profile versions" {
    const Registry = kafka.cluster.versions.Registry;

    try std.testing.expect(Registry.isRuntimeSupportedVersion(.ApiVersions, 4));
    try std.testing.expect(Registry.isRuntimeSupportedVersion(.ApiVersions, 2));
    try std.testing.expect(!Registry.isRuntimeSupportedVersion(.ApiVersions, 3));

    try std.testing.expect(Registry.isRuntimeSupportedVersion(.Fetch, 12));
    try std.testing.expect(!Registry.isRuntimeSupportedVersion(.Fetch, 13));
}

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

test "metadata cache apply deep-copies broker host memory" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    var host_buf = [_]u8{ 'a', 'b', 'c' };
    const response = kafka.generated.metadata.Response{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = host_buf[0..],
                .port = 9092,
            },
        },
        .topics = &.{},
    };

    try cache.apply(response);
    host_buf[0] = 'z';

    const broker = cache.brokers.get(1) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("abc", broker.host);
    try std.testing.expect(broker.owns_host);
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

test "topic-only metadata refresh keeps last-known-good topic as transient error" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    try cache.apply(.{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
        },
        .topics = &.{.{
            .error_code = 0,
            .name = "t1",
            .topic_id = .{1} ** 16,
            .partitions = &.{.{ .error_code = 0, .partition_index = 0, .leader_id = 1 }},
        }},
    });

    try cache.applyTopicOnly(.{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
        },
        .topics = &.{.{
            .error_code = 6,
            .name = "t1",
            .partitions = &.{},
        }},
    });

    try std.testing.expect(cache.leaders.get("t1") != null);
    try std.testing.expect(cache.partition_state.get("t1") != null);
}

test "topic-only metadata refresh keeps topic on NOT_LEADER_OR_FOLLOWER" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    try cache.apply(.{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
        },
        .topics = &.{.{
            .error_code = 0,
            .name = "t1",
            .topic_id = .{1} ** 16,
            .partitions = &.{.{ .error_code = 0, .partition_index = 0, .leader_id = 1 }},
        }},
    });

    try cache.applyTopicOnly(.{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
        },
        .topics = &.{.{
            .error_code = 6,
            .name = "t1",
            .partitions = &.{},
        }},
    });

    try std.testing.expect(cache.leaders.get("t1") != null);
    try std.testing.expect(cache.partition_state.get("t1") != null);
    try std.testing.expect(cache.topic_ids.get("t1") != null);
}

test "topic-only metadata refresh drops topic on UNKNOWN_TOPIC_OR_PARTITION" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    try cache.apply(.{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
        },
        .topics = &.{.{
            .error_code = 0,
            .name = "t1",
            .topic_id = .{1} ** 16,
            .partitions = &.{.{ .error_code = 0, .partition_index = 0, .leader_id = 1 }},
        }},
    });

    try cache.applyTopicOnly(.{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
        },
        .topics = &.{.{
            .error_code = 3,
            .name = "t1",
            .partitions = &.{},
        }},
    });

    try std.testing.expect(cache.leaders.get("t1") == null);
    try std.testing.expect(cache.partition_state.get("t1") == null);
    try std.testing.expect(cache.topic_ids.get("t1") == null);
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

test "refreshMetadataWithDeadline returns MetadataUnavailable when refresh slot is inflight" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{ .request_timeout_ms = 250 });
    defer c.deinit();

    c.metadata_refresh_inflight = true;
    try std.testing.expectError(error.MetadataUnavailable, c.refreshMetadataWithDeadline(std.time.milliTimestamp()));
    try std.testing.expectEqual(@as(u64, 1), c.statistics().metadata_refresh_blocked_inflight);
}

test "refreshMetadataWithDeadline returns RetryBackoffActive when retry gate is active" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{ .request_timeout_ms = 250 });
    defer c.deinit();

    const now = std.time.milliTimestamp();
    c.next_metadata_retry_ms = now + 5_000;

    try std.testing.expectError(error.RetryBackoffActive, c.refreshMetadataWithDeadline(now));
    try std.testing.expectEqual(@as(u64, 1), c.statistics().metadata_refresh_blocked_backoff);
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
    const leader_topic_name = try std.testing.allocator.dupe(u8, "topic-a");
    var leaders = std.AutoHashMap(i32, i32).init(std.testing.allocator);
    try leaders.put(0, 1);
    try c.cache.leaders.put(leader_topic_name, leaders);

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
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{
        .request_timeout_ms = 250,
        .metadata_retry_backoff_ms = 77,
    });
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
    try std.testing.expectEqual(@as(i32, 77), c.metadata_retry_backoff_ms);
    try std.testing.expectEqual(@as(i64, 0), c.next_metadata_retry_ms);
    try std.testing.expectEqual(@as(i64, 0), c.metadata_refresh_not_before_ms);
}

test "cluster config wires max_total_connections into pool" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{
        .max_total_connections = 3,
    });
    defer c.deinit();

    try std.testing.expectEqual(@as(?usize, 3), c.pool.max_total_connections);
}

test "cluster config validate rejects invalid metadata timings and limits" {
    try std.testing.expectError(error.InvalidConfiguration, (kafka.cluster.cluster.Config{
        .metadata_ttl_ms = 0,
    }).validate());

    try std.testing.expectError(error.InvalidConfiguration, (kafka.cluster.cluster.Config{
        .metadata_retry_backoff_ms = 500,
        .metadata_retry_backoff_cap_ms = 100,
    }).validate());

    try std.testing.expectError(error.InvalidConfiguration, (kafka.cluster.cluster.Config{
        .protocol_limits = .{ .decode_depth_max = 0 },
    }).validate());
}

test "cluster config validate rejects zero metadata soft caps" {
    try std.testing.expectError(error.InvalidConfiguration, (kafka.cluster.cluster.Config{
        .max_metadata_topics = 0,
    }).validate());

    try std.testing.expectError(error.InvalidConfiguration, (kafka.cluster.cluster.Config{
        .max_metadata_partitions = 0,
    }).validate());
}

test "cluster metadata soft caps reject oversized metadata shapes" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{
        .max_metadata_topics = 1,
        .max_metadata_partitions = 1,
    });
    defer c.deinit();

    const t0 = kafka.generated.metadata.Response.MetadataResponseTopic{
        .error_code = 0,
        .name = "a",
        .partitions = &.{.{ .error_code = 0, .partition_index = 0, .leader_id = 1 }},
    };
    const t1 = kafka.generated.metadata.Response.MetadataResponseTopic{
        .error_code = 0,
        .name = "b",
        .partitions = &.{.{ .error_code = 0, .partition_index = 0, .leader_id = 1 }},
    };

    const response = kafka.generated.metadata.Response{
        .brokers = &.{},
        .topics = &[_]kafka.generated.metadata.Response.MetadataResponseTopic{ t0, t1 },
    };

    try std.testing.expect(!c.metadataFitsSoftCaps(response));
}

test "cluster init picks metadata timing knobs from config" {
    var c = kafka.cluster.cluster.Cluster.init(std.testing.allocator, .{
        .metadata_ttl_ms = 1234,
        .metadata_retry_backoff_ms = 77,
        .metadata_retry_backoff_cap_ms = 777,
        .metadata_refresh_backoff_ms = 33,
    });
    defer c.deinit();

    try std.testing.expectEqual(@as(i32, 1234), c.metadata_ttl_ms);
    try std.testing.expectEqual(@as(i32, 77), c.metadata_retry_backoff_ms);
    try std.testing.expectEqual(@as(i32, 777), c.metadata_retry_backoff_cap_ms);
    try std.testing.expectEqual(@as(i32, 33), c.metadata_refresh_backoff_ms);
}

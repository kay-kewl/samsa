const std = @import("std");
const kafka = @import("kafka");
const fake = @import("fake_broker_harness.zig");

fn requireScriptedFakeBrokerSuite() !void {
    if (std.posix.getenv("SAMSA_FAKE_BROKER_REQUIRED") == null) {
        return error.SkipZigTest;
    }
}

fn bootstrapBrokerId(host: []const u8, port: u16) i32 {
    var hash = std.hash.Wyhash.init(42);
    hash.update(host);

    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, port, .big);
    hash.update(&buf);

    return @intCast(hash.final() & 0x7fff_ffff);
}

fn seedSinglePartitionState(
    c: *kafka.cluster.cluster.Cluster,
    topic: []const u8,
    broker_id: i32,
    leader_epoch: i32,
) !void {
    try c.cache.brokers.put(broker_id, .{
        .node_id = broker_id,
        .host = "127.0.0.1",
        .port = 9092,
    });

    const topic_name = try std.testing.allocator.dupe(u8, topic);
    var parts = std.AutoHashMap(i32, kafka.cluster.model.PartitionState).init(std.testing.allocator);
    try parts.put(0, .{
        .error_code = 0,
        .leader_id = broker_id,
        .leader_epoch = leader_epoch,
        .replica_ids = try std.testing.allocator.alloc(i32, 0),
        .isr_ids = try std.testing.allocator.alloc(i32, 0),
        .offline_replica_ids = try std.testing.allocator.alloc(i32, 0),
    });

    try c.cache.partition_state.put(topic_name, parts);
    c.metadata_epoch_ms = std.time.milliTimestamp();
}

fn seedV1VersionRanges(c: *kafka.cluster.cluster.Cluster, broker_id: i32) !void {
    var by_api = std.AutoHashMap(i16, kafka.cluster.versions.Range).init(std.testing.allocator);
    try by_api.put(@intFromEnum(kafka.protocol.types.ApiKey.Metadata), .{ .min = 12, .max = 12 });
    try by_api.put(@intFromEnum(kafka.protocol.types.ApiKey.Produce), .{ .min = 12, .max = 12 });
    try by_api.put(@intFromEnum(kafka.protocol.types.ApiKey.Fetch), .{ .min = 12, .max = 12 });
    try by_api.put(@intFromEnum(kafka.protocol.types.ApiKey.ListOffsets), .{ .min = 10, .max = 10 });
    try by_api.put(@intFromEnum(kafka.protocol.types.ApiKey.ApiVersions), .{ .min = 2, .max = 4 });
    try c.broker_version_ranges.put(broker_id, by_api);

    try c.version_registry.by_api_key.put(@intFromEnum(kafka.protocol.types.ApiKey.Metadata), .{ .min = 12, .max = 12 });
    try c.version_registry.by_api_key.put(@intFromEnum(kafka.protocol.types.ApiKey.Produce), .{ .min = 12, .max = 12 });
    try c.version_registry.by_api_key.put(@intFromEnum(kafka.protocol.types.ApiKey.Fetch), .{ .min = 12, .max = 12 });
    try c.version_registry.by_api_key.put(@intFromEnum(kafka.protocol.types.ApiKey.ListOffsets), .{ .min = 10, .max = 10 });
    try c.version_registry.by_api_key.put(@intFromEnum(kafka.protocol.types.ApiKey.ApiVersions), .{ .min = 2, .max = 4 });
}

fn attachReadyConnection(c: *kafka.cluster.cluster.Cluster, broker_id: i32, fd: std.posix.fd_t) !void {
    c.pool.remove(broker_id);

    var conn = kafka.transport.connection.Connection.init(std.testing.allocator, .{
        .host = "127.0.0.1",
        .port = 9092,
        .connect_timeout_ms = 50,
        .request_timeout_ms = 200,
    });
    conn.stream = .{ .handle = fd };
    conn.state = .Ready;
    conn.correlation_id = 1;

    try c.pool.map.put(broker_id, conn);
}

fn encodeProduceResponseFrame(
    allocator: std.mem.Allocator,
    correlation_id: i32,
    topic: []const u8,
    partition: i32,
    error_code: i16,
    base_offset: i64,
) ![]u8 {
    var buf: [8192]u8 = undefined;
    var e = kafka.protocol.codec.Encoder.init(&buf);

    const p = kafka.generated.produce.Response.TopicProduceResponse.PartitionProduceResponse{
        .index = partition,
        .error_code = error_code,
        .base_offset = base_offset,
    };

    const t = kafka.generated.produce.Response.TopicProduceResponse.PartitionProduceResponse{
        .name = topic,
        .partition_responses = &[_]kafka.generated.produce.Response.TopicProduceResponse.PartitionProduceResponse{p},
    };

    const body = kafka.generated.produce.Response{
        .responses = &[_]kafka.generated.produce.Response.TopicProduceResponse{t},
        .throttle_time_ms = 0,
    };

    try body.encode(&e, 12);
    return fake.wrapKafkaResponseFrame(allocator, correlation_id, .v1, e.written());
}

fn encodeMetadataResponseFrame(
    allocator: std.mem.Allocator,
    correlation_id: i32,
    topic: []const u8,
    broker_id: i32,
    leader_epoch: i32,
) ![]u8 {
    var buf: [16 * 1024]u8 = undefined;
    var e = kafka.protocol.codec.Encoder.init(&buf);

    const broker = kafka.generated.metadata.Response.MetadataResponseBroker{
        .node_id = broker_id,
        .host = "127.0.0.1",
        .port = 9092,
    };

    const part = kafka.generated.metadata.Response.MetadataResponseTopic.MetadataResponsePartition{
        .error_code = 0,
        .partition_index = 0,
        .leader_id = broker_id,
        .leader_epoch = leader_epoch,
        .replica_nodes = &[_]i32{broker_id},
        .isr_nodes = &[_]i32{broker_id},
        .offline_replicas = &[_]i32{},
    };

    const top = kafka.generated.metadata.Response.MetadataResponseTopic{
        .error_code = 0,
        .name = topic,
        .partitions = &[_]kafka.generated.metadata.Response.MetadataResponseTopic.MetadataResponsePartition{part},
    };

    const body = kafka.generated.metadata.Response{
        .brokers = &[_]kafka.generated.metadata.Response.MetadataResponseBroker{broker},
        .topics = &[_]kafka.generated.metadata.Response.MetadataResponseTopic{top},
    };

    try body.encode(&e, 12);
    return fake.wrapKafkaResponseFrame(allocator, correlation_id, .v1, e.written());
}

fn encodeFetchResponseFrame(
    allocator: std.mem.Allocator,
    correlation_id: i32,
    topic: []const u8,
    partition: i32,
    error_code: i16,
    records: ?[]const u8,
) ![]u8 {
    var buf: [16 * 1024]u8 = undefined;
    var e = kafka.protocol.codec.Encoder.init(&buf);

    const p = kafka.generated.fetch.Response.FetchableTopicResponse.PartitionData{
        .partition_index = partition,
        .error_code = error_code,
        .high_watermark = 0,
        .records = records,
    };

    const t = kafka.generated.fetch.Response.FetchableTopicResponse{
        .name = topic,
        .partitions = &[_]kafka.generated.fetch.Response.FetchableTopicResponse.PartitionData{p},
    };

    const body = kafka.generated.fetch.Response{
        .throttle_time_ms = 0,
        .error_code = 0,
        .session_id = 0,
        .responses = &[_]kafka.generated.fetch.Response.FetchableTopicResponse{t},
    };

    try body.encode(&e, 12);
    return fake.wrapKafkaResponseFrame(allocator, correlation_id, .v1, e.written());
}

test "classification: route refresh matrix remains stable for broker topology codes" {
    const route_codes = [_]i16{ 6, 74, 75, 129 };
    for (route_codes) |code| {
        try std.testing.expect(kafka.client.client.isRouteRefreshError(code));
    }

    const non_route_codes = [_]i16{ 1, 7, 10, 19, 20, 56, 42 };
    for (non_route_codes) |code| {
        try std.testing.expect(!kafka.client.client.isRouteRefreshError(code));
    }
}

test "classification: retryable send error includes stale metadata path" {
    try std.testing.expect(kafka.client.client.isRetryableSendError(error.StaleMetadata));
    try std.testing.expect(kafka.client.client.isRetryableSendError(error.Timeout));
    try std.testing.expect(kafka.client.client.isRetryableSendError(error.EndOfStream));
    try std.testing.expect(!kafka.client.client.isRetryableSendError(error.InvalidConfiguration));
}

test "scripted producer: NOT_LEADER_OR_FOLLOWER refreshes metadata and retries successfully" {
    try requireScriptedFakeBrokerSuite();

    const allocator = std.testing.allocator;
    const broker_id = bootstrapBrokerId("127.0.0.1", 9092);

    var p = try kafka.client.Producer.init(allocator, .{
        .bootstrap_host = "127.0.0.1",
        .bootstrap_port = 9092,
        .connect_timeout_ms = 100,
        .request_timeout_ms = 300,
    }, .{
        .request_ms = 300,
        .retries_max_attempts = 3,
    });
    defer p.deinit();

    try seedSinglePartitionState(&p.cluster, "events", broker_id, 9);
    try seedV1VersionRanges(&p.cluster, broker_id);

    const pair = try fake.socketPairStream();

    const r1 = try encodeProduceResponseFrame(allocator, 1, "events", 0, 6, -1);
    defer allocator.free(r1);

    const r2 = try encodeMetadataResponseFrame(allocator, 2, "events", broker_id, 10);
    defer allocator.free(r2);

    const r3 = try encodeProduceResponseFrame(allocator, 3, "events", 0, 0, 41);
    defer allocator.free(r3);

    var h = fake.Harness.init(allocator, &[_]fake.ScriptedExchange{
        .{
            .response_frame = r1,
        },
        .{
            .response_frame = r2,
        },
        .{
            .response_frame = r3,
        },
    });
    defer h.deinit();

    const t = try std.Thread.spawn(.{}, struct {
        fn run(hh: *fake.Harness, fd: std.posix.fd_t) void {
            hh.runOnAcceptedStream(.{ .handle = fd }) catch {};
        }
    }.run, .{ &h, pair[0] });

    try attachReadyConnection(&p.cluster, broker_id, pair[1]);

    const out = try p.send("events", "k", "v");
    t.join();

    try std.testing.expectEqual(@as(i64, 41), out.base_offset);
    try std.testing.expect(p.getStatistics().produce_retries >= 1);
    try std.testing.expect(p.getStatistics().metadata_refresh_attempts >= 1);

    try std.testing.expectEqual(@as(usize, 3), h.captures.items.len);

    const e0 = try fake.decodeRequestEnvelope(h.captures.items[0].frame);
    const e1 = try fake.decodeRequestEnvelope(h.captures.items[1].frame);
    const e2 = try fake.decodeRequestEnvelope(h.captures.items[2].frame);

    try std.testing.expectEqual(@as(i16, 0), e0.api_key);
    try std.testing.expectEqual(@as(i16, 3), e1.api_key);
    try std.testing.expectEqual(@as(i16, 0), e2.api_key);
}

test "scripted producer: UNKNOWN_LEADER_EPOCH clears epoch then succeeds after metadata refresh" {
    try requireScriptedFakeBrokerSuite();

    const allocator = std.testing.allocator;
    const broker_id = bootstrapBrokerId("127.0.0.1", 9092);

    var p = try kafka.client.Producer.init(allocator, .{
        .bootstrap_host = "127.0.0.1",
        .bootstrap_port = 9092,
        .connect_timeout_ms = 100,
        .request_timeout_ms = 300,
    }, .{
        .request_ms = 300,
        .retries_max_attempts = 3,
    });
    defer p.deinit();

    try seedSinglePartitionState(&p.cluster, "events", broker_id, 9);
    try seedV1VersionRanges(&p.cluster, broker_id);
    try std.testing.expectEqual(@as(?i32, 9), p.cluster.leaderEpochFor("events", 0));

    const pair = try fake.socketPairStream();

    const r1 = try encodeProduceResponseFrame(allocator, 1, "events", 0, 75, -1);
    defer allocator.free(r1);

    const r2 = try encodeMetadataResponseFrame(allocator, 2, "events", broker_id, 17);
    defer allocator.free(r2);

    const r3 = try encodeProduceResponseFrame(allocator, 3, "events", 0, 0, 99);
    defer allocator.free(r3);

    var h = fake.Harness.init(allocator, &[_]fake.ScriptedExchange{
        .{
            .response_frame = r1,
        },
        .{
            .response_frame = r2,
        },
        .{
            .response_frame = r3,
        },
    });
    defer h.deinit();

    const t = try std.Thread.spawn(.{}, struct {
        fn run(hh: *fake.Harness, fd: std.posix.fd_t) void {
            hh.runOnAcceptedStream(.{ .handle = fd }) catch {};
        }
    }.run, .{ &h, pair[0] });

    try attachReadyConnection(&p.cluster, broker_id, pair[1]);

    const out = try p.send("events", "k", "v");
    t.join();

    try std.testing.expectEqual(@as(i64, 99), out.base_offset);
    try std.testing.expectEqual(@as(?i32, 17), p.cluster.leaderEpochFor("events", 0));
    try std.testing.expect(p.getStatistics().metadata_refresh_attempts >= 1);
}

test "scripted producer: UNKNOWN_LEADER_EPOCH triggers refresh and subsequent fetch succeeds" {
    try requireScriptedFakeBrokerSuite();

    const allocator = std.testing.allocator;
    const broker_id = bootstrapBrokerId("127.0.0.1", 9092);

    var c = try kafka.client.Consumer.init(allocator, .{
        .bootstrap_host = "127.0.0.1",
        .bootstrap_port = 9092,
        .connect_timeout_ms = 100,
        .request_timeout_ms = 300,
    }, .{
        .request_ms = 300,
        .retries_max_attempts = 3,
    });
    defer c.deinit();

    try seedSinglePartitionState(&c.cluster, "events", broker_id, 9);
    try seedV1VersionRanges(&c.cluster, broker_id);

    try c.assign("events", 0);
    try c.seek("events", 0, 0);

    const pair = try fake.socketPairStream();

    const r1 = try encodeFetchResponseFrame(allocator, 1, "events", 0, 75, null);
    defer allocator.free(r1);

    const r2 = try encodeMetadataResponseFrame(allocator, 2, "events", broker_id, 11);
    defer allocator.free(r2);

    const r3 = try encodeProduceResponseFrame(allocator, 3, "events", 0, 0, null);
    defer allocator.free(r3);

    var h = fake.Harness.init(allocator, &[_]fake.ScriptedExchange{
        .{
            .response_frame = r1,
        },
        .{
            .response_frame = r2,
        },
        .{
            .response_frame = r3,
        },
    });
    defer h.deinit();

    const t = try std.Thread.spawn(.{}, struct {
        fn run(hh: *fake.Harness, fd: std.posix.fd_t) void {
            hh.runOnAcceptedStream(.{ .handle = fd }) catch {};
        }
    }.run, .{ &h, pair[0] });

    try attachReadyConnection(&c.cluster, broker_id, pair[1]);

    const out = try c.poll(300);
    _ = out;
    t.join();

    try std.testing.expect(c.getStatistics().poll_retries >= 1);
    try std.testing.expect(c.getStatistics().metadata_refresh_attempts >= 1);
    try std.testing.expectEqual(@as(?i32, 11), c.cluster.leaderEpochFor("events", 0));
}

test "scripted consumer: mid-flight disconnect is retryable with connection replacement" {
    try requireScriptedFakeBrokerSuite();

    const allocator = std.testing.allocator;
    const broker_id = bootstrapBrokerId("127.0.0.1", 9092);

    var c = try kafka.client.Consumer.init(allocator, .{
        .bootstrap_host = "127.0.0.1",
        .bootstrap_port = 9092,
        .connect_timeout_ms = 100,
        .request_timeout_ms = 300,
    }, .{
        .request_ms = 300,
        .retries_max_attempts = 4,
    });
    defer c.deinit();

    try seedSinglePartitionState(&c.cluster, "events", broker_id, 9);
    try seedV1VersionRanges(&c.cluster, broker_id);

    try c.assign("events", 0);
    try c.seek("events", 0, 0);

    const pair1 = try fake.socketPairStream();
    const pair2 = try fake.socketPairStream();

    const ok_fetch = try encodeFetchResponseFrame(allocator, 1, "events", 0, 0, null);
    defer allocator.free(ok_fetch);

    var h1 = fake.Harness.init(allocator, &[_]fake.ScriptedExchange{
        .{
            .response_frame = "",
            .close_without_response = true,
        },
    });
    defer h1.deinit();

    var h2 = fake.Harness.init(allocator, &[_]fake.ScriptedExchange{
        .{
            .response_frame = ok_fetch,
        },
    });
    defer h2.deinit();

    const t1 = try std.Thread.spawn(.{}, struct {
        fn run(hh: *fake.Harness, fd: std.posix.fd_t) void {
            hh.runOnAcceptedStream(.{ .handle = fd }) catch {};
        }
    }.run, .{ &h1, pair1[0] });

    const t2 = try std.Thread.spawn(.{}, struct {
        fn run(hh: *fake.Harness, fd: std.posix.fd_t) void {
            hh.runOnAcceptedStream(.{ .handle = fd }) catch {};
        }
    }.run, .{ &h2, pair2[0] });

    try attachReadyConnection(&c.cluster, broker_id, pair1[1]);

    const swapper = try std.Thread.spawn(.{}, struct {
        fn run(cc: *kafka.client.Consumer, id: i32, fd: std.posix.fd_t) void {
            std.Thread.sleep(5 * std.time.ns_per_ms);
            cc.cluster.pool.remove(id);
            attachReadyConnection(&cc.cluster, id, fd) catch {};
        }
    }.run, .{ &c, broker_id, pair2[1] });

    const out = try c.poll(300);
    _ = out;

    swapper.join();
    t1.join();
    t2.join();

    try std.testing.expect(c.getStatistics().connection_drop_events >= 1);
    try std.testing.expect(c.getStatistics().poll_retries >= 1);
}

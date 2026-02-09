const std = @import("std");
const kafka = @import("kafka");

const default_host = "127.0.0.1";
const default_port: u16 = 9092;
const api_version: i16 = 4;
const correlation_id: i32 = 4242;

fn isMultiSuiteEnabled() bool {
    return std.posix.getenv("SAMSA_MULTI_BROKER_REQUIRED") != null;
}

fn requireSingleSuite() !void {
    if (isMultiSuiteEnabled()) {
        return error.SkipZigTest;
    }
}

fn requireMultiSuite() !void {
    if (!isMultiSuiteEnabled()) {
        return error.SkipZigTest;
    }
}

fn requireSingleSuiteReady(allocator: std.mem.Allocator) !void {
    try requireSingleSuite();
    try waitForBrokerReady(allocator);
}

fn writeFrame(stream: std.net.Stream, payload: []const u8) !void {
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &len_buf, @as(i32, @intCast(payload.len)), .big);

    try stream.writeAll(&len_buf);
    try stream.writeAll(payload);
}

fn readFrame(allocator: std.mem.Allocator, stream: std.net.Stream) ![]u8 {
    var len_buf: [4]u8 = undefined;
    const result_header = try stream.readAtLeast(&len_buf, len_buf.len);
    if (result_header != len_buf.len) {
        return error.EndOfStream;
    }

    const len_i32 = std.mem.readInt(i32, &len_buf, .big);
    if (len_i32 < 0) {
        return error.InvalidLength;
    }
    const len: usize = @intCast(len_i32);

    const frame = try allocator.alloc(u8, len);
    errdefer allocator.free(frame);

    const result_body = try stream.readAtLeast(frame, len);
    if (result_body != len) {
        return error.EndOfStream;
    }

    return frame;
}

fn buildApiVersionsRequestPayload(buf: []u8) ![]const u8 {
    const codec = kafka.protocol.codec;
    const header = kafka.protocol.header;
    const api = kafka.generated.api_versions;

    var e = codec.Encoder.init(buf);

    const request_header = header.RequestHeaderV2{
        .api_key = @intFromEnum(api.api_key),
        .api_version = api_version,
        .correlation_id = correlation_id,
        .client_id = "samsa-it",
    };
    try request_header.encode(&e);

    const request = api.Request{
        .client_software_name = "samsa",
        .client_software_version = "0.1.0",
    };
    try request.encode(&e, api_version);

    return e.written();
}

fn requireIntegrationInfra() !void {
    if (std.posix.getenv("SAMSA_INTEGRATION_REQUIRED") != null) {
        return error.IntegrationInfraUnavailable;
    }

    return error.SkipZigTest;
}

fn runCommandWithStderr(allocator: std.mem.Allocator, timeout_seconds: u8, inherit_stderr: bool, argv: []const []const u8) !void {
    const timeout_str = try std.fmt.allocPrint(allocator, "{d}s", .{timeout_seconds});
    defer allocator.free(timeout_str);

    const timeout_arg = try std.fmt.allocPrint(allocator, "{d}s", .{@as(u32, timeout_seconds) + 5});
    defer allocator.free(timeout_arg);

    var cmd: std.ArrayList([]const u8) = .empty;
    defer cmd.deinit(allocator);

    try cmd.appendSlice(allocator, &.{ "timeout", "-k", timeout_arg, timeout_str });
    try cmd.appendSlice(allocator, argv);

    var proc = std.process.Child.init(cmd.items, allocator);
    proc.stdin_behavior = .Ignore;
    proc.stdout_behavior = .Ignore;
    proc.stderr_behavior = if (inherit_stderr) .Inherit else .Ignore;

    const term = proc.spawnAndWait() catch return requireIntegrationInfra();
    switch (term) {
        .Exited => |code| {
            if (code != 0) {
                return requireIntegrationInfra();
            }
        },
        else => {
            return;
        },
    }
}

fn runCommand(allocator: std.mem.Allocator, timeout_seconds: u8, argv: []const []const u8) !void {
    return runCommandWithStderr(allocator, timeout_seconds, true, argv);
}

fn runCommandQuiet(allocator: std.mem.Allocator, timeout_seconds: u8, argv: []const []const u8) !void {
    return runCommandWithStderr(allocator, timeout_seconds, false, argv);
}

fn waitForBrokerReady(allocator: std.mem.Allocator) !void {
    const args = [_][]const u8{
        "docker",
        "exec",
        "samsa-kafka-4-0-1",
        "/opt/kafka/bin/kafka-broker-api-versions.sh",
        "--bootstrap-server",
        "localhost:9092",
    };

    var attempt: u8 = 0;
    while (attempt < 10) : (attempt += 1) {
        runCommandQuiet(allocator, 3, &args) catch {
            std.Thread.sleep(2 * std.time.ns_per_s);
            continue;
        };

        return;
    }

    return requireIntegrationInfra();
}

fn requireMultiBrokerInfra() !void {
    if (std.posix.getenv("SAMSA_MULTI_BROKER_REQUIRED") != null) {
        return error.MultiBrokerInfraUnavailable;
    }

    return error.SkipZigTest;
}

fn waitForMultiBrokerReady(allocator: std.mem.Allocator) !void {
    const checks = [_]struct {
        container: []const u8,
        bootstrap: []const u8,
    }{
        .{ .container = "samsa-kafka-4-0-1-node1", .bootstrap = "localhost:19092" },
        .{ .container = "samsa-kafka-4-0-1-node2", .bootstrap = "localhost:19094" },
        .{ .container = "samsa-kafka-4-0-1-node3", .bootstrap = "localhost:19096" },
    };

    for (checks) |check| {
        const args = [_][]const u8{
            "docker",
            "exec",
            check.container,
            "/opt/kafka/bin/kafka-broker-api-versions.sh",
            "--bootstrap-server",
            check.bootstrap,
        };

        var ok = false;
        var attempt: u8 = 0;
        while (attempt < 10) : (attempt += 1) {
            runCommandQuiet(allocator, 3, &args) catch {
                std.Thread.sleep(2 * std.time.ns_per_s);
                continue;
            };

            ok = true;
            break;
        }

        if (!ok) {
            return requireMultiBrokerInfra();
        }
    }
}

fn makeTopicName(allocator: std.mem.Allocator, prefix: []const u8) ![]u8 {
    const nonce = std.crypto.random.int(u32);
    return std.fmt.allocPrint(allocator, "{s}-{d}-{d}", .{ prefix, std.time.milliTimestamp(), nonce });
}

fn ensureTopicCompression(allocator: std.mem.Allocator, topic: []const u8, compression: []const u8) !void {
    try waitForBrokerReady(allocator);

    const compression_cfg = try std.fmt.allocPrint(allocator, "compression.type={s}", .{compression});
    defer allocator.free(compression_cfg);

    const create_args = [_][]const u8{
        "docker",
        "exec",
        "samsa-kafka-4-0-1",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--partitions",
        "3",
        "--replication-factor",
        "1",
        "--config",
        "compression.type=uncompressed",
        "--config",
        compression_cfg,
    };

    try runCommand(allocator, 5, &create_args);

    const alter_args = [_][]const u8{
        "docker",
        "exec",
        "samsa-kafka-4-0-1",
        "/opt/kafka/bin/kafka-configs.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        topic,
        "--add-config",
        compression_cfg,
    };
    try runCommand(allocator, 5, &alter_args);
}

fn ensureTopicUncompressed(allocator: std.mem.Allocator, topic: []const u8) !void {
    return ensureTopicCompression(allocator, topic, "uncompressed");
}

fn ensureTopicGzip(allocator: std.mem.Allocator, topic: []const u8) !void {
    return ensureTopicCompression(allocator, topic, "gzip");
}

test "integration: ApiVersions TCP handshake" {
    const allocator = std.testing.allocator;
    try requireSingleSuiteReady(allocator);

    const codec = kafka.protocol.codec;
    const header = kafka.protocol.header;
    const api = kafka.generated.api_versions;

    const address = try std.net.Address.parseIp(default_host, default_port);
    var stream = std.net.tcpConnectToAddress(address) catch |err| switch (err) {
        error.ConnectionRefused, error.NetworkUnreachable, error.ConnectionTimedOut => return requireIntegrationInfra(),
        else => return err,
    };
    defer stream.close();

    var request_buf: [2048]u8 = undefined;
    const request_payload = try buildApiVersionsRequestPayload(&request_buf);
    try writeFrame(stream, request_payload);

    const frame = try readFrame(allocator, stream);
    defer allocator.free(frame);

    var d = codec.Decoder.init(frame);

    const response_header = try header.ResponseHeaderV0.decode(&d);
    try std.testing.expectEqual(correlation_id, response_header.correlation_id);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const response = try api.Response.decode(arena.allocator(), &d, api_version);

    try std.testing.expectEqual(@as(i16, 0), response.error_code);
    try std.testing.expect(response.api_keys.len > 0);

    var has_api_versions = false;
    for (response.api_keys) |k| {
        if (k.api_key == @as(i16, @intFromEnum(api.api_key))) {
            has_api_versions = true;
            break;
        }
    }

    try std.testing.expect(has_api_versions);
    try std.testing.expectEqual(@as(usize, 0), d.remaining());
}

test "integration: cluster refresh metadata" {
    const allocator = std.testing.allocator;
    try requireSingleSuiteReady(allocator);

    var c = kafka.cluster.cluster.Cluster.init(allocator, .{
        .bootstrap_host = default_host,
        .bootstrap_port = default_port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    });
    defer c.deinit();

    c.refreshMetadata() catch |err| switch (err) {
        error.ConnectionRefused,
        error.ConnectionReset,
        error.NetworkUnreachable,
        error.Timeout,
        error.MetadataUnavailable,
        error.Unexpected,
        error.NoBrokers,
        error.ProtocolError,
        => return requireIntegrationInfra(),
        else => return err,
    };

    try std.testing.expect(c.cache.brokers.count() > 0);
}

test "integration: cluster metadata and broker routing" {
    const allocator = std.testing.allocator;
    try requireSingleSuiteReady(allocator);

    var c = kafka.cluster.cluster.Cluster.init(allocator, .{
        .bootstrap_host = default_host,
        .bootstrap_port = default_port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    });
    defer c.deinit();

    c.refreshMetadata() catch |err| switch (err) {
        error.ConnectionRefused,
        error.ConnectionReset,
        error.NetworkUnreachable,
        error.Timeout,
        error.MetadataUnavailable,
        error.Unexpected,
        error.NoBrokers,
        error.ProtocolError,
        => return requireIntegrationInfra(),
        else => return err,
    };

    const s = c.statistics();
    try std.testing.expect(s.broker_count > 0);
    try std.testing.expect(s.topic_count >= 0);
    try std.testing.expect(s.metadata_age_ms >= 0);

    _ = c.refreshTopicMetadata("missing") catch {};
    try std.testing.expect(c.cache.brokers.count() > 0);

    _ = c.brokerForTopicPartition("missing", 0) catch {};
    const s2 = c.statistics();
    try std.testing.expect(s2.broker_count > 0);
}

test "integration: cluster brokers-only metadata refresh" {
    const allocator = std.testing.allocator;
    try requireSingleSuiteReady(allocator);

    var c = kafka.cluster.cluster.Cluster.init(allocator, .{
        .bootstrap_host = default_host,
        .bootstrap_port = default_port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    });
    defer c.deinit();

    c.refreshMetadata() catch |err| switch (err) {
        error.ConnectionRefused,
        error.ConnectionReset,
        error.NetworkUnreachable,
        error.Timeout,
        error.MetadataUnavailable,
        error.Unexpected,
        error.NoBrokers,
        error.ProtocolError,
        => return requireIntegrationInfra(),
        else => return err,
    };

    var topic_it = c.cache.leaders.iterator();
    const existing_topic = if (topic_it.next()) |entry| entry.key_ptr.* else return requireIntegrationInfra();
    const topic_copy = try allocator.dupe(u8, existing_topic);
    defer allocator.free(topic_copy);

    c.refreshBrokersOnlyMetadata() catch |err| switch (err) {
        error.ConnectionRefused,
        error.ConnectionReset,
        error.NetworkUnreachable,
        error.Timeout,
        error.MetadataUnavailable,
        error.Unexpected,
        error.NoBrokers,
        error.ProtocolError,
        => return requireIntegrationInfra(),
        else => return err,
    };

    try std.testing.expect(c.cache.brokers.count() > 0);
    try std.testing.expect(c.cache.leaders.get(topic_copy) != null);
}

test "integration: bootstrap_endpoints fallback reaches second endpoint" {
    const allocator = std.testing.allocator;
    try requireSingleSuiteReady(allocator);

    var endpoints = [_]kafka.cluster.cluster.Endpoint{
        .{ .host = default_host, .port = 1 },
        .{ .host = default_host, .port = default_port },
    };

    var c = kafka.cluster.cluster.Cluster.init(allocator, .{
        .bootstrap_endpoints = &endpoints,
        .connect_timeout_ms = 250,
        .request_timeout_ms = 2000,
    });
    defer c.deinit();

    c.refreshMetadata() catch |err| switch (err) {
        error.ConnectionRefused,
        error.ConnectionReset,
        error.NetworkUnreachable,
        error.Timeout,
        error.MetadataUnavailable,
        error.Unexpected,
        error.NoBrokers,
        error.ProtocolError,
        => return requireIntegrationInfra(),
        else => return err,
    };

    try std.testing.expect(c.cache.brokers.count() > 0);
}

test "integration: producer send and consumer poll roundtrip" {
    const allocator = std.testing.allocator;
    try requireSingleSuiteReady(allocator);

    var p = try kafka.client.Producer.init(allocator, .{
        .bootstrap_host = default_host,
        .bootstrap_port = default_port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    }, .{
        .request_ms = 1_000,
        .retries_max_attempts = 5,
    });
    defer p.deinit();

    const topic = try makeTopicName(allocator, "samsa-client-it");
    defer allocator.free(topic);

    try ensureTopicUncompressed(allocator, topic);

    std.Thread.sleep(1 * std.time.ns_per_s);
    const produced = p.send(topic, "k1", "v1") catch |err| return err;

    var c = try kafka.client.Consumer.init(allocator, .{
        .bootstrap_host = default_host,
        .bootstrap_port = default_port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    }, .{ .start_position = .earliest, .request_ms = 1_000 });
    defer c.deinit();

    try c.assign(topic, produced.partition);
    const recs = c.poll(3000) catch |err| return err;
    try std.testing.expect(recs.len > 0);
}

test "integration: producer acks none returns without response wait" {
    const allocator = std.testing.allocator;
    try requireSingleSuiteReady(allocator);

    var p = try kafka.client.Producer.init(allocator, .{
        .bootstrap_host = default_host,
        .bootstrap_port = default_port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    }, .{
        .acks = .none,
        .request_ms = 1_000,
        .retries_max_attempts = 1,
    });
    defer p.deinit();

    const topic = try makeTopicName(allocator, "samsa-client-it-acks0");
    defer allocator.free(topic);

    std.Thread.sleep(1 * std.time.ns_per_s);

    try ensureTopicUncompressed(allocator, topic);

    const result = try p.send(topic, "k", "v");
    try std.testing.expectEqual(@as(i64, -1), result.base_offset);
    try std.testing.expectEqual(@as(i64, -1), result.timestamp);

    const statistics = p.getStatistics();
    try std.testing.expectEqual(@as(u64, 1), statistics.produce_calls);
    try std.testing.expectEqual(@as(u64, 0), statistics.produce_errors);
}

test "integration: compressed topic reports unsupported_compression in recent errors" {
    const allocator = std.testing.allocator;
    try requireSingleSuiteReady(allocator);

    var p = try kafka.client.Producer.init(allocator, .{
        .bootstrap_host = default_host,
        .bootstrap_port = default_port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    }, .{
        .request_ms = 1_000,
        .retries_max_attempts = 5,
    });
    defer p.deinit();

    const topic = try makeTopicName(allocator, "samsa-client-it-gzip");
    defer allocator.free(topic);

    try ensureTopicGzip(allocator, topic);
    std.Thread.sleep(1 * std.time.ns_per_s);

    const produced = try p.send(topic, "k-gzip", "v-gzip");

    var c = try kafka.client.Consumer.init(allocator, .{
        .bootstrap_host = default_host,
        .bootstrap_port = default_port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    }, .{
        .start_position = .earliest,
        .request_ms = 1_000,
    });
    defer c.deinit();

    try c.assign(topic, produced.partition);

    var has_unsupported = false;
    var attempts: u8 = 0;
    while (attempts < 3 and !has_unsupported) : (attempts += 1) {
        _ = c.poll(2000) catch |err| return err;

        const recent = try c.getRecentPollErrors(allocator);
        defer allocator.free(recent);

        for (recent) |pe| {
            if (pe.local_kind) |kind| {
                if (kind == .unsupported_compression) {
                    has_unsupported = true;
                    break;
                }
            }
        }
    }

    try std.testing.expect(has_unsupported);
}

test "integration-multi: producer survives leader-node stop via metadata refresh" {
    const allocator = std.testing.allocator;
    try requireMultiSuite();
    try waitForMultiBrokerReady(allocator);

    var p = try kafka.client.Producer.init(allocator, .{
        .bootstrap_endpoints = &[_]kafka.cluster.cluster.Endpoint{
            .{
                .host = default_host,
                .port = default_port,
            },
            .{
                .host = default_host,
                .port = 9094,
            },
            .{
                .host = default_host,
                .port = 9096,
            },
        },
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 4000,
    }, .{
        .request_ms = 2000,
        .retries_max_attempts = 5,
    });
    defer p.deinit();

    const topic = try makeTopicName(allocator, "samsa-multi-failover");
    defer allocator.free(topic);

    const create_topic_args = [_][]const u8{
        "docker",
        "exec",
        "samsa-kafka-4-0-1-node1",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "kafka1:19092",
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--partitions",
        "3",
        "--replication-factor",
        "3",
        "--config",
        "compression.type=uncompressed",
    };
    runCommand(allocator, 8, &create_topic_args) catch return requireMultiBrokerInfra();

    _ = p.send(topic, "k-before", "v-before") catch return requireMultiBrokerInfra();

    const stop_node_args = [_][]const u8{ "docker", "stop", "samsa-kafka-4-0-1-node1" };
    runCommand(allocator, 10, &stop_node_args) catch return requireMultiBrokerInfra();

    defer {
        const start_node_args = [_][]const u8{ "docker", "start", "samsa-kafka-4-0-1-node1" };
        _ = runCommand(allocator, 15, &start_node_args) catch {};
    }

    std.Thread.sleep(3 * std.time.ns_per_s);

    _ = p.send(topic, "k-after", "v-after") catch return requireMultiBrokerInfra();
}

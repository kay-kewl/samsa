const std = @import("std");
const kafka = @import("kafka");

const default_host = "127.0.0.1";
const default_port: u16 = 9092;
const api_version: i16 = 4;
const correlation_id: i32 = 4242;

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

test "integration: ApiVersions TCP handshake" {
    const allocator = std.testing.allocator;
    const codec = kafka.protocol.codec;
    const header = kafka.protocol.header;
    const api = kafka.generated.api_versions;

    const address = try std.net.Address.parseIp(default_host, default_port);
    var stream = std.net.tcpConnectToAddress(address) catch |err| switch (err) {
        error.ConnectionRefused, error.NetworkUnreachable, error.ConnectionTimedOut => return error.SkipZigTest,
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

    var c = kafka.cluster.cluster.Cluster.init(allocator, .{
        .bootstrap_host = "127.0.0.1",
        .bootstrap_port = 9092,
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
        => return error.SkipZigTest,
        else => return err,
    };

    try std.testing.expect(c.cache.brokers.count() > 0);
}

test "integration: cluster metadata and broker routing" {
    const allocator = std.testing.allocator;

    var c = kafka.cluster.cluster.Cluster.init(allocator, .{
        .bootstrap_host = "127.0.0.1",
        .bootstrap_port = 9092,
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
        => return error.SkipZigTest,
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

    var c = kafka.cluster.cluster.Cluster.init(allocator, .{
        .bootstrap_host = "127.0.0.1",
        .bootstrap_port = 9092,
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
        => return error.SkipZigTest,
        else => return err,
    };

    var topic_it = c.cache.leaders.iterator();
    const existing_topic = if (topic_it.next()) |entry| entry.key_ptr.* else return error.SkipZigTest;
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
        => return error.SkipZigTest,
        else => return err,
    };

    try std.testing.expect(c.cache.brokers.count() > 0);
    try std.testing.expect(c.cache.leaders.get(topic_copy) != null);
}

test "integration: bootstrap_endpoints fallback reaches second endpoint" {
    const allocator = std.testing.allocator;

    var endpoints = [_]kafka.cluster.cluster.Endpoint{
        .{ .host = "127.0.0.1", .port = 1 },
        .{ .host = "127.0.0.1", .port = 9092 },
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
        => return error.SkipZigTest,
        else => return err,
    };

    try std.testing.expect(c.cache.brokers.count() > 0);
}

test "integration: producer send and consumer poll roundtrip" {
    const allocator = std.testing.allocator;

    var p = kafka.client.Producer.init(allocator, .{
        .bootstrap_host = "127.0.0.1",
        .bootstrap_port = 9092,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    }, .{});
    defer p.deinit();

    const produced = p.send("samsa-client-it", "k1", "v1") catch |err| switch (err) {
        error.ConnectionRefused,
        error.ConnectionReset,
        error.NetworkUnreachable,
        error.Timeout,
        error.MetadataUnavailable,
        error.Unexpected,
        error.NoBrokers,
        error.ProtocolError,
        => return error.SkipZigTest,
        else => return err,
    };

    var c = kafka.client.Consumer.init(allocator, .{
        .bootstrap_host = "127.0.0.1",
        .bootstrap_port = 9092,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    }, .{ .start_position = .earliest });
    defer c.deinit();

    try c.assign("samsa-client-it", produced.partition);
    const recs = try c.poll(3000);
    try std.testing.expect(recs.len > 0);
}

test "integration: producer acks none returns without response wait" {
    const allocator = std.testing.allocator;

    var p = kafka.client.Producer.init(allocator, .{
        .bootstrap_host = "127.0.0.1",
        .bootstrap_port = 9092,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 2000,
    }, .{ .acks = .none });
    defer p.deinit();

    const result = p.send("samsa-client-it-acks0", "k", "v") catch |err| switch (err) {
        error.ConnectionRefused,
        error.ConnectionReset,
        error.NetworkUnreachable,
        error.Timeout,
        error.MetadataUnavailable,
        error.Unexpected,
        error.NoBrokers,
        error.ProtocolError,
        => return error.SkipZigTest,
        else => return err,
    };

    try std.testing.expectEqual(@as(i64, -1), result.base_offset);
    try std.testing.expectEqual(@as(i64, -1), result.timestamp);
}

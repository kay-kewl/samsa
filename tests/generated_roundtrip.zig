const std = @import("std");
const kafka = @import("kafka");

fn roundtripRequest(comptime Api: type, request: Api.Request, version: i16) !void {
    const codec = kafka.protocol.codec;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var buf: [8192]u8 = undefined;
    var e = codec.Encoder.init(&buf);
    try request.encode(&e, version);

    var d = codec.Decoder.init(e.written());
    _ = try Api.Request.decode(arena.allocator(), &d, version);
    try std.testing.expectEqual(@as(usize, 0), d.remaining());
}

fn roundtripResponse(comptime Api: type, response: Api.Response, version: i16) !void {
    const codec = kafka.protocol.codec;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var buf: [8192]u8 = undefined;
    var e = codec.Encoder.init(&buf);
    try response.encode(&e, version);

    var d = codec.Decoder.init(e.written());
    _ = try Api.Response.decode(arena.allocator(), &d, version);
    try std.testing.expectEqual(@as(usize, 0), d.remaining());
}

test "ApiVersions request matrix: min/flex/max" {
    const api = kafka.generated.api_versions;

    try roundtripRequest(api, .{}, 0);
    try roundtripRequest(api, .{
        .client_software_name = "samsa",
        .client_software_version = "0.1.0",
    }, 3);
    try roundtripRequest(api, .{
        .client_software_name = "samsa",
        .client_software_version = "0.1.0",
    }, 4);
}

test "ApiVersions response matrix: min/flex/max" {
    const api = kafka.generated.api_versions;

    try roundtripResponse(api, .{}, 0);
    try roundtripResponse(api, .{}, 3);
    try roundtripResponse(api, .{}, 4);
}

test "Produce request matrix: min/flex/max" {
    const api = kafka.generated.produce;

    try roundtripRequest(api, .{
        .transactional_id = null,
        .acks = 1,
        .timeout_ms = 30000,
        .topic_data = &.{},
    }, 3);
    try roundtripRequest(api, .{
        .transactional_id = null,
        .acks = 1,
        .timeout_ms = 30000,
        .topic_data = &.{},
    }, 9);
    try roundtripRequest(api, .{
        .transactional_id = null,
        .acks = 1,
        .timeout_ms = 30000,
        .topic_data = &.{},
    }, 12);
}

test "Produce response matrix: min/flex/max" {
    const api = kafka.generated.produce;

    try roundtripResponse(api, .{}, 3);
    try roundtripResponse(api, .{}, 9);
    try roundtripResponse(api, .{}, 12);
}

test "Metadata request matrix: min/flex/max" {
    const api = kafka.generated.metadata;

    try roundtripRequest(api, .{
        .topics = &.{},
        .allow_auto_topic_creation = true,
        .include_cluster_authorized_operations = false,
        .include_topic_authorized_operations = false,
    }, 0);
    try roundtripRequest(api, .{
        .topics = &.{},
        .allow_auto_topic_creation = true,
        .include_cluster_authorized_operations = false,
        .include_topic_authorized_operations = false,
    }, 9);
    try roundtripRequest(api, .{
        .topics = &.{},
        .allow_auto_topic_creation = true,
        .include_cluster_authorized_operations = false,
        .include_topic_authorized_operations = false,
    }, 13);
}

test "Metadata response matrix: min/flex/max" {
    const api = kafka.generated.metadata;

    try roundtripResponse(api, .{}, 0);
    try roundtripResponse(api, .{}, 9);
    try roundtripResponse(api, .{}, 13);
}

test "Fetch request matrix: min/flex/max" {
    const api = kafka.generated.fetch;

    try roundtripRequest(api, .{}, 4);
    try roundtripRequest(api, .{}, 12);
    try roundtripRequest(api, .{}, 17);
}

test "Fetch response matrix: min/flex/max" {
    const api = kafka.generated.fetch;

    try roundtripResponse(api, .{}, 4);
    try roundtripResponse(api, .{}, 12);
    try roundtripResponse(api, .{}, 17);
}

test "ListOffsets request matrix: min/flex/max" {
    const api = kafka.generated.list_offsets;

    try roundtripRequest(api, .{}, 1);
    try roundtripRequest(api, .{}, 6);
    try roundtripRequest(api, .{}, 10);
}

test "ListOffsets response matrix: min/flex/max" {
    const api = kafka.generated.list_offsets;

    try roundtripResponse(api, .{}, 1);
    try roundtripResponse(api, .{}, 6);
    try roundtripResponse(api, .{}, 10);
}

test "Metadata topics null is rejected in v0, allowed in v9" {
    const codec = kafka.protocol.codec;
    const api = kafka.generated.metadata;

    var buf: [1024]u8 = undefined;
    var e = codec.Encoder.init(&buf);

    const invalid_v0 = api.Request{ .topics = null };
    try std.testing.expectError(error.InvalidNullForVersion, invalid_v0.encode(&e, 0));

    try roundtripRequest(api, .{ .topics = null }, 9);
}

test "Metadata v9 null topics encodes as compact nullable array = 0 marker" {
    const codec = kafka.protocol.codec;
    const api = kafka.generated.metadata;

    const request = api.Request{ .topics = null };

    var buf: [1024]u8 = undefined;
    var e = codec.Encoder.init(&buf);
    try request.encode(&e, 9);

    var d = codec.Decoder.init(e.written());
    try std.testing.expectEqual(@as(u32, 0), try d.readUVarint32());
}

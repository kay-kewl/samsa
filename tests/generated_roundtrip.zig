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

fn flexOrMin(min_v: i16, flex_min_v: i16, flex_max_v: i16) i16 {
    if (flex_min_v <= flex_max_v) {
        return flex_min_v;
    }

    return min_v;
}

test "ApiVersions request matrix: min/flex/max" {
    const api = kafka.generated.api_versions;

    try roundtripRequest(api, .{}, api.request_min_version);
    try roundtripRequest(api, .{
        .client_software_name = "samsa",
        .client_software_version = "0.1.0",
    }, flexOrMin(api.request_min_version, api.request_flexible_min_version, api.request_flexible_max_version));
    try roundtripRequest(api, .{
        .client_software_name = "samsa",
        .client_software_version = "0.1.0",
    }, api.request_max_version);
}

test "ApiVersions response matrix: min/flex/max" {
    const api = kafka.generated.api_versions;

    try roundtripResponse(api, .{}, api.response_min_version);
    try roundtripResponse(api, .{}, flexOrMin(api.response_min_version, api.response_flexible_min_version, api.response_flexible_max_version));
    try roundtripResponse(api, .{}, api.response_max_version);
}

test "Produce request matrix: min/flex/max" {
    const api = kafka.generated.produce;

    try roundtripRequest(api, .{
        .transactional_id = null,
        .acks = 1,
        .timeout_ms = 30000,
        .topic_data = &.{},
    }, api.request_min_version);
    try roundtripRequest(api, .{
        .transactional_id = null,
        .acks = 1,
        .timeout_ms = 30000,
        .topic_data = &.{},
    }, flexOrMin(api.request_min_version, api.request_flexible_min_version, api.request_flexible_max_version));
    try roundtripRequest(api, .{
        .transactional_id = null,
        .acks = 1,
        .timeout_ms = 30000,
        .topic_data = &.{},
    }, api.request_max_version);
}

test "Produce response matrix: min/flex/max" {
    const api = kafka.generated.produce;

    try roundtripResponse(api, .{}, api.response_min_version);
    try roundtripResponse(api, .{}, flexOrMin(api.response_min_version, api.response_flexible_min_version, api.response_flexible_max_version));
    try roundtripResponse(api, .{}, api.response_max_version);
}

test "Metadata request matrix: min/flex/max" {
    const api = kafka.generated.metadata;

    try roundtripRequest(api, .{
        .topics = &.{},
        .allow_auto_topic_creation = true,
        .include_cluster_authorized_operations = false,
        .include_topic_authorized_operations = false,
    }, api.request_min_version);
    try roundtripRequest(api, .{
        .topics = &.{},
        .allow_auto_topic_creation = true,
        .include_cluster_authorized_operations = false,
        .include_topic_authorized_operations = false,
    }, flexOrMin(api.request_min_version, api.request_flexible_min_version, api.request_flexible_max_version));
    try roundtripRequest(api, .{
        .topics = &.{},
        .allow_auto_topic_creation = true,
        .include_cluster_authorized_operations = false,
        .include_topic_authorized_operations = false,
    }, api.request_max_version);
}

test "Metadata response matrix: min/flex/max" {
    const api = kafka.generated.metadata;

    try roundtripResponse(api, .{}, api.response_min_version);
    try roundtripResponse(api, .{}, flexOrMin(api.response_min_version, api.response_flexible_min_version, api.response_flexible_max_version));
    try roundtripResponse(api, .{}, api.response_max_version);
}

test "Fetch request matrix: min/flex/max" {
    const api = kafka.generated.fetch;

    try roundtripRequest(api, .{}, api.request_min_version);
    try roundtripRequest(api, .{}, flexOrMin(api.request_min_version, api.request_flexible_min_version, api.request_flexible_max_version));
    try roundtripRequest(api, .{}, api.request_max_version);
}

test "Fetch response matrix: min/flex/max" {
    const api = kafka.generated.fetch;

    try roundtripResponse(api, .{}, api.response_min_version);
    try roundtripResponse(api, .{}, flexOrMin(api.response_min_version, api.response_flexible_min_version, api.response_flexible_max_version));
    try roundtripResponse(api, .{}, api.response_max_version);
}

test "ListOffsets request matrix: min/flex/max" {
    const api = kafka.generated.list_offsets;

    try roundtripRequest(api, .{}, api.request_min_version);
    try roundtripRequest(api, .{}, flexOrMin(api.request_min_version, api.request_flexible_min_version, api.request_flexible_max_version));
    try roundtripRequest(api, .{}, api.request_max_version);
}

test "ListOffsets response matrix: min/flex/max" {
    const api = kafka.generated.list_offsets;

    try roundtripResponse(api, .{}, api.response_min_version);
    try roundtripResponse(api, .{}, flexOrMin(api.response_min_version, api.response_flexible_min_version, api.response_flexible_max_version));
    try roundtripResponse(api, .{}, api.response_max_version);
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

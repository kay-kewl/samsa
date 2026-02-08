const std = @import("std");
const kafka = @import("kafka");
const fixtures = @import("protocol_fixtures.zig");

fn roundtripRequestFixture(comptime Api: type, version: i16, name: []const u8) !void {
    const allocator = std.testing.allocator;
    const bytes = try fixtures.requireFixture(allocator, name, 64 * 1024 * 1024);
    defer allocator.free(bytes);
    try fixtures.verifyFixtureDigest(allocator, name, bytes);

    var d = kafka.protocol.codec.Decoder.init(bytes);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const decoded = try Api.Request.decode(arena.allocator(), &d, version);
    try std.testing.expectEqual(@as(usize, 0), d.remaining());

    const out_buf = try allocator.alloc(u8, bytes.len + 4096);
    defer allocator.free(out_buf);

    var e = kafka.protocol.codec.Encoder.init(out_buf);
    try decoded.encode(&e, version);

    try fixtures.expectEqualBytes(bytes, e.written());
}

fn roundtripResponseFixture(comptime Api: type, version: i16, name: []const u8) !void {
    const allocator = std.testing.allocator;
    const bytes = try fixtures.requireFixture(allocator, name, 64 * 1024 * 1024);
    defer allocator.free(bytes);
    try fixtures.verifyFixtureDigest(allocator, name, bytes);

    var d = kafka.protocol.codec.Decoder.init(bytes);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const decoded = try Api.Response.decode(arena.allocator(), &d, version);
    try std.testing.expectEqual(@as(usize, 0), d.remaining());

    const out_buf = try allocator.alloc(u8, bytes.len + 4096);
    defer allocator.free(out_buf);

    var e = kafka.protocol.codec.Encoder.init(out_buf);
    try decoded.encode(&e, version);

    try fixtures.expectEqualBytes(bytes, e.written());
}

test "golden: ApiVersions v4 request" {
    try roundtripRequestFixture(kafka.generated.api_versions, 4, "api_api_versions_v4_request.bin");
}

test "golden: ApiVersions v4 response" {
    try roundtripResponseFixture(kafka.generated.api_versions, 4, "api_api_versions_v4_response.bin");
}

test "golden: ApiVersions v0 response decode fallback shape" {
    try roundtripResponseFixture(kafka.generated.api_versions, 0, "api_api_versions_v0_response.bin");
}

test "golden: Metadata v12 request and response" {
    try roundtripRequestFixture(kafka.generated.metadata, 12, "api_metadata_v12_request.bin");
    try roundtripResponseFixture(kafka.generated.metadata, 12, "api_metadata_v12_response.bin");
}

test "golden: Produce v12 request and response" {
    try roundtripRequestFixture(kafka.generated.produce, 12, "api_produce_v12_request.bin");
    try roundtripResponseFixture(kafka.generated.produce, 12, "api_produce_v12_response.bin");
}

test "golden: Fetch v12 request and response" {
    try roundtripRequestFixture(kafka.generated.fetch, 12, "api_fetch_v12_request.bin");
    try roundtripResponseFixture(kafka.generated.fetch, 12, "api_fetch_v12_response.bin");
}

test "golden: ListOffsets v10 request and response" {
    try roundtripRequestFixture(kafka.generated.list_offsets, 10, "api_list_offsets_v10_request.bin");
    try roundtripResponseFixture(kafka.generated.list_offsets, 10, "api_list_offsets_v10_response.bin");
}

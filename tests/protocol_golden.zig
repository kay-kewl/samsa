const std = @import("std");
const kafka = @import("kafka");
const fixtures = @import("protocol_fixtures.zig");

fn enforceRealGoldenChecks() bool {
    return std.posix.getenv("SAMSA_REQUIRE_REAL_GOLDEN") != null;
}

fn requireExactResponseGolden() bool {
    return std.posix.getenv("SAMSA_REQUIRE_EXACT_RESPONSE_GOLDEN") != null;
}

fn ensureFixtureLooksReal(name: []const u8, bytes: []const u8) !void {
    if (!enforceRealGoldenChecks()) {
        return;
    }

    if (std.mem.eql(u8, name, "api_metadata_v12_response.bin") and bytes.len < 64) {
        return error.FixtureLooksSynthetic;
    }

    if (std.mem.eql(u8, name, "api_produce_v12_response.bin") and bytes.len < 32) {
        return error.FixtureLooksSynthetic;
    }

    if (std.mem.eql(u8, name, "api_fetch_v12_response.bin") and bytes.len < 64) {
        return error.FixtureLooksSynthetic;
    }

    if (std.mem.eql(u8, name, "api_list_offsets_v10_response.bin") and bytes.len < 24) {
        return error.FixtureLooksSynthetic;
    }
}

fn isFlexible(version: i16, min: i16, max: i16) bool {
    return version >= min and version <= max;
}

fn appendRaw(e: *kafka.protocol.codec.Encoder, bytes: []const u8) !void {
    if (e.pos + bytes.len > e.buf.len) {
        return error.NoSpace;
    }

    @memcpy(e.buf[e.pos .. e.pos + bytes.len], bytes);
    e.pos += bytes.len;
}

fn roundtripRequestFixture(comptime Api: type, version: i16, name: []const u8) !void {
    const allocator = std.testing.allocator;
    const bytes = try fixtures.requireFixture(allocator, name, 64 * 1024 * 1024);
    defer allocator.free(bytes);
    try fixtures.verifyFixtureDigest(allocator, name, bytes);
    try ensureFixtureLooksReal(name, bytes);

    var d = kafka.protocol.codec.Decoder.init(bytes);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const frame_len = try d.readI32();
    try std.testing.expect(frame_len >= 0);
    try std.testing.expectEqual(@as(usize, @intCast(frame_len)), bytes.len - 4);

    const request_is_flexible = isFlexible(version, Api.request_flexible_min_version, Api.request_flexible_max_version);
    const request_header_version = kafka.protocol.header.requestHeaderVersion(request_is_flexible);

    const header_start = d.pos;
    const api_key = try d.readI16();
    const api_version = try d.readI16();
    _ = try d.readI32();
    _ = try d.readNullableString();

    switch (request_header_version) {
        .v1 => {},
        .v2 => try kafka.protocol.tagged_fields.skipAll(&d),
        .v0 => unreachable,
    }

    const header_bytes = bytes[header_start..d.pos];

    try std.testing.expectEqual(@as(i16, @intFromEnum(Api.api_key)), api_key);
    try std.testing.expectEqual(version, api_version);

    const decoded = try Api.Request.decode(arena.allocator(), &d, version);
    try std.testing.expectEqual(@as(usize, 0), d.remaining());

    const out_buf = try allocator.alloc(u8, bytes.len + 4096);
    defer allocator.free(out_buf);

    var e = kafka.protocol.codec.Encoder.init(out_buf);
    try e.writeI32(0);
    try appendRaw(&e, header_bytes);
    try decoded.encode(&e, version);

    const total_len = e.written().len - 4;
    std.mem.writeInt(i32, out_buf[0..4], @intCast(total_len), .big);

    try fixtures.expectEqualBytes(bytes, e.written());
}

fn roundtripResponseFixture(comptime Api: type, version: i16, name: []const u8) !void {
    const allocator = std.testing.allocator;
    const bytes = try fixtures.requireFixture(allocator, name, 64 * 1024 * 1024);
    defer allocator.free(bytes);
    try fixtures.verifyFixtureDigest(allocator, name, bytes);
    try ensureFixtureLooksReal(name, bytes);

    var d = kafka.protocol.codec.Decoder.init(bytes);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const frame_len = try d.readI32();
    try std.testing.expect(frame_len >= 0);
    try std.testing.expectEqual(@as(usize, @intCast(frame_len)), bytes.len - 4);

    const response_is_flexible = isFlexible(version, Api.response_flexible_min_version, Api.response_flexible_max_version);
    const response_header_version = kafka.protocol.header.responseHeaderVersion(Api.api_key, response_is_flexible);

    const header_start = d.pos;
    switch (response_header_version) {
        .v0 => _ = try kafka.protocol.header.ResponseHeaderV0.decode(&d),
        .v1, .v2 => _ = try kafka.protocol.header.ResponseHeaderV1.decode(&d),
    }

    const header_bytes = bytes[header_start..d.pos];

    const decoded = try Api.Response.decode(arena.allocator(), &d, version);
    try std.testing.expectEqual(@as(usize, 0), d.remaining());

    const out_buf = try allocator.alloc(u8, bytes.len + 4096);
    defer allocator.free(out_buf);

    var e = kafka.protocol.codec.Encoder.init(out_buf);
    try e.writeI32(0);
    try appendRaw(&e, header_bytes);
    try decoded.encode(&e, version);

    const reencoded = e.written();
    const total_len = reencoded.len - 4;
    std.mem.writeInt(i32, out_buf[0..4], @intCast(total_len), .big);

    var d2 = kafka.protocol.codec.Decoder.init(reencoded);
    const frame_len2 = try d2.readI32();
    try std.testing.expect(frame_len2 >= 0);
    try std.testing.expectEqual(@as(usize, @intCast(frame_len2)), reencoded.len - 4);

    switch (response_header_version) {
        .v0 => _ = try kafka.protocol.header.ResponseHeaderV0.decode(&d2),
        .v1, .v2 => _ = try kafka.protocol.header.ResponseHeaderV1.decode(&d2),
    }

    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();
    _ = try Api.Response.decode(arena2.allocator(), &d2, version);
    try std.testing.expectEqual(@as(usize, 0), d2.remaining());

    if (requireExactResponseGolden()) {
        try fixtures.expectEqualBytes(bytes, reencoded);
    }
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

test "golden: ApiVersions v2 uses classic client_id and flexible empty tags" {
    var buf: [128]u8 = undefined;
    var e = kafka.protocol.codec.Encoder.init(&buf);

    const h = kafka.protocol.header.RequestHeaderV2{
        .api_key = 18,
        .api_version = 4,
        .correlation_id = 42,
        .client_id = "samsa-golden",
    };
    try h.encode(&e);

    var d = kafka.protocol.codec.Decoder.init(e.written());
    try std.testing.expectEqual(@as(i16, 18), try d.readI16());
    try std.testing.expectEqual(@as(i16, 4), try d.readI16());
    try std.testing.expectEqual(@as(i32, 42), try d.readI32());

    const client_id = (try d.readNullableString()).?;
    try std.testing.expectEqualStrings("samsa-golden", client_id);

    try std.testing.expectEqual(@as(u32, 0), try d.readUVarint32());
    try std.testing.expectEqual(@as(usize, 0), d.remaining());
}

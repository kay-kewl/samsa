const std = @import("std");
const kafka = @import("kafka");

const codec = kafka.protocol.codec;
const generated = kafka.generated;

fn writeIfChanged(dir: std.fs.Dir, name: []const u8, bytes: []const u8, preserve_existing: bool, allocator: std.mem.Allocator) !void {
    if (preserve_existing) {
        const already_exists = dir.readFileAlloc(allocator, name, 64 * 1024 * 1024) catch null;
        if (already_exists) |prev| {
            allocator.free(prev);
            return;
        }
    }

    const old = dir.readFileAlloc(allocator, name, 64 * 1024 * 1024) catch null;
    if (old) |prev| {
        defer allocator.free(prev);
        if (std.mem.eql(u8, prev, bytes)) {
            return;
        }
    }

    var f = try dir.createFile(name, .{ .truncate = true });
    defer f.close();
    try f.writeAll(bytes);
}

fn writeFixtureManifest(out_dir: std.fs.Dir, allocator: std.mem.Allocator) !void {
    const fixture_names = [_][]const u8{
        "api_api_versions_v0_request.bin",
        "api_api_versions_v0_response.bin",
        "api_api_versions_v4_request.bin",
        "api_api_versions_v4_response.bin",
        "api_fetch_v12_request.bin",
        "api_fetch_v12_response.bin",
        "api_list_offsets_v10_request.bin",
        "api_list_offsets_v10_response.bin",
        "api_metadata_v12_request.bin",
        "api_metadata_v12_response.bin",
        "api_produce_v12_request.bin",
        "api_produce_v12_response.bin",
    };

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);

    try out.appendSlice(allocator, "{\n    \"fixtures\": [\n");

    for (fixture_names, 0..) |name, i| {
        const bytes = try out_dir.readFileAlloc(allocator, name, 1024 * 1024);
        defer allocator.free(bytes);

        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const hex = std.fmt.bytesToHex(digest, .lower);

        const comma = if (i + 1 < fixture_names.len) "," else "";
        try out.writer(allocator).print("        {{\"name\":\"{s}\",\"sha256\":\"{s}\"}}{s}\n", .{ name, hex[0..], comma });
    }

    try out.appendSlice(allocator, "    ]\n}\n");
    try writeIfChanged(out_dir, "manifest.json", out.items, false, allocator);
}

fn emitRequest(comptime Api: type, request: Api.Request, version: i16, dir: std.fs.Dir, name: []const u8, preserve_existing: bool, allocator: std.mem.Allocator) !void {
    var buf: [256 * 1024]u8 = undefined;
    var e = codec.Encoder.init(&buf);
    try request.encode(&e, version);
    try writeIfChanged(dir, name, e.written(), preserve_existing, allocator);
}

fn emitResponse(comptime Api: type, response: Api.Response, version: i16, dir: std.fs.Dir, name: []const u8, preserve_existing: bool, allocator: std.mem.Allocator) !void {
    var buf: [256 * 1024]u8 = undefined;
    var e = codec.Encoder.init(&buf);
    try response.encode(&e, version);
    try writeIfChanged(dir, name, e.written(), preserve_existing, allocator);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const out_path = "tests/protocol/fixtures";
    try std.fs.cwd().makePath(out_path);
    var out_dir = try std.fs.cwd().openDir(out_path, .{});
    defer out_dir.close();

    try emitRequest(generated.api_versions, .{
        .client_software_name = "samsa",
        .client_software_version = "0.1.0",
    }, 4, out_dir, "api_api_versions_v4_request.bin", true, allocator);

    try emitResponse(generated.api_versions, .{
        .error_code = 0,
        .api_keys = &.{
            .{
                .api_key = 18,
                .min_version = 2,
                .max_version = 4,
            },
        },
    }, 4, out_dir, "api_api_versions_v4_response.bin", true, allocator);

    try emitResponse(generated.api_versions, .{
        .error_code = 35,
        .api_keys = &.{
            .{
                .api_key = 18,
                .min_version = 2,
                .max_version = 4,
            },
        },
    }, 0, out_dir, "api_api_versions_v0_response.bin", true, allocator);

    try emitRequest(generated.metadata, .{}, 12, out_dir, "api_metadata_v12_request.bin", true, allocator);
    try emitResponse(generated.metadata, .{}, 12, out_dir, "api_metadata_v12_response.bin", true, allocator);

    try emitRequest(generated.produce, .{}, 12, out_dir, "api_produce_v12_request.bin", true, allocator);
    try emitResponse(generated.produce, .{}, 12, out_dir, "api_produce_v12_response.bin", true, allocator);

    try emitRequest(generated.fetch, .{}, 12, out_dir, "api_fetch_v12_request.bin", true, allocator);
    try emitResponse(generated.fetch, .{}, 12, out_dir, "api_fetch_v12_response.bin", true, allocator);

    try emitRequest(generated.list_offsets, .{}, 10, out_dir, "api_list_offsets_v10_request.bin", true, allocator);
    try emitResponse(generated.list_offsets, .{}, 10, out_dir, "api_list_offsets_v10_response.bin", true, allocator);

    try writeFixtureManifest(out_dir, allocator);
}

const std = @import("std");
const kafka = @import("kafka");

const codec = kafka.protocol.codec;
const generated = kafka.generated;

fn writeIfChanged(dir: std.fs.Dir, allocator: std.mem.Allocator, name: []const u8, bytes: []const u8) !void {
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

fn emitRequest(comptime Api: type, request: Api.Request, version: i16, name: []const u8, dir: std.fs.Dir, allocator: std.mem.Allocator) !void {
    var buf: [256 * 1024]u8 = undefined;
    var e = codec.Encoder.init(&buf);
    try request.encode(&e, version);
    try writeIfChanged(dir, allocator, name, e.written());
}

fn emitResponse(comptime Api: type, response: Api.Response, version: i16, name: []const u8, dir: std.fs.Dir, allocator: std.mem.Allocator) !void {
    var buf: [256 * 1024]u8 = undefined;
    var e = codec.Encoder.init(&buf);
    try response.encode(&e, version);
    try writeIfChanged(dir, allocator, name, e.written());
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
    }, 4, "api_api_versions_v4_request.bin", out_dir, allocator);

    try emitResponse(generated.api_versions, .{
        .error_code = 0,
        .api_keys = &.{
            .{
                .api_key = 18,
                .min_version = 2,
                .max_version = 4,
            },
        },
    }, 4, "api_api_versions_v4_response.bin", out_dir, allocator);

    try emitResponse(generated.api_versions, .{
        .error_code = 35,
        .api_keys = &.{
            .{
                .api_key = 18,
                .min_version = 2,
                .max_version = 4,
            },
        },
    }, 0, "api_api_versions_v0_response.bin", out_dir, allocator);

    try emitRequest(generated.metadata, .{}, 12, "api_metadata_v12_request.bin", out_dir, allocator);
    try emitResponse(generated.metadata, .{}, 12, "api_metadata_v12_response.bin", out_dir, allocator);

    try emitRequest(generated.produce, .{}, 12, "api_produce_v12_request.bin", out_dir, allocator);
    try emitResponse(generated.produce, .{}, 12, "api_produce_v12_response.bin", out_dir, allocator);

    try emitRequest(generated.fetch, .{}, 12, "api_fetch_v12_request.bin", out_dir, allocator);
    try emitResponse(generated.fetch, .{}, 12, "api_fetch_v12_response.bin", out_dir, allocator);

    try emitRequest(generated.list_offsets, .{}, 10, "api_list_offsets_v10_request.bin", out_dir, allocator);
    try emitResponse(generated.list_offsets, .{}, 10, "api_list_offsets_v10_response.bin", out_dir, allocator);

    const manifest =
        \\{
        \\    "fixtures": []
        \\}
    ;
    try writeIfChanged(out_dir, allocator, "manifest.json", manifest);
}

const std = @import("std");

const kafka_tag = "4.0.1";
const base_url = "https://raw.githubusercontent.com/apache/kafka/" ++ kafka_tag ++ "/clients/src/main/resources/common/message";
const profile_dir_path = "kafka-profile";
const max_schema_bytes = 1024 * 1024;

const files = [_][]const u8{
    "ApiVersionsRequest.json",
    "ApiVersionsResponse.json",
    "MetadataRequest.json",
    "MetadataResponse.json",
    "ProduceRequest.json",
    "ProduceResponse.json",
    "FetchRequest.json",
    "FetchResponse.json",
    "ListOffsetsRequest.json",
    "ListOffsetsResponse.json",
};

const ProfileManifest = struct {
    files: []const struct {
        name: []const u8,
        sha256: []const u8,
    },
};

fn expectedSha256(manifest: ProfileManifest, name: []const u8) ?[]const u8 {
    for (manifest.files) |entry| {
        if (std.mem.eql(u8, entry.name, name)) {
            return entry.sha256;
        }
    }

    return null;
}

fn sha256Hex(bytes: []const u8) [64]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}

fn writeFileIfChanged(dir: std.Io.Dir, io: std.Io, name: []const u8, bytes: []const u8, allocator: std.mem.Allocator) !void {
    const old = dir.readFileAlloc(io, name, allocator, .limited(max_schema_bytes)) catch null;
    if (old) |prev| {
        defer allocator.free(prev);
        if (std.mem.eql(u8, prev, bytes)) {
            return;
        }
    }

    try dir.writeFile(io, .{
        .sub_path = name,
        .data = bytes,
        .flags = .{ .truncate = true },
    });
}

fn fetchUrl(allocator: std.mem.Allocator, url: []const u8) ![]u8 {
    var client = std.http.Client{ .allocator = allocator };
    defer client.deinit();

    const uri = try std.Uri.parse(url);

    var body = std.Io.Writer.Allocating.init(allocator);
    defer body.deinit();

    var redirect_buffer: [4096]u8 = undefined;

    const result = try client.fetch(.{
        .location = .{ .uri = uri },
        .method = .GET,
        .redirect_buffer = &redirect_buffer,
        .response_writer = &body.writer,
    });

    if (result.status != .ok) {
        std.debug.print("HTTP request failed: {s}, status={any}\n", .{ url, result.status });
        return error.HttpRequestFailed;
    }

    const downloaded = body.written();
    if (downloaded.len == 0 or downloaded.len > max_schema_bytes) {
        return error.InvalidDownloadedSize;
    }

    return try allocator.dupe(u8, downloaded);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    try std.Io.Dir.cwd().createDirPath(init.io, profile_dir_path);

    var profile_dir = try std.Io.Dir.cwd().openDir(init.io, profile_dir_path, .{});
    defer profile_dir.close(init.io);

    const manifest_bytes = try profile_dir.readFileAlloc(init.io, "manifest.json", allocator, .limited(max_schema_bytes));
    defer allocator.free(manifest_bytes);

    const parsed = try std.json.parseFromSlice(ProfileManifest, allocator, manifest_bytes, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer parsed.deinit();

    for (files) |file_name| {
        const expected = expectedSha256(parsed.value, file_name) orelse {
            std.debug.print("No checksum in manifest for {s}\n", .{file_name});
            return error.MissingManifestEntry;
        };

        const url = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ base_url, file_name });
        defer allocator.free(url);

        std.debug.print("Fetching {s}...\n", .{file_name});

        const bytes = try fetchUrl(allocator, url);
        defer allocator.free(bytes);

        const actual_hex = sha256Hex(bytes);
        if (!std.mem.eql(u8, expected, actual_hex[0..])) {
            std.debug.print(
                "Checksum mismatch for {s}: expected {s}, got {s}\n",
                .{ file_name, expected, actual_hex[0..] },
            );
            return error.SchemaDigestMismatch;
        }

        try writeFileIfChanged(profile_dir, init.io, file_name, bytes, allocator);
    }

    std.debug.print("All schemas fetched successfully. Proceed with: zig build gen\n", .{});
}

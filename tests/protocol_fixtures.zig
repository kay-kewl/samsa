const std = @import("std");

pub const FixtureDigest = struct {
    name: []const u8,
    sha256: []const u8,
};

pub const FixtureManifest = struct {
    fixtures: []const FixtureDigest,
};

pub fn fixturePath(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "tests/protocol/fixtures/{s}", .{name});
}

pub fn maybeReadFixture(allocator: std.mem.Allocator, name: []const u8, max_bytes: usize) !?[]u8 {
    const path = try fixturePath(allocator, name);
    defer allocator.free(path);

    return std.fs.cwd().readFileAlloc(allocator, path, max_bytes) catch |err| switch (err) {
        error.FileNotFound => null,
        else => err,
    };
}

pub fn requireFixture(allocator: std.mem.Allocator, name: []const u8, max_bytes: usize) ![]u8 {
    const maybe = try maybeReadFixture(allocator, name, max_bytes);
    if (maybe) |bytes| {
        return bytes;
    }

    _ = std.fs.cwd().makePath("tests/protocol/fixtures") catch {};
    if (std.posix.getenv("SAMSA_REQUIRE_GOLDEN") != null) {
        return error.GoldenFixtureMissing;
    }

    return error.SkipZigTest;
}

pub fn expectEqualBytes(expected: []const u8, actual: []const u8) !void {
    if (std.mem.eql(u8, expected, actual)) {
        return;
    }

    const min_len = @min(expected.len, actual.len);
    var index: usize = 0;
    while (index < min_len) : (index += 1) {
        if (expected[index] != actual[index]) {
            break;
        }
    }

    std.debug.print("bytes mismatch at index={d}, expected_len={d}, actual_len={d}\n", .{
        index, expected.len, actual.len,
    });

    return error.TestExpectedEqual;
}

pub fn maybeReadManifest(allocator: std.mem.Allocator) !?std.json.Parsed(FixtureManifest) {
    const path = "tests/protocol/fixtures/manifest.json";
    const bytes = std.fs.cwd().readFileAlloc(allocator, path, 1024 * 1024) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer allocator.free(bytes);

    return try std.json.parseFromSlice(FixtureManifest, allocator, bytes, .{
        .ignore_unknown_fields = true,
    });
}

pub fn verifyFixtureDigest(allocator: std.mem.Allocator, fixture_name: []const u8, bytes: []const u8) !void {
    const maybe_manifest = try maybeReadManifest(allocator);
    if (maybe_manifest) |manifest_parsed| {
        defer manifest_parsed.deinit();

        for (manifest_parsed.value.fixtures) |entry| {
            if (std.mem.eql(u8, entry.name, fixture_name)) {
                var digest: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});

                const hex = std.fmt.bytesToHex(digest, .lower);

                if (!std.mem.eql(u8, entry.sha256, hex[0..])) {
                    return error.FixtureDigestMismatch;
                }

                return;
            }
        }
    }
}

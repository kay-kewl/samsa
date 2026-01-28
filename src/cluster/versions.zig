const std = @import("std");
const types = @import("../protocol/types.zig");
const api_versions = @import("../generated/api_versions.zig");
const errors = @import("errors.zig");

pub const Range = struct {
    min: i16,
    max: i16,
};

pub const Registry = struct {
    by_api_key: std.AutoHashMap(i16, Range),

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{
            .by_api_key = std.AutoHashMap(i16, Range).init(allocator),
        };
    }

    pub fn deinit(self: *Registry) void {
        self.by_api_key.deinit();
    }

    pub fn reset(self: *Registry) void {
        self.by_api_key.clearRetainingCapacity();
    }

    pub fn has(self: *const Registry, api_key: types.ApiKey) bool {
        return self.by_api_key.contains(@intFromEnum(api_key));
    }

    pub fn updateFromApiVersions(self: *Registry, response: api_versions.Response) !void {
        self.by_api_key.clearRetainingCapacity();
        for (response.api_keys) |k| {
            try self.by_api_key.put(k.api_key, .{
                .min = k.min_version,
                .max = k.max_version,
            });
        }
    }

    fn preferredVersions(api_key: types.ApiKey) []const i16 {
        return switch (api_key) {
            .ApiVersions => &[_]i16{ 4, 2 },
            .Metadata => &[_]i16{12},
            .Produce => &[_]i16{12},
            .Fetch => &[_]i16{12},
            .ListOffsets => &[_]i16{10},
            else => &[_]i16{},
        };
    }

    pub fn choose(self: *const Registry, api_key: types.ApiKey) errors.ClusterError!i16 {
        const range = self.by_api_key.get(@intFromEnum(api_key)) orelse return error.VersionNotNegotiated;

        for (preferredVersions(api_key)) |version| {
            if (version >= range.min and version <= range.max) {
                return version;
            }
        }

        return error.NoSupportedVersion;
    }
};

const testing = std.testing;

test "versions choose clamps and missing key fails" {
    var registry = Registry.init(testing.allocator);
    defer registry.deinit();

    try registry.by_api_key.put(@intFromEnum(types.ApiKey.Metadata), .{ .min = 4, .max = 12 });
    try testing.expectEqual(@as(i16, 12), try registry.choose(.Metadata));

    try registry.by_api_key.put(@intFromEnum(types.ApiKey.Metadata), .{ .min = 4, .max = 10 });
    try testing.expectError(error.NoSupportedVersion, registry.choose(.Metadata));

    try testing.expectError(error.VersionNotNegotiated, registry.choose(.Fetch));
}

const std = @import("std");
const kafka = @import("kafka");

test "cluster versions registry behavior" {
    var registry = kafka.cluster.versions.Registry.init(std.testing.allocator);
    defer registry.deinit();

    try registry.by_api_key.put(@intFromEnum(kafka.protocol.types.ApiKey.Metadata), .{ .min = 4, .max = 10 });
    try std.testing.expectEqual(@as(i16, 4), try registry.choose(.Metadata, 1));
    try std.testing.expectEqual(@as(i16, 10), try registry.choose(.Metadata, 100));
    try std.testing.expectError(error.VersionNotNegotiated, registry.choose(.Fetch, 1));
}

test "metadata cache apply skips bad broker ports" {
    var cache = kafka.cluster.metadata_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();

    const response = kafka.generated.metadata.Response{
        .brokers = &.{
            .{
                .node_id = 1,
                .host = "a",
                .port = 9092,
            },
            .{
                .node_id = 2,
                .host = "b",
                .port = -1,
            },
        },
        .topics = &.{},
    };

    try cache.apply(response);
    try std.testing.expectEqual(@as(usize, 1), cache.brokers.count());
}

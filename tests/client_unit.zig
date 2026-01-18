const std = @import("std");
const kafka = @import("kafka");

test "producer init validates frame/request relationship" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(error.InvalidConfiguration, kafka.client.Producer.init(allocator, .{
        .max_frame_bytes = 1024,
    }, .{
        .max_request_bytes = 2048,
    }));
}

test "consumer init partition fetch vs fetch_max_bytes" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(error.InvalidConfiguration, kafka.client.Consumer.init(allocator, .{
        .max_frame_bytes = 1024 * 1024,
    }, .{
        .fetch_max_bytes = 1000,
        .max_partition_fetch_bytes = 2000,
    }));
}

test "takeRecentPollErrors returns and clears" {
    const allocator = std.testing.allocator;
    var c = try kafka.client.Consumer.init(allocator, .{}, .{});
    defer c.deinit();

    try c.recent_errors.append(allocator, .{
        .topic = "events",
        .partition = 3,
        .error_code = 6,
        .error_message = "NOT_LEADER_OR_FOLLOWER",
    });

    const taken = try c.takeRecentPollErrors(allocator);
    defer allocator.free(taken);

    try std.testing.expectEqual(@as(usize, 1), taken.len);
    try std.testing.expectEqual(@as(usize, 0), c.getRecentPollErrors().len);
}

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

test "cluster config validate rejects empty identity fields" {
    try std.testing.expectError(error.InvalidConfiguration, (kafka.client.ClusterConfig{
        .client_id = "",
    }).validate());

    try std.testing.expectError(error.InvalidConfiguration, (kafka.client.ClusterConfig{
        .client_software_name = "",
    }).validate());

    try std.testing.expectError(error.InvalidConfiguration, (kafka.client.ClusterConfig{
        .client_software_version = "",
    }).validate());
}

test "cluster config validate rejects bad bootstrap endpoint entries" {
    var endpoints = [_]kafka.cluster.cluster.Endpoint{
        .{ .host = "", .port = 9092 },
    };

    try std.testing.expectError(error.InvalidConfiguration, (kafka.client.ClusterConfig{
        .bootstrap_endpoints = &endpoints,
    }).validate());
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
    try std.testing.expectEqual(@as(usize, 0), c.peekRecentPollErrors().len);
}

test "freeOwnedRecords deep-frees topic, key, value, and headers" {
    const allocator = std.testing.allocator;

    var records = try allocator.alloc(kafka.client.OwnedRecord, 1);

    records[0] = .{
        .topic = try allocator.dupe(u8, "events"),
        .partition = 0,
        .offset = 1,
        .timestamp = 2,
        .key = try allocator.dupe(u8, "k"),
        .value = try allocator.dupe(u8, "v"),
        .headers = try allocator.alloc(kafka.client.RecordHeader, 1),
    };

    records[0].headers[0] = .{
        .key = try allocator.dupe(u8, "h"),
        .value = try allocator.dupe(u8, "hv"),
    };

    kafka.client.freeOwnedRecords(allocator, records);
}

test "consumer init validates cluster config" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(error.InvalidConfiguration, kafka.client.Consumer.init(allocator, .{
        .max_frame_bytes = 0,
    }, .{}));

    var c = try kafka.client.Client.init(allocator, .{
        .connect_timeout_ms = 50,
        .request_timeout_ms = 50,
    });
    defer c.deinit();
}

test "producer and consumer expose getMetrics alias" {
    const allocator = std.testing.allocator;

    var p = try kafka.client.Producer.init(allocator, .{
        .request_timeout_ms = 50,
        .connect_timeout_ms = 50,
    }, .{});
    defer p.deinit();

    var c = try kafka.client.Consumer.init(allocator, .{
        .request_timeout_ms = 50,
        .connect_timeout_ms = 50,
    }, .{});
    defer c.deinit();

    _ = p.getMetrics();
    _ = c.getMetrics();
}

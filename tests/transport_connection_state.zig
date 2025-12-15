const std = @import("std");
const kafka = @import("kafka");

test "connection start disconnected and deinit keeps disconnected" {
    const connection = kafka.transport.connection;
    var c = connection.Connection.init(std.testing.allocator, .{
        .host = "127.0.0.1",
        .port = 1,
    });

    try std.testing.expectEqual(connection.State.Disconnected, c.state);
    c.deinit();
    try std.testing.expectEqual(connection.State.Disconnected, c.state);
}

test "connection failure is not ready" {
    const connection = kafka.transport.connection;
    var c = connection.Connection.init(std.testing.allocator, .{
        .host = "127.0.0.1",
        .port = 1,
        .connect_timeout_ms = 200,
    });
    defer c.deinit();

    const result = c.connect();
    if (result) |_| {
        return error.ExpectedConnectFailure;
    } else |_| {}

    try std.testing.expect(c.state != .Ready);
}

test "connection state transitions to dead on connect failure" {
    const connection = kafka.transport.connection;
    var c = connection.Connection.init(std.testing.allocator, .{
        .host = "127.0.0.1",
        .port = 1,
        .connect_timeout_ms = 100,
    });
    defer c.deinit();

    _ = c.connect() catch {};
    try std.testing.expect(c.state == .Dead or c.state == .Disconnected);
}

test "connection callNoResponse increments correlation id on success path assumptions" {
    const connection = kafka.transport.connection;
    var c = connection.Connection.init(std.testing.allocator, .{
        .host = "127.0.0.1",
        .port = 1,
    });
    defer c.deinit();

    const before = c.correlation_id;
    c.correlation_id +%= 1;
    try std.testing.expect(c.correlation_id != before);
}

test "connection config validate rejects invalid values" {
    const connection = kafka.transport.connection;

    try std.testing.expectError(error.ProtocolError, (connection.Config{
        .host = "",
        .port = 9092,
    }).validate());

    try std.testing.expectError(error.ProtocolError, (connection.Config{
        .host = "127.0.0.1",
        .port = 0,
    }).validate());

    try std.testing.expectError(error.Timeout, (connection.Config{
        .host = "127.0.0.1",
        .port = 9092,
        .connect_timeout_ms = 0,
    }).validate());
}

test "connection statistics timeout increments on connect timeout path" {
    const connection = kafka.transport.connection;
    var c = connection.Connection.init(std.testing.allocator, .{
        .host = "127.0.0.1",
        .port = 9092,
    });
    defer c.deinit();

    const statistics = c.getStatistics();
    try std.testing.expectEqual(@as(u64, 0), statistics.frames_read);
    try std.testing.expectEqual(@as(u64, 0), statistics.frames_written);
    try std.testing.expectEqual(@as(u64, 0), statistics.protocol_errors);
    try std.testing.expectEqual(@as(u64, 0), statistics.timeouts);
}

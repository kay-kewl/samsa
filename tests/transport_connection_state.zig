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

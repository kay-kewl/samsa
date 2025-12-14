const std = @import("std");
const kafka = @import("kafka");

test "transport module loads" {
    _ = kafka.transport.errors;
    _ = kafka.transport.framing;
    _ = kafka.transport.connection;
    _ = kafka.transport.pool;
    try std.testing.expect(true);
}

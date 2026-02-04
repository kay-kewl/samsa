const std = @import("std");
const kafka = @import("kafka");

test "fake-broker: ApiVersions v0 fallback body shape remains strict" {
    var c = kafka.transport.connection.Connection.init(std.testing.allocator, .{
        .host = "127.0.0.1",
        .port = 1,
    });
    defer c.deinit();

    const bad = [_]u8{
        0x00, 0x00,
        0x00, 0x2A,
        0x00, 0x23,
    };

    try std.testing.expectError(error.ProtocolError, c.decodeApiVersionsBodyWtihFallback(&bad, 4));
}

test "fake-broker: response header parse rejects truncated frame" {
    var d = kafka.protocol.codec.Decoder.init(&[_]u8{ 0x00, 0x00, 0x00 });
    try std.testing.expectError(error.EndOfStream, kafka.protocol.header.ResponseHeaderV0.decode(&d));
}

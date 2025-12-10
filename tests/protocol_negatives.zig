const std = @import("std");
const kafka = @import("kafka");

test "generated decode rejects duplicate tagged fields" {
    const codec = kafka.protocol.codec;
    const api = kafka.generated.api_versions;

    var buf: [128]u8 = undefined;
    var e = codec.Encoder.init(&buf);

    try e.writeCompactString("samsa");
    try e.writeCompactString("0.1.0");

    try e.writeUVarint32(2);
    try e.writeUVarint32(5);
    try e.writeUVarint32(0);
    try e.writeUVarint32(5);
    try e.writeUVarint32(0);

    var d = codec.Decoder.init(e.written());
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    try std.testing.expectError(error.DuplicateTag, api.Request.decode(arena.allocator(), &d, 4));
}

test "generated decode rejects out-of-order tagged fields" {
    const codec = kafka.protocol.codec;
    const api = kafka.generated.api_versions;

    var buf: [128]u8 = undefined;
    var e = codec.Encoder.init(&buf);

    try e.writeCompactString("samsa");
    try e.writeCompactString("0.1.0");

    try e.writeUVarint32(2);
    try e.writeUVarint32(7);
    try e.writeUVarint32(0);
    try e.writeUVarint32(3);
    try e.writeUVarint32(0);

    var d = codec.Decoder.init(e.written());
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    try std.testing.expectError(error.InvalidTagOrder, api.Request.decode(arena.allocator(), &d, 4));
}

test "ApiVersions v0 body fixture decodes and simulates fallback path" {
    const codec = kafka.protocol.codec;
    const api = kafka.generated.api_versions;

    const v0_body = [_]u8{
        0x00, 0x23,
        0x00, 0x00,
        0x00, 0x01,
        0x00, 0x12,
        0x00, 0x02,
        0x00, 0x04,
    };

    {
        var d4 = codec.Decoder.init(&v0_body);
        var arena4 = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena4.deinit();

        const decode_as_v4 = api.Response.decode(arena4.allocator(), &d4, 4);
        if (decode_as_v4) |_| {
            return error.ExpectedDecodeFailure;
        } else |err| switch (err) {
            error.InvalidLength, error.EndOfStream => {},
            else => return err,
        }
    }

    {
        var d0 = codec.Decoder.init(&v0_body);
        var arena0 = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena0.deinit();

        const response = try api.Response.decode(arena0.allocator(), &d0, 0);
        try std.testing.expectEqual(@as(i16, 35), response.error_code);
        try std.testing.expectEqual(@as(usize, 1), response.api_keys.len);
        try std.testing.expectEqual(@as(i16, 18), response.api_keys[0].api_key);
        try std.testing.expectEqual(@as(usize, 0), d0.remaining());
    }
}

test "protocol truncation of ApiVersions v0 fixture always fails" {
    const codec = kafka.protocol.codec;
    const api = kafka.generated.api_versions;

    const full = [_]u8{
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x01,
        0x00, 0x12,
        0x00, 0x02,
        0x00, 0x04,
    };

    var end: usize = 0;
    while (end < full.len) : (end += 1) {
        var d = codec.Decoder.init(full[0..end]);
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const response = api.Response.decode(arena.allocator(), &d, 0);
        if (response) |_| {
            return error.ExpectedDecodeFailure;
        } else |err| switch (err) {
            error.EndOfStream, error.InvalidLength, error.Overflow, error.InvalidVariant => {},
            else => return err,
        }
    }
}

const std = @import("std");

pub fn calculate(data: []const u8) u32 {
    return std.hash.crc.Crc32Iscsi.hash(data);
}

const testing = std.testing;

test "Castagnoli CRC32C test vectors" {
    // https://crccalc.com/?crc=123456789&method=CRC-32/ISCSI&datatype=ascii&outtype=hex
    try testing.expectEqual(@as(u32, 0xE3069283), calculate("123456789"));
    try testing.expectEqual(@as(u32, 0x00000000), calculate(""));

    // https://crccalc.com/?crc=0000000000000000000000000000000000000000000000000000000000000000&method=CRC-32/ISCSI&datatype=hex&outtype=hex
    const zeros = [_]u8{0} ** 32;
    try testing.expectEqual(@as(u32, 0x8A9136AA), calculate(&zeros));

    // https://crccalc.com/?crc=kafka-record-batch-payload&method=CRC-32/ISCSI&datatype=ascii&outtype=hex
    const payload = "kafka-record-batch-payload";
    try testing.expectEqual(@as(u32, 0xD10C504A), calculate(payload));
}

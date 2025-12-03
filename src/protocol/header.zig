const std = @import("std");
const codec = @import("codec.zig");
const tagged_fields = @import("tagged_fields.zig");
const types = @import("types.zig");

pub const RequestHeaderV1 = struct {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: ?[]const u8,

    pub fn encode(self: RequestHeaderV1, e: *codec.Encoder) !void {
        try e.writeI16(self.api_key);
        try e.writeI16(self.api_version);
        try e.writeI32(self.correlation_id);
        try e.writeNullableString(self.client_id);
    }
};

pub const RequestHeaderV2 = struct {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: ?[]const u8,

    pub fn encode(self: RequestHeaderV2, e: *codec.Encoder) !void {
        try e.writeI16(self.api_key);
        try e.writeI16(self.api_version);
        try e.writeI32(self.correlation_id);
        try e.writeNullableString(self.client_id);
        try tagged_fields.writeEmpty(e);
    }
};

pub const ResponseHeaderV0 = struct {
    correlation_id: i32,

    pub fn decode(d: *codec.Decoder) !ResponseHeaderV0 {
        return .{
            .correlation_id = try d.readI32(),
        };
    }
};

pub const ResponseHeaderV1 = struct {
    correlation_id: i32,

    pub fn decode(d: *codec.Decoder) !ResponseHeaderV1 {
        const correlation_id = try d.readI32();
        _ = try tagged_fields.skipAll(d);

        return .{
            .correlation_id = correlation_id,
        };
    }
};

pub const HeaderVersion = enum { v0, v1, v2 };

pub fn requestHeaderVersion(is_flexible: bool) HeaderVersion {
    return if (is_flexible) .v2 else .v1;
}

pub fn responseHeaderVersion(api_key: types.ApiKey, is_flexible: bool) HeaderVersion {
    if (api_key == .ApiVersions) {
        return .v0;
    }

    return if (is_flexible) .v1 else .v0;
}

const testing = std.testing;

test "RequestHeaderV2 encodes with classic nullable string and empty tags" {
    var buf: [64]u8 = undefined;
    var e = codec.Encoder.init(&buf);

    const header = RequestHeaderV2{
        .api_key = 18,
        .api_version = 4,
        .correlation_id = 100,
        .client_id = "test-client",
    };
    try header.encode(&e);

    var d = codec.Decoder.init(e.written());
    try testing.expectEqual(@as(i16, 18), try d.readI16());
    try testing.expectEqual(@as(i16, 4), try d.readI16());
    try testing.expectEqual(@as(i32, 100), try d.readI32());

    const client_id = (try d.readNullableString()).?;
    try testing.expectEqualStrings("test-client", client_id);

    try testing.expectEqual(@as(u32, 0), try d.readUVarint32());
}

test "ResponseHeader selection logic" {
    try testing.expectEqual(HeaderVersion.v1, responseHeaderVersion(.Fetch, true));
    try testing.expectEqual(HeaderVersion.v0, responseHeaderVersion(.Produce, false));
    try testing.expectEqual(HeaderVersion.v0, responseHeaderVersion(.ApiVersions, true));
}

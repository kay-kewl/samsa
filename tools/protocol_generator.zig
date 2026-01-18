const std = @import("std");
const jsonc = @import("jsonc");

const required = [_][]const u8{
    "ApiVersionsRequest.json",
    "MetadataRequest.json",
    "ProduceRequest.json",
    "FetchRequest.json",
    "ListOffsetsRequest.json",
};

const VersionRange = struct {
    min: i16,
    max: i16,

    pub fn none() VersionRange {
        return .{
            .min = 1,
            .max = 0,
        };
    }

    pub fn contains(self: VersionRange, version: i16) bool {
        return version >= self.min and version <= self.max;
    }

    pub fn parse(raw_opt: ?[]const u8) !VersionRange {
        const raw = raw_opt orelse return .none();
        if (std.mem.eql(u8, raw, "none")) {
            return .none();
        }

        if (std.mem.indexOfScalar(u8, raw, '+')) |i| {
            const low = try std.fmt.parseInt(i16, raw[0..i], 10);
            return .{
                .min = low,
                .max = std.math.maxInt(i16),
            };
        }
        if (std.mem.indexOfScalar(u8, raw, '-')) |i| {
            const low = try std.fmt.parseInt(i16, raw[0..i], 10);
            const high = try std.fmt.parseInt(i16, raw[i + 1 ..], 10);
            return .{
                .min = low,
                .max = high,
            };
        }

        const v = try std.fmt.parseInt(i16, raw, 10);
        return .{
            .min = v,
            .max = v,
        };
    }
};

const FieldSpec = struct {
    name: []const u8,
    snake_name: []const u8,
    wire_type: []const u8,
    versions: VersionRange,
    nullable_versions: VersionRange,
    tagged_versions: VersionRange,
    tag: ?u32,
    default_raw: ?[]const u8,
    fields: []FieldSpec,

    fn isArray(self: FieldSpec) bool {
        return std.mem.startsWith(u8, self.wire_type, "[]");
    }

    fn innerTypeName(self: FieldSpec) []const u8 {
        if (self.isArray()) {
            return self.wire_type[2..];
        }

        return self.wire_type;
    }
};

const MessageSchema = struct {
    name: []const u8,
    api_key: i16,
    valid_versions: VersionRange,
    flexible_versions: VersionRange,
    fields: []FieldSpec,
};

const ApiSpec = struct {
    file_name: []const u8,
    api_name: []const u8,
    api_key: i16,
    request: MessageSchema,
    response: MessageSchema,
};

fn appendSnakeCase(allocator: std.mem.Allocator, out: *std.ArrayList(u8), input: []const u8) !void {
    for (input, 0..) |c, i| {
        const is_upper = c >= 'A' and c <= 'Z';
        if (is_upper and i != 0) {
            try out.append(allocator, '_');
        }

        try out.append(allocator, if (is_upper) (c + 32) else c);
    }
}

fn toSnakeCase(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try appendSnakeCase(allocator, &out, input);
    return out.toOwnedSlice(allocator);
}

fn requestJsonToApiFileName(allocator: std.mem.Allocator, json_name: []const u8) ![]u8 {
    if (!std.mem.endsWith(u8, json_name, "Request.json")) {
        return error.InvalidInput;
    }

    const stem = json_name[0 .. json_name.len - ".json".len];
    if (!std.mem.endsWith(u8, stem, "Request")) {
        return error.InvalidInput;
    }

    const base = stem[0 .. stem.len - "Request".len];

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    try appendSnakeCase(allocator, &out, base);
    try out.appendSlice(allocator, ".zig");
    return out.toOwnedSlice(allocator);
}

fn parseField(allocator: std.mem.Allocator, v: std.json.Value) !FieldSpec {
    const obj = v.object;
    const name = obj.get("name").?.string;
    const snake_name = try toSnakeCase(allocator, name);

    const tagged_raw = if (obj.get("taggedVersions")) |x| x.string else null;
    const tagged_versions = try VersionRange.parse(tagged_raw);

    const versions = if (obj.get("versions")) |x|
        try VersionRange.parse(x.string)
    else
        tagged_versions;

    const nullable_raw = if (obj.get("nullableVersions")) |x| x.string else null;
    const nullable_versions = try VersionRange.parse(nullable_raw);

    var children: std.ArrayList(FieldSpec) = .empty;
    if (obj.get("fields")) |field_v| {
        for (field_v.array.items) |child_v| {
            try children.append(allocator, try parseField(allocator, child_v));
        }
    }

    return .{
        .name = name,
        .snake_name = snake_name,
        .wire_type = obj.get("type").?.string,
        .versions = versions,
        .nullable_versions = nullable_versions,
        .tagged_versions = tagged_versions,
        .tag = if (obj.get("tag")) |x| @intCast(x.integer) else null,
        .default_raw = if (obj.get("default")) |x| x.string else null,
        .fields = try children.toOwnedSlice(allocator),
    };
}

fn parseMessageSchema(allocator: std.mem.Allocator, clean_json: []const u8) !MessageSchema {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, clean_json, .{});
    defer parsed.deinit();
    const root = parsed.value.object;

    var fields: std.ArrayList(FieldSpec) = .empty;
    for (root.get("fields").?.array.items) |field_v| {
        try fields.append(allocator, try parseField(allocator, field_v));
    }

    return .{
        .name = try allocator.dupe(u8, root.get("name").?.string),
        .api_key = @intCast(root.get("apiKey").?.integer),
        .valid_versions = try VersionRange.parse(root.get("validVersions").?.string),
        .flexible_versions = try VersionRange.parse(root.get("flexibleVersions").?.string),
        .fields = try fields.toOwnedSlice(allocator),
    };
}

fn rangeIsSet(range: VersionRange) bool {
    return range.min <= range.max;
}

fn writeVersionRangeCheck(w: anytype, version_expression: []const u8, range: VersionRange) !void {
    try w.print("{s} >= {d}", .{ version_expression, range.min });
    if (range.max != std.math.maxInt(i16)) {
        try w.print(" and {s} <= {d}", .{ version_expression, range.max });
    }
}

fn fieldDecodeUsesAllocator(field: FieldSpec) bool {
    if (field.isArray() or field.fields.len > 0) {
        return true;
    }

    return false;
}

fn structDecodeUsesAllocator(fields: []const FieldSpec) bool {
    for (fields) |f| {
        if (fieldDecodeUsesAllocator(f)) {
            return true;
        }
    }

    return false;
}

fn renderZigType(w: anytype, field: FieldSpec) !void {
    const is_nullable = rangeIsSet(field.nullable_versions);
    if (is_nullable) {
        try w.writeAll("?");
    }

    var t: []const u8 = field.wire_type;
    if (field.isArray()) {
        try w.writeAll("[]const ");
        t = field.innerTypeName();
    }

    if (std.mem.eql(u8, t, "int8")) {
        try w.writeAll("i8");
    } else if (std.mem.eql(u8, t, "int16")) {
        try w.writeAll("i16");
    } else if (std.mem.eql(u8, t, "int32")) {
        try w.writeAll("i32");
    } else if (std.mem.eql(u8, t, "int64")) {
        try w.writeAll("i64");
    } else if (std.mem.eql(u8, t, "float64")) {
        try w.writeAll("f64");
    } else if (std.mem.eql(u8, t, "boolean")) {
        try w.writeAll("bool");
    } else if (std.mem.eql(u8, t, "uuid")) {
        try w.writeAll("types.Uuid");
    } else if (std.mem.eql(u8, t, "string") or
        std.mem.eql(u8, t, "bytes") or
        std.mem.eql(u8, t, "records"))
    {
        try w.writeAll("[]const u8");
    } else {
        try w.writeAll(t);
    }
}

fn renderDefaultValue(w: anytype, field: FieldSpec) !void {
    if (field.default_raw) |raw| {
        if (std.mem.eql(u8, raw, "null")) {
            try w.writeAll("null");
        } else if (std.mem.eql(u8, field.wire_type, "bool") or std.mem.eql(u8, field.wire_type, "boolean")) {
            try w.writeAll(raw);
        } else if (std.mem.eql(u8, field.wire_type, "string")) {
            try w.print("\"{s}\"", .{raw});
        } else {
            try w.writeAll(raw);
        }

        return;
    }

    if (field.isArray()) {
        try w.writeAll("&.{}");
    } else if (field.fields.len > 0) {
        try w.writeAll(".{}");
    } else if (std.mem.eql(u8, field.wire_type, "string") or std.mem.eql(u8, field.wire_type, "bytes")) {
        try w.writeAll("&.{}");
    } else if (std.mem.eql(u8, field.wire_type, "records")) {
        if (rangeIsSet(field.nullable_versions)) {
            try w.writeAll("null");
        } else {
            try w.writeAll("&.{}");
        }
    } else if (std.mem.eql(u8, field.wire_type, "bool") or std.mem.eql(u8, field.wire_type, "boolean")) {
        try w.writeAll("false");
    } else if (std.mem.eql(u8, field.wire_type, "uuid")) {
        try w.writeAll(".{0} ** 16");
    } else {
        try w.writeAll("0");
    }
}

fn renderCodecCall(w: anytype, prefix: []const u8, field: FieldSpec, context: enum { encode, decode }, e_or_d: []const u8) !void {
    const t = field.wire_type;
    const inner = field.innerTypeName();

    const is_array = field.isArray();
    const is_nullable = rangeIsSet(field.nullable_versions);
    const is_string = std.mem.eql(u8, t, "string") or std.mem.eql(u8, inner, "string");
    const is_bytes = std.mem.eql(u8, t, "bytes") or std.mem.eql(u8, inner, "bytes");
    const is_records = std.mem.eql(u8, t, "records") or std.mem.eql(u8, inner, "records");
    const is_struct = field.fields.len > 0 and !is_array;
    const is_bool = std.mem.eql(u8, t, "bool") or std.mem.eql(u8, t, "boolean");

    if (context == .encode) {
        if (is_array) {
            try w.print("        {{\n", .{});
            if (is_nullable) {
                try w.print("            if ({s}) |array| {{\n", .{prefix});
            } else {
                try w.print("            const array = {s};\n", .{prefix});
            }

            try w.print(
                \\                if (is_flex) {{
                \\                    try {s}.writeCompactArray(array.len);
                \\                }} else {{
                \\                    try {s}.writeArrayLength(array.len);
                \\                }}
                \\
                \\                for (array) |item| {{
            , .{ e_or_d, e_or_d });

            if (field.fields.len > 0) {
                try w.print("                    try item.encode({s}, version);\n", .{e_or_d});
            } else if (std.mem.eql(u8, inner, "string")) {
                try w.print(
                    \\                    if (is_flex) {{
                    \\                        try {s}.writeCompactString(item);
                    \\                    }} else {{
                    \\                        try {s}.writeString(item);
                    \\                    }}
                , .{ e_or_d, e_or_d });
            } else if (std.mem.eql(u8, inner, "bytes") or std.mem.eql(u8, inner, "records")) {
                try w.print(
                    \\                    if (is_flex) {{
                    \\                        try {s}.writeCompactBytesArray(item);
                    \\                    }} else {{
                    \\                        try {s}.writeBytesArray(item);
                    \\                    }}
                , .{ e_or_d, e_or_d });
            } else if (std.mem.eql(u8, inner, "uuid")) {
                try w.print("                    try {s}.writeUuid(item);\n", .{e_or_d});
            } else {
                try w.print("                    try {s}.writeI{s}(item);\n", .{ e_or_d, inner[3..] });
            }

            try w.print("                }}\n", .{});

            if (is_nullable) {
                try w.writeAll("            } else {\n                if (!(");
                try writeVersionRangeCheck(w, "version", field.nullable_versions);
                try w.print(
                    \\)) return error.InvalidNullForVersion;
                    \\                if (is_flex) {{
                    \\                    try {s}.writeCompactNullableArray(null);
                    \\                }} else {{
                    \\                    try {s}.writeNullableArrayLength(null);
                    \\                }}
                    \\            }}
                , .{ e_or_d, e_or_d });
            }
            try w.print("        }}\n", .{});

            return;
        }

        if (is_struct) {
            if (is_nullable) {
                try w.print(
                    \\if ({s}) |value| {{
                    \\    try value.encode({s}, version);
                    \\}} else {{
                    \\    return error.UnsupportedNullableStruct;
                    \\}}
                    \\
                , .{ prefix, e_or_d });
            } else {
                try w.print(
                    \\try {s}.encode({s}, version);
                    \\
                , .{ prefix, e_or_d });
            }

            return;
        }

        if (is_string) {
            if (is_nullable) {
                try w.print("if ({s} == null and !(", .{prefix});
                try writeVersionRangeCheck(w, "version", field.nullable_versions);
                try w.writeAll(")) return error.InvalidNullForVersion;\n");

                try w.print(
                    \\if (is_flex) {{
                    \\    try {s}.writeCompactNullableString({s});
                    \\}} else {{
                    \\    try {s}.writeNullableString({s});
                    \\}}
                    \\
                , .{ e_or_d, prefix, e_or_d, prefix });
            } else {
                try w.print(
                    \\if (is_flex) {{
                    \\    try {s}.writeCompactString({s});
                    \\}} else {{
                    \\    try {s}.writeString({s});
                    \\}}
                    \\
                , .{ e_or_d, prefix, e_or_d, prefix });
            }
        } else if (is_bytes or is_records) {
            if (is_nullable) {
                try w.print("if ({s} == null and !(", .{prefix});
                try writeVersionRangeCheck(w, "version", field.nullable_versions);
                try w.writeAll(")) return error.InvalidNullForVersion;\n");

                try w.print(
                    \\if (is_flex) {{
                    \\    try {s}.writeCompactNullableBytesArray({s});
                    \\}} else {{
                    \\    try {s}.writeNullableBytesArray({s});
                    \\}}
                    \\
                , .{ e_or_d, prefix, e_or_d, prefix });
            } else {
                try w.print(
                    \\if (is_flex) {{
                    \\    try {s}.writeCompactBytesArray({s});
                    \\}} else {{
                    \\    try {s}.writeBytesArray({s});
                    \\}}
                    \\
                , .{ e_or_d, prefix, e_or_d, prefix });
            }
        } else if (is_bool) {
            try w.print(
                \\try {s}.writeBoolean({s});
                \\
            , .{ e_or_d, prefix });
        } else if (std.mem.eql(u8, t, "uuid")) {
            try w.print(
                \\try {s}.writeUuid({s});
                \\
            , .{ e_or_d, prefix });
        } else if (std.mem.eql(u8, t, "float64")) {
            try w.print(
                \\try {s}.writeFloat64({s});
                \\
            , .{ e_or_d, prefix });
        } else {
            try w.print(
                \\try {s}.writeI{s}({s});
                \\
            , .{ e_or_d, t[3..], prefix });
        }
    } else {
        if (is_array and field.fields.len > 0) {
            if (is_nullable) {
                try w.print(
                    \\if (is_flex) {{
                    \\    const raw_len = try {s}.readUVarint32();
                    \\    if (raw_len == 0) {{
                    \\        {s} = null;
                    \\    }} else {{
                    \\        const len: usize = @intCast(raw_len - 1);
                    \\        try ensureArrayLenSafe({s}, len);
                    \\        const array = try allocator.alloc({s}, len);
                    \\        for (array) |*item| {{
                    \\            item.* = try {s}.decode(allocator, {s}, version);
                    \\        }}
                    \\
                    \\        {s} = array;
                    \\    }}
                    \\}} else {{
                    \\    const raw_len = try {s}.readI32();
                    \\    if (raw_len < 0) {{
                    \\        {s} = null;
                    \\    }} else {{
                    \\        const len: usize = @intCast(raw_len);
                    \\        const array = try allocator.alloc({s}, len);
                    \\        for (array) |*item| {{
                    \\            item.* = try {s}.decode(allocator, {s}, version);
                    \\        }}
                    \\
                    \\        {s} = array;
                    \\    }}
                    \\}}
                    \\
                , .{ e_or_d, prefix, inner, inner, inner, e_or_d, prefix, e_or_d, prefix, inner, inner, e_or_d, prefix });
            } else {
                try w.print(
                    \\const len = if (is_flex) try {s}.readCompactArrayLength() else try {s}.readArrayLength();
                    \\const array = try allocator.alloc({s}, len);
                    \\for (array) |*item| {{
                    \\    item.* = try {s}.decode(allocator, {s}, version);
                    \\}}
                    \\
                    \\{s} = array;
                    \\
                , .{ e_or_d, e_or_d, inner, inner, e_or_d, prefix });
            }

            return;
        }

        if (is_struct) {
            if (is_nullable) {
                try w.print(
                    \\return error.UnsupportedNullableStruct;
                    \\
                , .{});
            } else {
                try w.print(
                    \\{s} = try {s}.decode(allocator, {s}, version);
                    \\
                , .{ prefix, t, e_or_d });
            }

            return;
        } else if (is_string) {
            if (is_nullable) {
                try w.print(
                    \\{s} = if (is_flex) try {s}.readCompactNullableString() else try {s}.readNullableString();
                    \\
                , .{ prefix, e_or_d, e_or_d });
            } else {
                try w.print(
                    \\{s} = if (is_flex) try {s}.readCompactString() else try {s}.readString();
                    \\
                , .{ prefix, e_or_d, e_or_d });
            }
        } else if (is_records) {
            if (is_nullable) {
                try w.print(
                    \\{s} = if (is_flex) try {s}.readCompactNullableRecords() else try {s}.readNullableRecords();
                    \\
                , .{ prefix, e_or_d, e_or_d });
            } else {
                try w.print(
                    \\{s} = if (is_flex) try {s}.readCompactRecords() else try {s}.readRecords();
                    \\
                , .{ prefix, e_or_d, e_or_d });
            }
        } else if (is_bytes) {
            if (is_nullable) {
                try w.print(
                    \\{s} = if (is_flex) try {s}.readCompactNullableBytesArray() else try {s}.readNullableBytesArray();
                    \\
                , .{ prefix, e_or_d, e_or_d });
            } else {
                try w.print(
                    \\{s} = if (is_flex) try {s}.readCompactBytesArray() else try {s}.readBytesArray();
                    \\
                , .{ prefix, e_or_d, e_or_d });
            }
        } else if (is_bool) {
            try w.print(
                \\{s} = try {s}.readBoolean();
                \\
            , .{ prefix, e_or_d });
        } else if (std.mem.eql(u8, t, "uuid")) {
            try w.print(
                \\{s} = try {s}.readUuid();
                \\
            , .{ prefix, e_or_d });
        } else if (std.mem.eql(u8, t, "float64")) {
            try w.print(
                \\{s} = try {s}.readFloat64();
                \\
            , .{ prefix, e_or_d });
        } else {
            if (is_array) {
                const zig_t = if (std.mem.eql(u8, inner, "int8"))
                    "i8"
                else if (std.mem.eql(u8, inner, "int16"))
                    "i16"
                else if (std.mem.eql(u8, inner, "int32"))
                    "i32"
                else
                    "i64";

                if (is_nullable) {
                    try w.print(
                        \\if (is_flex) {{
                        \\    const raw_len = try {s}.readUVarint32();
                        \\    if (raw_len == 0) {{
                        \\        {s} = null;
                        \\    }} else {{
                        \\        const len: usize = @intCast(raw_len - 1);
                        \\        try ensureArrayLenSafe({s}, len);
                        \\        const array = try allocator.alloc({s}, len);
                        \\        for (array) |*item| {{
                        \\            item.* = try {s}.readI{s}();
                        \\        }}
                        \\
                        \\        {s} = array;
                        \\    }}
                        \\}} else {{
                        \\    const raw_len = try {s}.readI32();
                        \\    if (raw_len < 0) {{
                        \\        {s} = null;
                        \\    }} else {{
                        \\        const len: usize = @intCast(raw_len);
                        \\        const array = try allocator.alloc({s}, len);
                        \\        for (array) |*item| {{
                        \\            item.* = try {s}.readI{s}();
                        \\        }}
                        \\
                        \\        {s} = array;
                        \\    }}
                        \\}}
                        \\
                    , .{ e_or_d, prefix, prefix, zig_t, e_or_d, inner[3..], prefix, e_or_d, prefix, zig_t, e_or_d, inner[3..], prefix });
                } else {
                    try w.print(
                        \\const len = if (is_flex) try {s}.readCompactArrayLength() else try {s}.readArrayLength();
                        \\const array = try allocator.alloc({s}, len);
                        \\for (array) |*item| {{
                        \\    item.* = try {s}.readI{s}();
                        \\}}
                        \\
                        \\{s} = array;
                        \\
                    , .{ e_or_d, e_or_d, zig_t, e_or_d, inner[3..], prefix });
                }
            } else {
                try w.print(
                    \\{s} = try {s}.readI{s}();
                    \\
                , .{ prefix, e_or_d, t[3..] });
            }
        }
    }
}

fn renderStruct(w: anytype, name: []const u8, fields: []const FieldSpec, flexible_versions: VersionRange) !void {
    try w.print("pub const {s} = struct {{\n", .{name});

    for (fields) |f| {
        if (f.fields.len > 0) {
            try renderStruct(w, f.innerTypeName(), f.fields, flexible_versions);
        }
    }

    for (fields) |f| {
        try w.print("    {s}: ", .{f.snake_name});
        try renderZigType(w, f);
        try w.writeAll(" = ");
        try renderDefaultValue(w, f);
        try w.writeAll(",\n");
    }

    try w.print("\n    pub fn encode(self: @This(), e: *codec.Encoder, version: i16) !void {{\n", .{});
    try w.writeAll("        const is_flex = ");
    if (rangeIsSet(flexible_versions)) {
        try writeVersionRangeCheck(w, "version", flexible_versions);
    } else {
        try w.writeAll("false");
    }
    try w.writeAll(";\n");

    var tagged_fields: std.ArrayList(FieldSpec) = .empty;
    defer tagged_fields.deinit(std.heap.page_allocator);

    for (fields) |f| {
        if (f.tag != null) {
            try tagged_fields.append(std.heap.page_allocator, f);
            continue;
        }

        try w.print("        if (version >= {d}", .{f.versions.min});
        if (f.versions.max != std.math.maxInt(i16)) {
            try w.print(" and version <= {d}", .{f.versions.max});
        }
        try w.writeAll(") {\n");
        const access = try std.fmt.allocPrint(std.heap.page_allocator, "self.{s}", .{f.snake_name});
        try renderCodecCall(w, access, f, .encode, "e");
        try w.writeAll("        }\n");
    }

    try w.print("\n        if (is_flex) {{\n", .{});
    if (tagged_fields.items.len == 0) {
        try w.print("            try e.writeUVarint32(0);\n", .{});
    } else {
        try w.print("            var num_tags: u32 = 0;\n", .{});
        for (tagged_fields.items) |tf| {
            try w.writeAll("            if (");
            try writeVersionRangeCheck(w, "version", tf.tagged_versions);
            try w.writeAll(") {\n                num_tags += 1;\n            }\n");
        }

        try w.print("        try e.writeUVarint32(num_tags);\n", .{});
        for (tagged_fields.items) |tf| {
            try w.writeAll("            if (");
            try writeVersionRangeCheck(w, "version", tf.tagged_versions);
            try w.writeAll(") {\n");

            try w.print(
                \\                var tag_buf: [4096]u8 = undefined;
                \\                var tag_e = codec.Encoder.init(&tag_buf);
                \\
            , .{});

            const access = try std.fmt.allocPrint(std.heap.page_allocator, "self.{s}", .{tf.snake_name});
            try renderCodecCall(w, access, tf, .encode, "(&tag_e)");

            try w.print(
                \\                const tag_bytes = tag_e.written();
                \\                try e.writeUVarint32({d});
                \\                try e.writeUVarint32(@intCast(tag_bytes.len));
                \\                if (e.pos + tag_bytes.len > e.buf.len) {{
                \\                    return error.NoSpace;
                \\                }}
                \\                @memcpy(e.buf[e.pos .. e.pos + tag_bytes.len], tag_bytes);
                \\                e.pos += tag_bytes.len;
                \\            }}
                \\
            , .{tf.tag.?});
        }
    }

    try w.print(
        \\        }}
        \\    }}
        \\
        \\    pub fn decode(allocator: std.mem.Allocator, d: *codec.Decoder, version: i16) !@This() {{
        \\        try d.enterDepth();
        \\        defer d.exitDepth();
        \\    
    , .{});

    if (!structDecodeUsesAllocator(fields)) {
        try w.writeAll("        _ = allocator;\n");
    }

    try w.writeAll("        const is_flex = ");
    if (rangeIsSet(flexible_versions)) {
        try writeVersionRangeCheck(w, "version", flexible_versions);
    } else {
        try w.writeAll("false");
    }
    try w.writeAll(";\n");

    try w.print("        var out: @This() = .{{}};\n", .{});
    for (fields) |f| {
        if (f.tag != null) {
            continue;
        }

        try w.print("        if (version >= {d}", .{f.versions.min});
        if (f.versions.max != std.math.maxInt(i16)) {
            try w.print(" and version <= {d}", .{f.versions.max});
        }
        try w.print(") {{\n        ", .{});

        const access = try std.fmt.allocPrint(std.heap.page_allocator, "out.{s}", .{f.snake_name});
        try renderCodecCall(w, access, f, .decode, "d");
        try w.print("        }}\n", .{});
    }

    try w.print(
        \\        if (is_flex) {{
        \\            const num_tags = try d.readUVarint32();
        \\            var i: u32 = 0;
        \\            var last_tag: ?u32 = null;
        \\            while (i < num_tags) : (i += 1) {{
        \\                const tag_id = try d.readUVarint32();
        \\                if (last_tag) |prev| {{
        \\                    if (tag_id == prev) {{
        \\                        return error.DuplicateTag;  
        \\                    }} else if (tag_id < prev) {{
        \\                        return error.InvalidTagOrder;
        \\                    }}
        \\                }}
        \\                last_tag = tag_id;
        \\
        \\                const tag_len_u32 = try d.readUVarint32();
        \\                const tag_len: usize = @intCast(tag_len_u32);
        \\                if (tag_len > d.limits.max_tagged_field_bytes) {{
        \\                    return error.TagTooLarge;
        \\                }} else if (tag_len > d.remaining()) {{
        \\                    return error.EndOfStream;
        \\                }}
        \\
        \\                const tag_start = d.pos;
        \\
    , .{});

    if (tagged_fields.items.len > 0) {
        try w.print("                switch (tag_id) {{\n", .{});
        for (tagged_fields.items) |tf| {
            try w.print("                    {d} => {{\n                        ", .{tf.tag.?});
            const access = try std.fmt.allocPrint(std.heap.page_allocator, "out.{s}", .{tf.snake_name});
            try renderCodecCall(w, access, tf, .decode, "d");
            try w.print("                    }},\n", .{});
        }
        try w.print(
            \\                    else => {{}},
            \\                }}
            \\
        , .{});
    } else {
        // try w.print(
        //     \\                _ = tag_id;
        //     \\
        // , .{});
    }
    try w.print(
        \\                const consumed = d.pos - tag_start;
        \\                if (consumed > tag_len) {{
        \\                    return error.InvalidTaggedFieldSize;
        \\                }}
        \\                _ = try d.readBytes(tag_len - consumed);
        \\            }}
        \\        }}
        \\
        \\        return out;
        \\    }}
        \\}};
        \\
    , .{});
}

fn writeIfChanged(allocator: std.mem.Allocator, dir: std.fs.Dir, path: []const u8, new_content: []const u8) !void {
    const existing = dir.readFileAlloc(allocator, path, 16 * 1024 * 1024) catch null;
    if (existing) |old| {
        defer allocator.free(old);
        if (std.mem.eql(u8, old, new_content)) {
            return;
        }
    }

    var f = try dir.createFile(path, .{ .truncate = true });
    defer f.close();
    try f.writeAll(new_content);
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    _ = args.next();

    const input_dir_path = args.next() orelse @panic("Expected input directory (kafka-profile)");
    const output_dir_path = args.next() orelse @panic("Expected output directory (src/generated)");

    var input_dir = try std.fs.cwd().openDir(input_dir_path, .{ .iterate = true });
    defer input_dir.close();

    var output_dir = try std.fs.cwd().openDir(output_dir_path, .{});
    defer output_dir.close();

    var specs: std.ArrayList(ApiSpec) = .empty;

    for (required) |name| {
        const file = try input_dir.openFile(name, .{});
        const raw_request_content = try file.readToEndAlloc(allocator, 1024 * 1024);
        file.close();

        const clean_request_json = try jsonc.stripJsonc(allocator, raw_request_content);
        const request_schema = try parseMessageSchema(allocator, clean_request_json);

        const response_name = try std.fmt.allocPrint(allocator, "{s}Response.json", .{request_schema.name[0 .. request_schema.name.len - "Request".len]});
        const response_file = try input_dir.openFile(response_name, .{});
        const raw_response_content = try response_file.readToEndAlloc(allocator, 1024 * 1024);
        response_file.close();

        const clean_response_json = try jsonc.stripJsonc(allocator, raw_response_content);
        const response_schema = try parseMessageSchema(allocator, clean_response_json);
        if (response_schema.api_key != request_schema.api_key) {
            return error.ApiKeyMismatch;
        }

        try specs.append(allocator, .{
            .file_name = try requestJsonToApiFileName(allocator, name),
            .api_name = request_schema.name[0 .. request_schema.name.len - "Request".len],
            .api_key = request_schema.api_key,
            .request = request_schema,
            .response = response_schema,
        });
        std.debug.print("Parsed API: {s}\n", .{request_schema.name});
    }

    const Context = struct {};
    const less = struct {
        fn f(_: Context, a: ApiSpec, b: ApiSpec) bool {
            if (a.api_key != b.api_key) {
                return a.api_key < b.api_key;
            }

            return std.mem.lessThan(u8, a.file_name, b.file_name);
        }
    }.f;
    std.mem.sort(ApiSpec, specs.items, Context{}, less);

    for (specs.items) |s| {
        var out: std.ArrayList(u8) = .empty;
        const w = out.writer(allocator);
        try w.print(
            \\const std = @import("std");
            \\const types = @import("../protocol/types.zig");
            \\const codec = @import("../protocol/codec.zig");
            \\
            \\fn ensureArrayLenSafe(d: *codec.Decoder, len: usize) !void {{
            \\    if (len > d.limits.max_array_elements) {{
            \\        return error.LimitExceeded;
            \\    }}
            \\
            \\    if (len > d.remaining()) {{
            \\        return error.EndOfStream;
            \\    }}
            \\}}
            \\
            \\pub const api_name = "{s}";
            \\pub const api_key: types.ApiKey = @enumFromInt({d});
            \\
        , .{ s.api_name, s.api_key });

        try renderStruct(w, "Request", s.request.fields, s.request.flexible_versions);
        try renderStruct(w, "Response", s.response.fields, s.response.flexible_versions);

        try writeIfChanged(allocator, output_dir, s.file_name, out.items);
        std.debug.print("Generated: {s}\n", .{s.file_name});
    }

    var module_out: std.ArrayList(u8) = .empty;
    const mw = module_out.writer(allocator);
    for (specs.items) |s| {
        const stem = s.file_name[0 .. s.file_name.len - ".zig".len];
        try mw.print("pub const {s} = @import(\"{s}\");\n", .{ stem, s.file_name });
    }
    try writeIfChanged(allocator, output_dir, "module.zig", module_out.items);
    std.debug.print("Updated module.zig\n", .{});

    var fmt_proc = std.process.Child.init(&[_][]const u8{ "zig", "fmt", output_dir_path }, allocator);
    const term = try fmt_proc.spawnAndWait();

    switch (term) {
        .Exited => |code| if (code == 0) {
            std.debug.print("Successfully formatted generated code\n", .{});
        } else {
            std.debug.print("Warning: zig fmt exited with code {d}\n", .{code});
        },
        else => std.debug.print("Warning: zig fmt terminated unexpectedly\n", .{}),
    }
}

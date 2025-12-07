const std = @import("std");
const jsonc = @import("jsonc");

const required = [_][]const u8{
    "ApiVersionsRequest.json",
    "ApiVersionsResponse.json",
    "MetadataRequest.json",
    "MetadataResponse.json",
    "ProduceRequest.json",
    "ProduceResponse.json",
    "FetchRequest.json",
    "FetchResponse.json",
    "ListOffsetsRequest.json",
    "ListOffsetsResponse.json",
};

const ApiSpec = struct {
    file_name: []const u8,
    api_name: []const u8,
    api_key: i16,
    flex_base: i16,
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

fn requestJsonToApiFileName(allocator: std.mem.Allocator, json_name: []const u8) ![]u8 {
    if (!std.mem.endsWith(u8, json_name, "Request.json")) {
        return error.InvalidInput;
    }

    const stem = json_name[0 .. json_name.len - ".json".len];
    if (!std.mem.endsWith(u8, stem, "Request")) {
        return error.InvalidInput;
    }

    const base = stem[0 .. stem.len - "Request".len];

    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);

    try appendSnakeCase(allocator, &out, base);
    try out.appendSlice(allocator, ".zig");
    return out.toOwnedSlice(allocator);
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

fn parseFlexBase(flexible_v: []const u8) !i16 {
    if (std.mem.eql(u8, flexible_v, "none")) {
        return 32767;
    }

    if (std.mem.indexOf(u8, flexible_v, "+")) |index| {
        return try std.fmt.parseInt(i16, flexible_v[0..index], 10);
    }

    return 32767;
}

fn renderApiFile(allocator: std.mem.Allocator, spec: ApiSpec) ![]u8 {
    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);
    const w = out.writer(allocator);

    try w.print(
        \\const types = @import("../protocol/types.zig");
        \\
        \\pub const api_name = "{s}";
        \\pub const api_key: types.ApiKey = @as(types.ApiKey, @enumFromInt({d}));
        \\pub const flexible_base_version: i16 = {d};
        \\
        \\pub fn isFlexible(version: i16) bool {{
        \\    return version >= flexible_base_version;
        \\}}
        \\
    , .{ spec.api_name, spec.api_key, spec.flex_base });

    return out.toOwnedSlice(allocator);
}

fn renderModuleFile(allocator: std.mem.Allocator, specs: []const ApiSpec) ![]u8 {
    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);
    const w = out.writer(allocator);

    for (specs) |s| {
        const stem = s.file_name[0 .. s.file_name.len - ".zig".len];
        try w.print("pub const {s} = @import(\"{s}\");\n", .{ stem, s.file_name });
    }

    return out.toOwnedSlice(allocator);
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

    for (required) |name| {
        _ = input_dir.openFile(name, .{}) catch {
            std.debug.print("Missing required schema: {s}\n", .{name});
            return error.FileNotFound;
        };
    }

    var output_dir = try std.fs.cwd().openDir(output_dir_path, .{});
    defer output_dir.close();

    var specs = std.ArrayList(ApiSpec){};
    defer specs.deinit(allocator);

    var it = input_dir.iterate();
    while (try it.next()) |entry| {
        if (!std.mem.endsWith(u8, entry.name, "Request.json")) {
            continue;
        }

        const file = try input_dir.openFile(entry.name, .{});
        defer file.close();

        const raw_content = try file.readToEndAlloc(allocator, 1024 * 1024);
        const clean_json = try jsonc.stripJsonc(allocator, raw_content);

        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, clean_json, .{});
        const root = parsed.value.object;

        const api_key_i64 = root.get("apiKey").?.integer;
        const name = root.get("name").?.string;
        const flexible_v = root.get("flexibleVersions").?.string;

        const api_key: i16 = @intCast(api_key_i64);
        const flex_base = try parseFlexBase(flexible_v);
        const file_name = try requestJsonToApiFileName(allocator, entry.name);

        try specs.append(allocator, .{
            .file_name = file_name,
            .api_name = name,
            .api_key = api_key,
            .flex_base = flex_base,
        });

        std.debug.print("Generating {s} from {s}\n", .{ file_name, entry.name });
    }

    for (specs.items) |s| {
        const content = try renderApiFile(allocator, s);
        try writeIfChanged(allocator, output_dir, s.file_name, content);
    }

    const module_content = try renderModuleFile(allocator, specs.items);
    try writeIfChanged(allocator, output_dir, "module.zig", module_content);

    std.debug.print("Generated {d} API files + module.zig\n", .{specs.items.len});
}

const std = @import("std");

pub fn stripJsonc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);

    try out.ensureTotalCapacity(allocator, input.len);

    var i: usize = 0;
    var in_string = false;
    while (i < input.len) : (i += 1) {
        const c = input[i];
        if (c == '"' and (i == 0 or input[i - 1] != '\\')) {
            in_string = !in_string;
        }

        if (!in_string and c == '/' and i + 1 < input.len and input[i + 1] == '/') {
            while (i + 1 < input.len and input[i + 1] != '\n') : (i += 1) {}
        } else {
            try out.append(allocator, c);
        }
    }

    return out.toOwnedSlice(allocator);
}

test "stripJsonc: removes comments" {
    const input =
        \\{
        \\  "key": "value", // comment
        \\  "url": "https://example.com"
        \\}
    ;

    const expected =
        \\{
        \\  "key": "value", 
        \\  "url": "https://example.com"
        \\}
    ;

    const output = try stripJsonc(std.testing.allocator, input);
    defer std.testing.allocator.free(output);
    try std.testing.expectEqualSlices(u8, expected, output);
}

const std = @import("std");

pub fn stripJsonc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var out = std.ArrayList(u8).init(allocator);
    errdefer out.deinit();
    var i: usize = 0;
    var in_string = false;
    while (i < input.len) : (i += 1) {
        const c = input[i];
        if (c == '"' and (i == 0 or input[i - 1] != '\\')) {
            in_string = !in_string;
        }

        if (!in_string and c == '/' and i + 1 < input.len and input[i + 1] == '/') {
            while (i < input.len and input[i] != '\n') : (i += 1) {}
        } else {
            try out.append(c);
        }
    }

    return out.toOwnedSlice();
}

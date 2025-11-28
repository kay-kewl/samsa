const std = @import("std");

pub const protocol = @import("protocol/mod.zig");
pub const generated = @import("generated/mod.zig");

test "sanity" {
    try std.testing.expect(true);
}

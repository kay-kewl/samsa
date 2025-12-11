const std = @import("std");

pub const protocol = @import("protocol/module.zig");
pub const generated = @import("generated/module.zig");
pub const transport = @import("transport/module.zig");

test "sanity" {
    try std.testing.expect(true);
}

test {
    _ = @import("protocol/module.zig");
    _ = @import("transport/module.zig");
}

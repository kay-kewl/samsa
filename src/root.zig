const std = @import("std");

pub const protocol = @import("protocol/module.zig");
pub const generated = @import("generated/module.zig");
pub const transport = @import("transport/module.zig");
pub const cluster = @import("cluster/module.zig");
pub const client = @import("client/module.zig");

test "sanity" {
    try std.testing.expect(true);
}

test {
    _ = @import("protocol/module.zig");
    _ = @import("transport/module.zig");
    _ = @import("cluster/module.zig");
    _ = @import("client/module.zig");
}

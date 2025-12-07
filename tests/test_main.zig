const std = @import("std");

test "samsa test suite" {
    std.testing.refAllDecls(@This());
}

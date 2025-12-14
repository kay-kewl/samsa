const std = @import("std");
const connection = @import("connection.zig");

pub const Pool = struct {
    allocator: std.mem.Allocator,
    map: std.AutoHashMap(i32, connection.Connection),

    pub fn init(allocator: std.mem.Allocator) Pool {
        return .{
            .allocator = allocator,
            .map = std.AutoHashMap(i32, connection.Connection).init(allocator),
        };
    }

    pub fn deinit(self: *Pool) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }

        self.map.deinit();
    }

    pub fn getOrCreate(self: *Pool, broker_id: i32, config: connection.Config) !*connection.Connection {
        const gop = try self.map.getOrPut(broker_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = connection.Connection.init(self.allocator, config);
        }

        return gop.value_ptr;
    }
};

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

    pub fn getReady(self: *Pool, broker_id: i32, config: connection.Config) !*connection.Connection {
        const conn = try self.getOrCreate(broker_id, config);
        conn.connect() catch |err| {
            if (conn.state == .Dead) {
                self.remove(broker_id);
            }

            return err;
        };

        return conn;
    }

    pub fn remove(self: *Pool, broker_id: i32) void {
        if (self.map.fetchRemove(broker_id)) |kv| {
            var c = kv.value;
            c.deinit();
        }
    }

    pub fn closeAll(self: *Pool) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }

        self.map.clearRetainingCapacity();
    }
};

const testing = std.testing;

test "pool remove deletes connection entry" {
    var p = Pool.init(testing.allocator);
    defer p.deinit();

    _ = try p.getOrCreate(1, .{
        .host = "127.0.0.1",
        .port = 9092,
    });
    try testing.expectEqual(@as(usize, 1), p.map.count());

    p.remove(1);
    try testing.expectEqual(@as(usize, 0), p.map.count());
}

test "pool closeAll clears all entries" {
    var p = Pool.init(testing.allocator);
    defer p.deinit();

    _ = try p.getOrCreate(1, .{
        .host = "127.0.0.1",
        .port = 9092,
    });
    _ = try p.getOrCreate(2, .{
        .host = "127.0.0.1",
        .port = 9093,
    });
    try testing.expectEqual(@as(usize, 2), p.map.count());

    p.closeAll();
    try testing.expectEqual(@as(usize, 0), p.map.count());
}

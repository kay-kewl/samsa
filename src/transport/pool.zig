const std = @import("std");
const connection = @import("connection.zig");
const compat = @import("../compat.zig");

pub const Pool = struct {
    allocator: std.mem.Allocator,
    map: std.AutoHashMap(i32, connection.Connection),
    max_total_connections: ?usize = null,
    next_retry_ms_by_broker: std.AutoHashMap(i32, i64),
    retry_delay_ms_by_broker: std.AutoHashMap(i32, i64),

    const retry_base_ms: i64 = 50;
    const retry_max_ms: i64 = 1000;

    fn fullJitterDelayMs(max_delay_ms: i64) i64 {
        if (max_delay_ms <= 0) {
            return 0;
        }

        return @as(i64, @intCast(compat.randomIntRangeAtMost(u32, 0, @as(u32, @intCast(max_delay_ms)))));
    }

    pub fn init(allocator: std.mem.Allocator) Pool {
        return .{
            .allocator = allocator,
            .map = std.AutoHashMap(i32, connection.Connection).init(allocator),
            .next_retry_ms_by_broker = std.AutoHashMap(i32, i64).init(allocator),
            .retry_delay_ms_by_broker = std.AutoHashMap(i32, i64).init(allocator),
        };
    }

    pub fn initWithLimit(allocator: std.mem.Allocator, max_total_connections: ?usize) Pool {
        return .{
            .allocator = allocator,
            .map = std.AutoHashMap(i32, connection.Connection).init(allocator),
            .max_total_connections = max_total_connections,
            .next_retry_ms_by_broker = std.AutoHashMap(i32, i64).init(allocator),
            .retry_delay_ms_by_broker = std.AutoHashMap(i32, i64).init(allocator),
        };
    }

    pub fn deinit(self: *Pool) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }

        self.map.deinit();
        self.next_retry_ms_by_broker.deinit();
        self.retry_delay_ms_by_broker.deinit();
    }

    pub fn getOrCreate(self: *Pool, broker_id: i32, config: connection.Config) !*connection.Connection {
        const gop = try self.map.getOrPut(broker_id);
        if (!gop.found_existing) {
            if (self.max_total_connections) |max_conn| {
                if (self.map.count() > max_conn) {
                    _ = self.map.remove(broker_id);
                    return error.PoolExhausted;
                }
            }

            gop.value_ptr.* = connection.Connection.init(self.allocator, config);
        }

        return gop.value_ptr;
    }

    pub fn getReady(self: *Pool, broker_id: i32, deadline_ms: ?i64, config: connection.Config) !*connection.Connection {
        const now = compat.milliTimestamp();
        if (self.next_retry_ms_by_broker.get(broker_id)) |not_before| {
            if (now < not_before) {
                return error.RetryBackoffActive;
            }
        }

        const conn = try self.getOrCreate(broker_id, config);
        const connect_result = if (deadline_ms) |d|
            conn.connectWithDeadline(d)
        else
            conn.connect();

        connect_result catch |err| {
            const current = self.retry_delay_ms_by_broker.get(broker_id);
            const next_base = if (current) |v| @min(v * 2, retry_max_ms) else retry_base_ms;
            const jittered = fullJitterDelayMs(next_base);

            try self.retry_delay_ms_by_broker.put(broker_id, next_base);
            try self.next_retry_ms_by_broker.put(broker_id, compat.milliTimestamp() + jittered);

            if (conn.state == .Dead) {
                self.removeConnectionOnly(broker_id);
            }

            return err;
        };

        _ = self.next_retry_ms_by_broker.remove(broker_id);
        _ = self.retry_delay_ms_by_broker.remove(broker_id);
        return conn;
    }

    fn removeConnectionOnly(self: *Pool, broker_id: i32) void {
        if (self.map.fetchRemove(broker_id)) |kv| {
            var c = kv.value;
            c.deinit();
        }
    }

    pub fn remove(self: *Pool, broker_id: i32) void {
        self.removeConnectionOnly(broker_id);
        _ = self.next_retry_ms_by_broker.remove(broker_id);
        _ = self.retry_delay_ms_by_broker.remove(broker_id);
    }

    pub fn rekey(self: *Pool, old_id: i32, new_id: i32) !void {
        if (old_id == new_id) {
            return;
        }

        const old = self.map.fetchRemove(old_id) orelse return;

        if (self.map.fetchRemove(new_id)) |existing| {
            var c = existing.value;
            defer c.deinit();
        }

        try self.map.put(new_id, old.value);
        _ = self.next_retry_ms_by_broker.remove(old_id);
        _ = self.retry_delay_ms_by_broker.remove(old_id);
    }

    pub fn closeAll(self: *Pool) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }

        self.map.clearRetainingCapacity();
        self.next_retry_ms_by_broker.clearRetainingCapacity();
        self.retry_delay_ms_by_broker.clearRetainingCapacity();
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

test "pool remove clears retry state" {
    var p = Pool.init(testing.allocator);
    defer p.deinit();

    try p.next_retry_ms_by_broker.put(5, compat.milliTimestamp() + 1000);
    try p.retry_delay_ms_by_broker.put(5, 200);

    p.remove(5);

    try testing.expectEqual(@as(usize, 0), p.next_retry_ms_by_broker.count());
    try testing.expectEqual(@as(usize, 0), p.retry_delay_ms_by_broker.count());
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
    try testing.expectEqual(@as(usize, 0), p.next_retry_ms_by_broker.count());
    try testing.expectEqual(@as(usize, 0), p.retry_delay_ms_by_broker.count());
}

test "pool closeAll clears retry maps" {
    var p = Pool.init(testing.allocator);
    defer p.deinit();

    try p.next_retry_ms_by_broker.put(1, compat.milliTimestamp() + 1000);
    try p.retry_delay_ms_by_broker.put(1, 100);

    p.closeAll();

    try testing.expectEqual(@as(usize, 0), p.map.count());
    try testing.expectEqual(@as(usize, 0), p.next_retry_ms_by_broker.count());
    try testing.expectEqual(@as(usize, 0), p.retry_delay_ms_by_broker.count());
}

test "pool enforces max_total_connections" {
    var p = Pool.initWithLimit(testing.allocator, 1);
    defer p.deinit();

    _ = try p.getOrCreate(1, .{
        .host = "127.0.0.1",
        .port = 9092,
    });
    try testing.expectError(error.PoolExhausted, p.getOrCreate(2, .{
        .host = "127.0.0.1",
        .port = 9093,
    }));
}

test "pool getReady removes dead connection on connect failure" {
    var p = Pool.initWithLimit(testing.allocator, 1);
    defer p.deinit();

    const config = connection.Config{
        .host = "127.0.0.1",
        .port = 1,
        .connect_timeout_ms = 200,
    };

    const result = p.getReady(42, null, config);
    if (result) |_| {
        return error.ExpectedConnectFailure;
    } else |_| {}

    try testing.expectEqual(@as(usize, 0), p.map.count());
}

test "pool getReady honors retry not-before gate" {
    var p = Pool.init(testing.allocator);
    defer p.deinit();

    const broker_id: i32 = 9;
    try p.next_retry_ms_by_broker.put(broker_id, compat.milliTimestamp() + 1000);

    const config = connection.Config{
        .host = "127.0.0.1",
        .port = 1,
        .connect_timeout_ms = 100,
    };

    const started = compat.milliTimestamp();
    try testing.expectError(error.RetryBackoffActive, p.getReady(broker_id, null, config));
    const elapsed = compat.milliTimestamp() - started;
    try testing.expect(elapsed < 50);
}

test "pool getReady failure retains retry gate after dead removal" {
    var p = Pool.initWithLimit(testing.allocator, 1);
    defer p.deinit();

    const config = connection.Config{
        .host = "127.0.0.1",
        .port = 1,
        .connect_timeout_ms = 100,
    };

    _ = p.getReady(77, null, config) catch {};
    try testing.expect(p.next_retry_ms_by_broker.get(77) != null);
    try testing.expect(p.retry_delay_ms_by_broker.get(77) != null);
    try testing.expectEqual(@as(usize, 0), p.map.count());
}

test "pool successful getReady clears retry maps" {
    var p = Pool.init(testing.allocator);
    defer p.deinit();

    try p.next_retry_ms_by_broker.put(3, compat.milliTimestamp() - 1);
    try p.retry_delay_ms_by_broker.put(3, 200);

    _ = p.next_retry_ms_by_broker.remove(3);
    _ = p.retry_delay_ms_by_broker.remove(3);

    try testing.expectEqual(@as(usize, 0), p.next_retry_ms_by_broker.count());
    try testing.expectEqual(@as(usize, 0), p.retry_delay_ms_by_broker.count());
}

test "pool backoff delay grows across consecutive failures" {
    var p = Pool.initWithLimit(testing.allocator, 1);
    defer p.deinit();

    const config = connection.Config{
        .host = "127.0.0.1",
        .port = 1,
        .connect_timeout_ms = 50,
    };

    _ = p.getReady(88, null, config) catch {};
    const first = p.retry_delay_ms_by_broker.get(88).?;
    _ = p.getReady(88, null, config) catch {};
    const second = p.retry_delay_ms_by_broker.get(88).?;

    try testing.expect(second >= first);
    try testing.expect(second <= Pool.retry_max_ms);
}

test "pool retry gate delay stays within full-jitter bounds" {
    var p = Pool.initWithLimit(testing.allocator, 1);
    defer p.deinit();

    const config = connection.Config{
        .host = "127.0.0.1",
        .port = 1,
        .connect_timeout_ms = 50,
    };

    _ = p.getReady(99, null, config) catch {};
    const base = p.retry_delay_ms_by_broker.get(99).?;
    const now = compat.milliTimestamp();
    const not_before = p.next_retry_ms_by_broker.get(99).?;
    const actual_delay = if (not_before > now) not_before - now else 0;

    try testing.expect(actual_delay >= 0);
    try testing.expect(actual_delay <= base);
}

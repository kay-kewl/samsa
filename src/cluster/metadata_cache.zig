const std = @import("std");
const metadata = @import("../generated/metadata.zig");
const model = @import("model.zig");

pub const Cache = struct {
    allocator: std.mem.Allocator,
    brokers: std.AutoHashMap(i32, model.Broker),
    leaders: std.StringHashMap(std.AutoHashMap(i32, i32)),

    pub fn init(allocator: std.mem.Allocator) Cache {
        return .{
            .allocator = allocator,
            .brokers = std.AutoHashMap(i32, model.Broker).init(allocator),
            .leaders = std.StringHashMap(std.AutoHashMap(i32, i32)).init(allocator),
        };
    }

    pub fn deinit(self: *Cache) void {
        var it = self.leaders.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
            self.allocator.free(entry.key_ptr.*);
        }

        self.leaders.deinit();
        self.brokers.deinit();
    }

    pub fn clear(self: *Cache) void {
        var it = self.leaders.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
            self.allocator.free(entry.key_ptr.*);
        }

        self.leaders.clearRetainingCapacity();
        self.brokers.clearRetainingCapacity();
    }

    pub fn apply(self: *Cache, response: metadata.Response) !void {
        self.clear();

        for (response.brokers) |b| {
            if (b.port <= 0 or b.port > std.math.maxInt(u16)) {
                continue;
            }

            try self.brokers.put(b.node_id, .{
                .node_id = b.node_id,
                .host = b.host,
                .port = @intCast(b.port),
            });
        }

        for (response.topics) |t| {
            if (t.error_code != 0) {
                continue;
            }

            const topic_name = t.name orelse continue;
            const name_copy = try self.allocator.dupe(u8, topic_name);
            var part_map = std.AutoHashMap(i32, i32).init(self.allocator);
            errdefer part_map.deinit();

            for (t.partitions) |p| {
                if (p.error_code != 0 or p.leader_id < 0) {
                    continue;
                }

                try part_map.put(p.partition_index, p.leader_id);
            }

            if (part_map.count() == 0) {
                part_map.deinit();
                self.allocator.free(name_copy);
                continue;
            }

            if (self.leaders.fetchPut(name_copy, part_map)) |old| {
                old.value.deinit();
                self.allocator.free(old.key);
            }

            try self.leaders.put(name_copy, part_map);
        }
    }
};

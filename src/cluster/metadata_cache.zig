const std = @import("std");
const model = @import("model.zig");
const metadata = @import("../generated/metadata.zig");
const types = @import("../protocol/types.zig");

pub const Cache = struct {
    allocator: std.mem.Allocator,
    brokers: std.AutoHashMap(i32, model.Broker),
    leaders: std.StringHashMap(std.AutoHashMap(i32, i32)),
    partition_state: std.StringHashMap(std.AutoHashMap(i32, model.PartitionState)),
    topic_ids: std.StringHashMap(types.Uuid),
    topic_generations: std.StringHashMap(u64),
    cluster_id: ?[]u8 = null,
    controller_id: i32 = -1,

    pub fn init(allocator: std.mem.Allocator) Cache {
        return .{
            .allocator = allocator,
            .brokers = std.AutoHashMap(i32, model.Broker).init(allocator),
            .leaders = std.StringHashMap(std.AutoHashMap(i32, i32)).init(allocator),
            .partition_state = std.StringHashMap(std.AutoHashMap(i32, model.PartitionState)).init(allocator),
            .topic_ids = std.StringHashMap(types.Uuid).init(allocator),
            .topic_generations = std.StringHashMap(u64).init(allocator),
            .cluster_id = null,
            .controller_id = -1,
        };
    }

    pub fn deinitPartitionStateMap(self: *Cache, map: *std.AutoHashMap(i32, model.PartitionState)) void {
        var it = map.iterator();
        while (it.next()) |entry| {
            const ps = entry.value_ptr.*;
            self.allocator.free(ps.replica_ids);
            self.allocator.free(ps.isr_ids);
            self.allocator.free(ps.offline_replica_ids);
        }

        map.deinit();
    }

    pub fn deinit(self: *Cache) void {
        {
            var it = self.leaders.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit();
                self.allocator.free(entry.key_ptr.*);
            }
        }

        {
            var ps_it = self.partition_state.iterator();
            while (ps_it.next()) |entry| {
                self.deinitPartitionStateMap(entry.value_ptr);
                self.allocator.free(entry.key_ptr.*);
            }
        }

        {
            var tid_it = self.topic_ids.iterator();
            while (tid_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
        }

        if (self.cluster_id) |cid| {
            self.allocator.free(cid);
            self.cluster_id = null;
        }

        self.freeOwnedBrokerHosts();

        {
            var gen_it = self.topic_generations.iterator();
            while (gen_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
        }

        self.topic_generations.deinit();
        self.topic_ids.deinit();
        self.partition_state.deinit();
        self.leaders.deinit();
        self.brokers.deinit();
    }

    pub fn clear(self: *Cache) void {
        {
            var it = self.leaders.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit();
                self.allocator.free(entry.key_ptr.*);
            }
        }

        {
            var ps_it = self.partition_state.iterator();
            while (ps_it.next()) |entry| {
                self.deinitPartitionStateMap(entry.value_ptr);
                self.allocator.free(entry.key_ptr.*);
            }
        }

        {
            var tid_it = self.topic_ids.iterator();
            while (tid_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
        }

        if (self.cluster_id) |cid| {
            self.allocator.free(cid);
            self.cluster_id = null;
        }
        self.controller_id = -1;

        self.freeOwnedBrokerHosts();
        {
            var gen_it = self.topic_generations.iterator();
            while (gen_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
        }

        self.topic_generations.clearRetainingCapacity();
        self.topic_ids.clearRetainingCapacity();
        self.partition_state.clearRetainingCapacity();
        self.leaders.clearRetainingCapacity();
        self.brokers.clearRetainingCapacity();
    }

    pub fn apply(self: *Cache, response: metadata.Response) !void {
        self.clear();

        self.controller_id = response.controller_id;
        if (response.cluster_id) |cid| {
            self.cluster_id = try self.allocator.dupe(u8, cid);
        }

        for (response.brokers) |b| {
            try self.upsertOwnedBroker(b);
        }

        for (response.topics) |t| {
            if (t.error_code != 0) {
                continue;
            }

            const topic_name = t.name orelse continue;
            const leader_name = try self.allocator.dupe(u8, topic_name);
            const state_name = try self.allocator.dupe(u8, topic_name);
            const id_name = try self.allocator.dupe(u8, topic_name);

            var leader_map = std.AutoHashMap(i32, i32).init(self.allocator);
            errdefer leader_map.deinit();

            var state_map = std.AutoHashMap(i32, model.PartitionState).init(self.allocator);
            errdefer self.deinitPartitionStateMap(&state_map);

            for (t.partitions) |p| {
                const leader_opt: ?i32 = if (p.leader_id < 0) null else p.leader_id;
                const epoch_opt: ?i32 = if (p.leader_epoch < 0) null else p.leader_epoch;

                const replicas = try self.allocator.dupe(i32, p.replica_nodes);
                const isr = try self.allocator.dupe(i32, p.isr_nodes);
                const offline = try self.allocator.dupe(i32, p.offline_replicas);

                errdefer self.allocator.free(replicas);
                errdefer self.allocator.free(isr);
                errdefer self.allocator.free(offline);

                try state_map.put(p.partition_index, .{
                    .error_code = p.error_code,
                    .leader_id = leader_opt,
                    .leader_epoch = epoch_opt,
                    .replica_ids = replicas,
                    .isr_ids = isr,
                    .offline_replica_ids = offline,
                });

                if (p.error_code == 0 and leader_opt != null) {
                    try leader_map.put(p.partition_index, leader_opt.?);
                }
            }

            if (try self.leaders.fetchPut(leader_name, leader_map)) |old| {
                var old_map = old.value;
                old_map.deinit();
                self.allocator.free(old.key);
            }

            if (try self.partition_state.fetchPut(state_name, state_map)) |old| {
                var old_map = old.value;
                self.deinitPartitionStateMap(&old_map);
                self.allocator.free(old.key);
            }

            if (try self.topic_ids.fetchPut(id_name, t.topic_id)) |old| {
                self.allocator.free(old.key);
            }
        }
    }

    fn removeTopic(self: *Cache, topic_name: []const u8) void {
        if (self.leaders.fetchRemove(topic_name)) |old| {
            var old_part_map = old.value;
            old_part_map.deinit();
            self.allocator.free(old.key);
        }

        if (self.partition_state.fetchRemove(topic_name)) |old| {
            var old_ps_map = old.value;
            self.deinitPartitionStateMap(&old_ps_map);
            self.allocator.free(old.key);
        }

        if (self.topic_generations.fetchRemove(topic_name)) |old| {
            self.allocator.free(old.key);
        }
    }

    pub fn applyTopicOnly(self: *Cache, response: metadata.Response) !void {
        self.controller_id = response.controller_id;
        if (response.cluster_id) |cid| {
            if (self.cluster_id) |old| {
                self.allocator.free(old);
            }

            self.cluster_id = try self.allocator.dupe(u8, cid);
        }

        for (response.brokers) |b| {
            try self.upsertOwnedBroker(b);
        }

        for (response.topics) |t| {
            const topic_name = t.name orelse continue;

            if (self.topic_ids.get(topic_name)) |old_id| {
                const same_topic_id = std.mem.eql(u8, std.mem.asBytes(&old_id), std.mem.asBytes(&t.topic_id));
                if (!same_topic_id) {
                    self.bumpTopicGeneration(topic_name);
                }
            }

            self.removeTopic(topic_name);

            // if (self.topic_ids.get(topic_name)) |old_id| {
            //     const same_topic_id = std.mem.eql(u8, std.mem.asBytes(&old_id), std.mem.asBytes(&t.topic_id));
            //     if (!same_topic_id) {
            //         self.removeTopic(topic_name);
            //     } else {
            //         self.removeTopic(topic_name);
            //     }
            // } else {
            //     self.removeTopic(topic_name);
            // }

            if (t.error_code != 0) {
                continue;
            }

            const leader_name = try self.allocator.dupe(u8, topic_name);
            const state_name = try self.allocator.dupe(u8, topic_name);
            const id_name = try self.allocator.dupe(u8, topic_name);

            var leader_map = std.AutoHashMap(i32, i32).init(self.allocator);
            errdefer leader_map.deinit();

            var state_map = std.AutoHashMap(i32, model.PartitionState).init(self.allocator);
            errdefer self.deinitPartitionStateMap(&state_map);

            for (t.partitions) |p| {
                const leader_opt: ?i32 = if (p.leader_id < 0) null else p.leader_id;
                const epoch_opt: ?i32 = if (p.leader_epoch < 0) null else p.leader_epoch;

                const replicas = try self.allocator.dupe(i32, p.replica_nodes);
                const isr = try self.allocator.dupe(i32, p.isr_nodes);
                const offline = try self.allocator.dupe(i32, p.offline_replicas);

                errdefer self.allocator.free(replicas);
                errdefer self.allocator.free(isr);
                errdefer self.allocator.free(offline);

                try state_map.put(p.partition_index, .{
                    .error_code = p.error_code,
                    .leader_id = leader_opt,
                    .leader_epoch = epoch_opt,
                    .replica_ids = replicas,
                    .isr_ids = isr,
                    .offline_replica_ids = offline,
                });

                if (p.error_code == 0 and leader_opt != null) {
                    try leader_map.put(p.partition_index, leader_opt.?);
                }
            }

            try self.leaders.put(leader_name, leader_map);
            try self.partition_state.put(state_name, state_map);
            try self.topic_ids.put(id_name, t.topic_id);
        }
    }

    pub fn partitionStateFor(self: *const Cache, topic: []const u8, partition: i32) ?model.PartitionState {
        const by_partition = self.partition_state.get(topic) orelse return null;
        return by_partition.get(partition);
    }

    pub fn leaderEpochFor(self: *const Cache, topic: []const u8, partition: i32) ?i32 {
        const state = self.partitionStateFor(topic, partition) orelse return null;
        return state.leader_epoch;
    }

    pub fn clearLeaderEpoch(self: *Cache, topic: []const u8, partition: i32) bool {
        const by_partition = self.partition_state.getPtr(topic) orelse return false;
        const state = by_partition.getPtr(partition) orelse return false;
        state.leader_epoch = null;
        return true;
    }

    fn freeOwnedBrokerHosts(self: *Cache) void {
        var it = self.brokers.iterator();
        while (it.next()) |entry| {
            const b = entry.value_ptr.*;
            if (b.owns_host) {
                self.allocator.free(b.host);
            }
        }
    }

    fn bumpTopicGeneration(self: *Cache, topic_name: []const u8) !void {
        const next_generation = (self.topic_generations.get(topic_name) orelse 0) + 1;
        const key = try self.allocator.dupe(u8, topic_name);

        if (try self.topic_generations.fetchPut(key, next_generation)) |old| {
            self.allocator.free(old.key);
        }
    }

    pub fn topicGeneration(self: *const Cache, topic_name: []const u8) ?u64 {
        return self.topic_generations.get(topic_name);
    }

    fn upsertOwnedBroker(self: *Cache, b: metadata.Response.MetadataResponseBroker) !void {
        if (b.port <= 0 or b.port > std.math.maxInt(u16)) {
            return;
        }

        const host_copy = try self.allocator.dupe(u8, b.host);
        errdefer self.allocator.free(host_copy);

        if (try self.brokers.fetchPut(b.node_id, .{
            .node_id = b.node_id,
            .host = host_copy,
            .port = @intCast(b.port),
            .owns_host = true,
        })) |old| {
            if (old.value.owns_host) {
                self.allocator.free(old.value.host);
            }
        }
    }
};

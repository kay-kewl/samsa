const std = @import("std");
const builtin = @import("builtin");

var random_counter: std.atomic.Value(u64) = .init(0);

pub fn milliTimestamp() i64 {
    var ts: std.posix.timespec = undefined;
    const rc = std.posix.system.clock_gettime(std.posix.CLOCK.REALTIME, &ts);
    if (std.posix.errno(rc) != .SUCCESS) {
        return 0;
    }

    return @as(i64, @intCast(ts.sec)) * std.time.ms_per_s +
        @divTrunc(@as(i64, @intCast(ts.nsec)), std.time.ns_per_ms);
}

pub fn nanoTimestampMonotonic() u64 {
    var ts: std.posix.timespec = undefined;
    const rc = std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts);
    if (std.posix.errno(rc) != .SUCCESS) {
        return 0;
    }

    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s +
        @as(u64, @intCast(ts.nsec));
}

pub const Timer = struct {
    started_ns: u64,

    pub fn start() !Timer {
        return .{ .started_ns = nanoTimestampMonotonic() };
    }

    pub fn read(self: Timer) u64 {
        const now = nanoTimestampMonotonic();
        return now -| self.started_ns;
    }
};

fn randomSeed() u64 {
    const counter = random_counter.fetchAdd(0x9e37_79b9_7f4a_7c15, .monotonic);
    const now = @as(u64, @bitCast(milliTimestamp()));
    return now ^ counter ^ @as(u64, @intFromPtr(&random_counter));
}

pub fn randomInt(comptime T: type) T {
    var prng = std.Random.DefaultPrng.init(randomSeed());
    return prng.random().int(T);
}

pub fn randomIntRangeAtMost(comptime T: type, at_least: T, at_most: T) T {
    var prng = std.Random.DefaultPrng.init(randomSeed());
    return prng.random().intRangeAtMost(T, at_least, at_most);
}

pub fn hasEnv(comptime name: []const u8) bool {
    if (builtin.is_test) {
        return std.testing.environ.containsConstant(name);
    }

    if (@hasDecl(std.process.Environ.Block, "global")) {
        const env: std.process.Environ = .{ .block = .global };
        return env.containsConstant(name);
    }

    return false;
}

pub fn readFileAlloc(allocator: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer threaded.deinit();

    return std.Io.Dir.cwd().readFileAlloc(threaded.io(), path, allocator, .limited(max_bytes));
}

pub fn makePath(path: []const u8) !void {
    var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer threaded.deinit();

    return std.Io.Dir.cwd().createDirPath(threaded.io(), path);
}

pub fn sleepNs(ns: u64) void {
    var req: std.posix.timespec = .{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    var rem: std.posix.timespec = undefined;

    while (true) {
        const rc = std.posix.system.nanosleep(&req, &rem);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => req = rem,
            else => return,
        }
    }
}

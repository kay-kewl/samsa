const std = @import("std");
const kafka = @import("kafka");

fn usage() noreturn {
    std.debug.print(
        \\Usage:
        \\  zig build -Doptimize=ReleaseFast bench-produce -- <host> <port> <topic> <messages> <record_size> [warmup]
        \\
        \\Example:
        \\  zig build -Doptimize=ReleaseFast bench-produce -- 127.0.0.1 9092 samsa-bench 10000 100 1000
        \\
    , .{});
    std.process.exit(2);
}

fn percentile(sorted: []const u64, pct: usize) u64 {
    if (sorted.len == 0) return 0;
    var index = (sorted.len * pct) / 100;
    if (index >= sorted.len) index = sorted.len - 1;
    return sorted[index];
}

fn sumU64(items: []const u64) u128 {
    var total: u128 = 0;
    for (items) |x| {
        total += x;
    }
    return total;
}

fn printNsAsUs(label: []const u8, ns: u64) void {
    std.debug.print("{s}: {d} us\n", .{ label, ns / 1000 });
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();

    const host = args.next() orelse usage();
    const port_raw = args.next() orelse usage();
    const topic = args.next() orelse usage();
    const messages_raw = args.next() orelse usage();
    const record_size_raw = args.next() orelse usage();
    const warmup_raw = args.next() orelse "1000";

    const port = try std.fmt.parseInt(u16, port_raw, 10);
    const messages = try std.fmt.parseInt(usize, messages_raw, 10);
    const record_size = try std.fmt.parseInt(usize, record_size_raw, 10);
    const warmup = try std.fmt.parseInt(usize, warmup_raw, 10);

    if (messages == 0 or record_size == 0) {
        return error.InvalidArguments;
    }

    const value = try allocator.alloc(u8, record_size);
    defer allocator.free(value);
    @memset(value, 'x');

    var producer = try kafka.client.Producer.init(allocator, .{
        .bootstrap_host = host,
        .bootstrap_port = port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 5000,
        .metadata_ttl_ms = 60_000,
        .max_frame_bytes = 16 * 1024 * 1024,
        .tcp_nodelay = true,
    }, .{
        .acks = .all,
        .request_ms = 5000,
        .retries_max_attempts = 3,
        .max_request_bytes = 4 * 1024 * 1024,
        .max_record_bytes = 4 * 1024 * 1024,
        .allow_auto_topic_creation = false,
    });
    defer producer.deinit();

    std.debug.print("Samsa Producer benchmark\n", .{});
    std.debug.print("broker: {s}:{d}\n", .{ host, port });
    std.debug.print("topic: {s}\n", .{topic});
    std.debug.print("messages: {d}\n", .{messages});
    std.debug.print("record_size_bytes: {d}\n", .{record_size});
    std.debug.print("warmup_messages: {d}\n", .{warmup});
    std.debug.print("acks: all\n", .{});
    std.debug.print("compression: none / uncompressed topic\n\n", .{});

    var i: usize = 0;
    while (i < warmup) : (i += 1) {
        _ = try producer.send(topic, "warmup-key", value);
    }

    var latencies = try allocator.alloc(u64, messages);
    defer allocator.free(latencies);

    var total_timer = try std.time.Timer.start();

    i = 0;
    while (i < messages) : (i += 1) {
        var one = try std.time.Timer.start();
        _ = try producer.send(topic, "bench-key", value);
        latencies[i] = one.read();
    }

    const total_ns = total_timer.read();

    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));

    const total_latency_ns = sumU64(latencies);
    const avg_ns: u64 = @intCast(total_latency_ns / messages);

    const msg_per_sec: u128 = (@as(u128, messages) * std.time.ns_per_s) / @max(total_ns, 1);
    const payload_bytes: u128 = @as(u128, messages) * @as(u128, record_size);
    const payload_kib_sec: u128 = (payload_bytes * std.time.ns_per_s) / @max(total_ns, 1) / 1024;

    const stats = producer.getStatistics();

    std.debug.print("Result\n", .{});
    std.debug.print("total_time_ms: {d}\n", .{total_ns / std.time.ns_per_ms});
    std.debug.print("throughput_msg_sec: {d}\n", .{msg_per_sec});
    std.debug.print("payload_kib_sec: {d}\n", .{payload_kib_sec});
    printNsAsUs("latency_avg", avg_ns);
    printNsAsUs("latency_p50", percentile(latencies, 50));
    printNsAsUs("latency_p95", percentile(latencies, 95));
    printNsAsUs("latency_p99", percentile(latencies, 99));

    std.debug.print("\nProducer statistics\n", .{});
    std.debug.print("produce_calls: {d}\n", .{stats.produce_calls});
    std.debug.print("produce_errors: {d}\n", .{stats.produce_errors});
    std.debug.print("produce_retries: {d}\n", .{stats.produce_retries});
    std.debug.print("connection_drop_events: {d}\n", .{stats.connection_drop_events});
    std.debug.print("retry_exhausted: {d}\n", .{stats.retry_exhausted});
    std.debug.print("bytes_encoded: {d}\n", .{stats.bytes_encoded});
    std.debug.print("bytes_decoded: {d}\n", .{stats.bytes_decoded});
}

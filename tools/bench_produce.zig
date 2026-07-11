const std = @import("std");
const kafka = @import("kafka");
const compat = kafka.compat;

fn usage() noreturn {
    std.debug.print(
        \\Usage:
        \\  zig build -Doptimize=ReleaseFast bench-produce -- <host> <port> <topic> <messages> <record_size> [warmup] [acks] [batch_size]
        \\
        \\Example:
        \\  zig build -Doptimize=ReleaseFast bench-produce -- 127.0.0.1 9092 samsa-bench 10000 100 1000
        \\
        \\acks:
        \\  all | leader | none
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

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();

    _ = args.next();

    const host = args.next() orelse usage();
    const port_raw = args.next() orelse usage();
    const topic = args.next() orelse usage();
    const messages_raw = args.next() orelse usage();
    const record_size_raw = args.next() orelse usage();
    const warmup_raw = args.next() orelse "1000";
    const acks_raw = args.next() orelse "all";
    const batch_size_raw = args.next() orelse "1";

    const port = try std.fmt.parseInt(u16, port_raw, 10);
    const messages = try std.fmt.parseInt(usize, messages_raw, 10);
    const record_size = try std.fmt.parseInt(usize, record_size_raw, 10);
    const warmup = try std.fmt.parseInt(usize, warmup_raw, 10);
    const batch_size = try std.fmt.parseInt(usize, batch_size_raw, 10);

    if (messages == 0 or record_size == 0 or batch_size == 0) {
        return error.InvalidArguments;
    }

    const acks: kafka.client.Acks = if (std.mem.eql(u8, acks_raw, "all"))
        .all
    else if (std.mem.eql(u8, acks_raw, "leader"))
        .leader
    else if (std.mem.eql(u8, acks_raw, "none"))
        .none
    else
        usage();

    const value = try allocator.alloc(u8, record_size);
    defer allocator.free(value);
    @memset(value, 'x');

    const batch_records = try allocator.alloc(kafka.client.ProducerRecord, batch_size);
    defer allocator.free(batch_records);
    for (batch_records) |*record| {
        record.* = .{
            .key = "bench-key",
            .value = value,
        };
    }

    var producer = try kafka.client.Producer.init(allocator, .{
        .bootstrap_host = host,
        .bootstrap_port = port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 5000,
        .metadata_ttl_ms = 60_000,
        .max_frame_bytes = 16 * 1024 * 1024,
        .tcp_nodelay = true,
    }, .{
        .acks = acks,
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
    std.debug.print("acks: {s}\n", .{acks_raw});
    std.debug.print("batch_size: {d}\n", .{batch_size});
    std.debug.print("compression: none / uncompressed topic\n\n", .{});

    var sent: usize = 0;
    while (sent < warmup) {
        const chunk = @min(batch_size, warmup - sent);
        if (chunk == 1) {
            _ = try producer.send(topic, "bench-key", value);
        } else {
            _ = try producer.sendBatch(topic, batch_records[0..chunk]);
        }
        sent += chunk;
    }

    const measured_batches = (messages + batch_size - 1) / batch_size;
    var latencies = try allocator.alloc(u64, measured_batches);
    defer allocator.free(latencies);

    var total_timer = try compat.Timer.start();

    sent = 0;
    var batch_index: usize = 0;
    while (sent < messages) {
        const chunk = @min(batch_size, messages - sent);
        var one = try compat.Timer.start();
        if (chunk == 1) {
            _ = try producer.send(topic, "bench-key", value);
        } else {
            _ = try producer.sendBatch(topic, batch_records[0..chunk]);
        }
        latencies[batch_index] = one.read();
        sent += chunk;
        batch_index += 1;
    }

    const total_ns = total_timer.read();

    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));

    const total_latency_ns = sumU64(latencies);
    const avg_ns: u64 = @intCast(total_latency_ns / measured_batches);

    const msg_per_sec: u128 = (@as(u128, messages) * std.time.ns_per_s) / @max(total_ns, 1);
    const batch_per_sec: u128 = (@as(u128, measured_batches) * std.time.ns_per_s) / @max(total_ns, 1);
    const payload_bytes: u128 = @as(u128, messages) * @as(u128, record_size);
    const payload_kib_sec: u128 = (payload_bytes * std.time.ns_per_s) / @max(total_ns, 1) / 1024;

    const stats = producer.getStatistics();

    std.debug.print("Result\n", .{});
    std.debug.print("total_time_ms: {d}\n", .{total_ns / std.time.ns_per_ms});
    std.debug.print("throughput_msg_sec: {d}\n", .{msg_per_sec});
    std.debug.print("throughput_batch_sec: {d}\n", .{batch_per_sec});
    std.debug.print("payload_kib_sec: {d}\n", .{payload_kib_sec});
    std.debug.print("measured_batches: {d}\n", .{measured_batches});
    std.debug.print("latency_unit: batch_call\n", .{});
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

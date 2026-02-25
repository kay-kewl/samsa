const std = @import("std");
const kafka = @import("kafka");

fn usage() noreturn {
    std.debug.print(
        \\Usage:
        \\  zig build -Doptimize=ReleaseFast bench-consume -- <host> <port> <topic> <expected_records> [poll_ms]
        \\
        \\Example:
        \\  zig build -Doptimize=ReleaseFast bench-consume -- 127.0.0.1 9092 samsa-bench 100000 1000
        \\
    , .{});
    std.process.exit(2);
}

fn recordPayloadBytes(r: kafka.client.Record) usize {
    var total: usize = 0;

    if (r.key) |k| total += k.len;
    if (r.value) |v| total += v.len;

    for (r.headers) |h| {
        total += h.key.len;
        if (h.value) |v| total += v.len;
    }

    return total;
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
    const expected_raw = args.next() orelse usage();
    const poll_ms_raw = args.next() orelse "1000";

    const port = try std.fmt.parseInt(u16, port_raw, 10);
    const expected_records = try std.fmt.parseInt(usize, expected_raw, 10);
    const poll_ms = try std.fmt.parseInt(i32, poll_ms_raw, 10);

    if (expected_records == 0 or poll_ms <= 0) {
        return error.InvalidArguments;
    }

    var consumer = try kafka.client.Consumer.init(allocator, .{
        .bootstrap_host = host,
        .bootstrap_port = port,
        .connect_timeout_ms = 1000,
        .request_timeout_ms = 5000,
        .metadata_ttl_ms = 60_000,
        .max_frame_bytes = 64 * 1024 * 1024,
        .tcp_nodelay = true,
    }, .{
        .start_position = .earliest,
        .request_ms = 5000,
        .fetch_max_bytes = 64 * 1024 * 1024,
        .max_partition_fetch_bytes = 64 * 1024 * 1024,
        .max_poll_records = 100000,
        .max_poll_bytes = 64 * 1024 * 1024,
        .crc_validation_enabled = true,
    });
    defer consumer.deinit();

    try consumer.assign(topic, 0);

    std.debug.print("Samsa Consumer benchmark\n", .{});
    std.debug.print("broker: {s}:{d}\n", .{ host, port });
    std.debug.print("topic: {s}\n", .{topic});
    std.debug.print("partition: 0\n", .{});
    std.debug.print("expected_records: {d}\n", .{expected_records});
    std.debug.print("poll_ms: {d}\n", .{poll_ms});
    std.debug.print("start_position: earliest\n\n", .{});

    var total_records: usize = 0;
    var total_payload_bytes: usize = 0;
    var poll_calls: usize = 0;
    var empty_polls: usize = 0;

    var timer = try std.time.Timer.start();

    while (total_records < expected_records) {
        const records = try consumer.poll(poll_ms);
        const recent = consumer.peekRecentPollErrors();
        if (recent.len > 0) {
            std.debug.print("poll {d}: recent_errors={d}\n", .{ poll_calls, recent.len });

            const limit = @min(recent.len, 5);
            for (recent[0..limit]) |err| {
                std.debug.print(
                    "  source={s} topic={s} partition={d} error_code={d}",
                    .{ @tagName(err.source), err.topic, err.partition, err.error_code },
                );

                if (err.local_kind) |kind| {
                    std.debug.print(" local_kind={s}", .{@tagName(kind)});
                }

                if (err.error_message) |msg| {
                    std.debug.print(" message={s}", .{msg});
                }

                std.debug.print("\n", .{});
            }
        }
        poll_calls += 1;

        if (records.len == 0) {
            empty_polls += 1;
            continue;
        }

        for (records) |r| {
            if (total_records >= expected_records) {
                break;
            }

            total_records += 1;
            total_payload_bytes += recordPayloadBytes(r);
        }
    }

    const total_ns = timer.read();
    const records_per_sec: u128 = (@as(u128, total_records) * std.time.ns_per_s) / @max(total_ns, 1);
    const payload_kib_sec: u128 = (@as(u128, total_payload_bytes) * std.time.ns_per_s) / @max(total_ns, 1) / 1024;

    const stats = consumer.getStatistics();

    std.debug.print("Result\n", .{});
    std.debug.print("total_time_ms: {d}\n", .{total_ns / std.time.ns_per_ms});
    std.debug.print("records_read: {d}\n", .{total_records});
    std.debug.print("payload_bytes_read: {d}\n", .{total_payload_bytes});
    std.debug.print("throughput_records_sec: {d}\n", .{records_per_sec});
    std.debug.print("payload_kib_sec: {d}\n", .{payload_kib_sec});
    std.debug.print("poll_calls_local: {d}\n", .{poll_calls});
    std.debug.print("empty_polls_local: {d}\n", .{empty_polls});

    std.debug.print("\nConsumer statistics\n", .{});
    std.debug.print("poll_calls: {d}\n", .{stats.poll_calls});
    std.debug.print("poll_errors: {d}\n", .{stats.poll_errors});
    std.debug.print("poll_retries: {d}\n", .{stats.poll_retries});
    std.debug.print("empty_polls: {d}\n", .{stats.empty_polls});
    std.debug.print("records_returned: {d}\n", .{stats.records_returned});
    std.debug.print("connection_drop_events: {d}\n", .{stats.connection_drop_events});
    std.debug.print("retry_exhausted: {d}\n", .{stats.retry_exhausted});
    std.debug.print("bytes_encoded: {d}\n", .{stats.bytes_encoded});
    std.debug.print("bytes_decoded: {d}\n", .{stats.bytes_decoded});
    std.debug.print("crc_mismatch_count: {d}\n", .{stats.crc_mismatch_count});
    std.debug.print("record_decode_error_count: {d}\n", .{stats.record_decode_error_count});
}

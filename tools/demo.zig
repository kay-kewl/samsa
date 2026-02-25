const std = @import("std");
const kafka = @import("kafka");

fn usage() noreturn {
    std.debug.print(
        \\Usage:
        \\  zig build demo -- produce <host> <port> <topic> <key> <value>
        \\  zig build demo -- consume <host> <port> <topic> [max_records]
        \\
        \\Examples:
        \\  zig build demo -- produce 127.0.0.1 9092 samsa-demo demo-key hello-from-samsa
        \\  zig build demo -- consume 127.0.0.1 9092 samsa-demo 10
        \\
    , .{});
    std.process.exit(2);
}

fn parsePort(raw: []const u8) !u16 {
    return std.fmt.parseInt(u16, raw, 10);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();

    const mode = args.next() orelse usage();
    const host = args.next() orelse usage();
    const port_raw = args.next() orelse usage();
    const topic = args.next() orelse usage();
    const port = try parsePort(port_raw);

    if (std.mem.eql(u8, mode, "produce")) {
        const key_arg = args.next() orelse usage();
        const value_arg = args.next() orelse usage();

        const key: ?[]const u8 = if (std.mem.eql(u8, key_arg, "-")) null else key_arg;
        const value: ?[]const u8 = if (std.mem.eql(u8, value_arg, "-")) null else value_arg;

        var producer = try kafka.client.Producer.init(allocator, .{
            .bootstrap_host = host,
            .bootstrap_port = port,
            .connect_timeout_ms = 1000,
            .request_timeout_ms = 3000,
            .metadata_ttl_ms = 60_000,
        }, .{
            .request_ms = 3000,
            .retries_max_attempts = 5,
            .allow_auto_topic_creation = false,
        });
        defer producer.deinit();

        std.debug.print("Samsa Producer demo\n", .{});
        std.debug.print("broker: {s}:{d}\n", .{ host, port });
        std.debug.print("topic: {s}\n", .{topic});
        std.debug.print("key: {s}\n", .{key_arg});
        std.debug.print("value: {s}\n", .{value_arg});

        const result = try producer.send(topic, key, value);

        std.debug.print("produce: OK\n", .{});
        std.debug.print("partition: {d}\n", .{result.partition});
        std.debug.print("base_offset: {d}\n", .{result.base_offset});
        std.debug.print("timestamp: {d}\n", .{result.timestamp});
        return;
    }

    if (std.mem.eql(u8, mode, "consume")) {
        const max_records_raw = args.next() orelse "10";
        const max_records = try std.fmt.parseInt(usize, max_records_raw, 10);

        var consumer = try kafka.client.Consumer.init(allocator, .{
            .bootstrap_host = host,
            .bootstrap_port = port,
            .connect_timeout_ms = 1000,
            .request_timeout_ms = 3000,
            .metadata_ttl_ms = 60_000,
        }, .{
            .request_ms = 3000,
            .retries_max_attempts = 5,
            .start_position = .earliest,
            .allow_auto_topic_creation = false,
            .max_poll_records = max_records,
            .max_poll_bytes = 1024 * 1024,
        });
        defer consumer.deinit();

        try consumer.assign(topic, 0);

        std.debug.print("Samsa Consumer demo\n", .{});
        std.debug.print("broker: {s}:{d}\n", .{ host, port });
        std.debug.print("topic: {s}\n", .{topic});
        std.debug.print("partition: 0\n", .{});
        std.debug.print("start_position: earliest\n\n", .{});

        var attempt: u8 = 0;
        while (attempt < 5) : (attempt += 1) {
            const records = try consumer.poll(3000);

            if (records.len == 0) {
                std.debug.print("poll attempt {d}: no records\n", .{attempt + 1});
                continue;
            }

            std.debug.print("poll: OK, records={d}\n\n", .{records.len});

            for (records) |r| {
                std.debug.print("record:\n", .{});
                std.debug.print("  topic: {s}\n", .{r.topic});
                std.debug.print("  partition: {d}\n", .{r.partition});
                std.debug.print("  offset: {d}\n", .{r.offset});
                std.debug.print("  timestamp: {d}\n", .{r.timestamp});

                if (r.key) |k| {
                    std.debug.print("  key: {s}\n", .{k});
                } else {
                    std.debug.print("  key: <null>\n", .{});
                }

                if (r.value) |v| {
                    std.debug.print("  value: {s}\n", .{v});
                } else {
                    std.debug.print("  value: <null>\n", .{});
                }

                std.debug.print("\n", .{});
            }

            return;
        }

        std.debug.print("no records received\n", .{});
        return error.NoRecordsReceived;
    }

    usage();
}

const std = @import("std");
const kafka = @import("kafka");

test "public module loads" {
    _ = kafka;
    try std.testing.expect(true);
}

test "generated module exports expected apis" {
    _ = kafka.generated.api_versions;
    _ = kafka.generated.fetch;
    _ = kafka.generated.list_offsets;
    _ = kafka.generated.metadata;
    _ = kafka.generated.produce;
    try std.testing.expect(true);
}

test {
    _ = @import("generated_roundtrip.zig");
    _ = @import("protocol_negatives.zig");
    _ = @import("transport_smoke.zig");
    _ = @import("transport_connection_state.zig");
    _ = @import("cluster_unit.zig");
    _ = @import("client_unit.zig");
    _ = @import("protocol_fixtures.zig");
}

test "client module exports producer and consumer" {
    _ = kafka.client.Producer;
    _ = kafka.client.Consumer;
    _ = kafka.client.Statistics;
    try std.testing.expect(true);
}

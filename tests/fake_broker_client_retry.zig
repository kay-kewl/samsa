const std = @import("std");
const kafka = @import("kafka");

test "classification: route refresh matrix remains stable for broker topology codes" {
    const route_codes = [_]i16{ 6, 74, 75, 129 };
    for (route_codes) |code| {
        try std.testing.expect(kafka.client.client.isRouteRefreshError(code));
    }

    const non_route_codes = [_]i16{ 1, 7, 10, 19, 20, 56, 42 };
    for (non_route_codes) |code| {
        try std.testing.expect(!kafka.client.client.isRouteRefreshError(code));
    }
}

test "classification: retryable send error includes stale metadata path" {
    try std.testing.expect(kafka.client.client.isRetryableSendError(error.StaleMetadata));
    try std.testing.expect(kafka.client.client.isRetryableSendError(error.Timeout));
    try std.testing.expect(kafka.client.client.isRetryableSendError(error.EndOfStream));
    try std.testing.expect(!kafka.client.client.isRetryableSendError(error.InvalidConfiguration));
}

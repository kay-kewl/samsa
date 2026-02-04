const std = @import("std");
const kafka = @import("kafka");

test "classification: route refresh codes include leader and epoch errors" {
    try std.testing.expect(kafka.client.client.isRouteRefreshError(6));
    try std.testing.expect(kafka.client.client.isRouteRefreshError(74));
    try std.testing.expect(kafka.client.client.isRouteRefreshError(75));
    try std.testing.expect(kafka.client.client.isRouteRefreshError(129));
}

test "classification: retryable send error includes stale metadata path" {
    try std.testing.expect(kafka.client.client.isRetryableSendError(error.StaleMetadata));
    try std.testing.expect(kafka.client.client.isRetryableSendError(error.Timeout));
    try std.testing.expect(!kafka.client.client.isRetryableSendError(error.InvalidConfiguration));
}

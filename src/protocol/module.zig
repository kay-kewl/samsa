pub const codec = @import("codec.zig");
pub const tagged_fields = @import("tagged_fields.zig");
pub const jsonc = @import("jsonc.zig");
pub const limits = @import("limits.zig");
pub const types = @import("types.zig");
pub const header = @import("header.zig");
pub const crc32c = @import("crc32c.zig");
pub const batch = @import("records/batch.zig");

test {
    _ = @import("codec.zig");
    _ = @import("tagged_fields.zig");
    _ = @import("jsonc.zig");
    _ = @import("header.zig");
    _ = @import("crc32c.zig");
    _ = @import("records/batch.zig");
}

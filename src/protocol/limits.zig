pub const Limits = struct {
    max_string_bytes: usize = 1024 * 1024,
    max_bytes_field_bytes: usize = 1024 * 1024,
    max_array_elements: usize = 10000,
    max_tagged_field_bytes: usize = 1024 * 1024,
    decode_depth_max: usize = 64,
};

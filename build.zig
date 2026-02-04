const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const src_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_src_tests = b.addRunArtifact(src_tests);

    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    unit_tests.root_module.addImport("kafka", b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    }));
    const run_unit_tests = b.addRunArtifact(unit_tests);

    const run_unit_tests_golden_strict = b.addRunArtifact(unit_tests);
    run_unit_tests_golden_strict.setEnvironmentVariable("SAMSA_REQUIRE_GOLDEN", "1");

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_src_tests.step);
    test_step.dependOn(&run_unit_tests.step);

    const test_golden_strict_step = b.step("test-golden-strict", "Run protocol golden tests with required fixtures");
    test_golden_strict_step.dependOn(&run_src_tests.step);
    test_golden_strict_step.dependOn(&run_unit_tests_golden_strict.step);

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration_main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    integration_tests.root_module.addImport("kafka", b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    }));
    const run_integration_tests = b.addRunArtifact(integration_tests);
    const integration_step = b.step("test-integration", "Run Docker/Kafka integration tests");
    integration_step.dependOn(&run_integration_tests.step);

    const run_integration_tests_strict = b.addRunArtifact(integration_tests);
    run_integration_tests_strict.setEnvironmentVariable("SAMSA_INTEGRATION_REQUIRED", "1");
    const integration_strict_step = b.step("test-integration-strict", "Run Docker/Kafka integration tests");
    integration_strict_step.dependOn(&run_integration_tests_strict.step);

    const fetch_cmd = b.addSystemCommand(&.{
        "bash",
        "tools/fetch_schemas.sh",
    });
    const fetch_step = b.step("fetch-schemas", "Download pinned Kafka schemas");
    fetch_step.dependOn(&fetch_cmd.step);

    const gen_exe = b.addExecutable(.{ .name = "protocol_generator", .root_module = b.createModule(.{
        .root_source_file = b.path("tools/protocol_generator.zig"),
        .target = target,
        .optimize = optimize,
    }) });

    const jsonc_module = b.createModule(.{
        .root_source_file = b.path("src/protocol/jsonc.zig"),
        .target = target,
        .optimize = optimize,
    });

    gen_exe.root_module.addImport("jsonc", jsonc_module);

    const run_gen = b.addRunArtifact(gen_exe);

    run_gen.addArg("kafka-profile");
    run_gen.addArg("src/generated");

    const gen_step = b.step("gen", "Generate Kafka protocol structs");
    gen_step.dependOn(&run_gen.step);

    const regen_protocol_opt = b.option(bool, "regen-protocol", "Regenerate protocol code before build/test") orelse false;
    if (regen_protocol_opt) {
        test_step.dependOn(&run_gen.step);
        integration_step.dependOn(&run_gen.step);
        integration_strict_step.dependOn(&run_gen.step);
    }
}

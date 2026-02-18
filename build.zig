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

    const run_unit_tests_fake_broker = b.addRunArtifact(unit_tests);
    run_unit_tests_fake_broker.setEnvironmentVariable("SAMSA_FAKE_BROKER_REQUIRED", "1");

    const test_fake_broker_step = b.step("test-fake-broker", "Run scripted fake-broker retry tests");
    test_fake_broker_step.dependOn(&run_src_tests.step);
    test_fake_broker_step.dependOn(&run_unit_tests_fake_broker.step);

    const test_golden_strict_step = b.step("test-golden-strict", "Run protocol golden tests with required fixtures");
    test_golden_strict_step.dependOn(&run_src_tests.step);
    test_golden_strict_step.dependOn(&run_unit_tests_golden_strict.step);

    const run_unit_tests_golden_real = b.addRunArtifact(unit_tests);
    run_unit_tests_golden_real.setEnvironmentVariable("SAMSA_REQUIRE_GOLDEN", "1");
    run_unit_tests_golden_real.setEnvironmentVariable("SAMSA_REQUIRE_REAL_GOLDEN", "1");

    const test_golden_real_step = b.step("test-golden-real", "Run protocol golden tests requiring real captured fixtures");
    test_golden_real_step.dependOn(&run_src_tests.step);
    test_golden_real_step.dependOn(&run_unit_tests_golden_real.step);

    const run_unit_tests_reliability = b.addRunArtifact(unit_tests);
    run_unit_tests_reliability.setEnvironmentVariable("SAMSA_REQUIRE_GOLDEN", "1");
    run_unit_tests_reliability.setEnvironmentVariable("SAMSA_REQUIRE_REAL_GOLDEN", "1");
    run_unit_tests_reliability.setEnvironmentVariable("SAMSA_REQUIRE_EXACT_RESPONSE_GOLDEN", "1");
    run_unit_tests_reliability.setEnvironmentVariable("SAMSA_FAKE_BROKER_REQUIRED", "1");

    const test_reliability_step = b.step("test-reliability", "Run strict reliability gate");
    test_reliability_step.dependOn(&run_src_tests.step);
    test_reliability_step.dependOn(&run_unit_tests_reliability.step);

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

    const run_integration_tests_multi = b.addRunArtifact(integration_tests);
    run_integration_tests_multi.setEnvironmentVariable("SAMSA_INTEGRATION_REQUIRED", "1");
    run_integration_tests_multi.setEnvironmentVariable("SAMSA_MULTI_BROKER_REQUIRED", "1");

    const integration_multi_step = b.step("test-integration-multi", "Run multi-broker Docker/Kafka integration tests");
    integration_multi_step.dependOn(&run_integration_tests_multi.step);

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

    const run_gen_check = b.addRunArtifact(gen_exe);
    run_gen_check.addArg("--check");
    run_gen_check.addArg("kafka-profile");
    run_gen_check.addArg("src/generated");

    const gen_check_step = b.step("gen-check", "Verify generated protocol code is up to date");
    gen_check_step.dependOn(&run_gen_check.step);
    test_reliability_step.dependOn(&run_gen_check.step);

    const golden_bootstrap_exe = b.addExecutable(
        .{
            .name = "bootstrap_golden_fixtures",
            .root_module = b.createModule(
                .{
                    .root_source_file = b.path("tools/bootstrap_golden_fixtures.zig"),
                    .target = target,
                    .optimize = optimize,
                },
            ),
        },
    );
    golden_bootstrap_exe.root_module.addImport("kafka", b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    }));
    const run_golden_bootstrap = b.addRunArtifact(golden_bootstrap_exe);
    const golden_bootstrap_step = b.step("gen-golden-fixtures", "Generate bootstrap protocol golden fixtures");
    golden_bootstrap_step.dependOn(&run_golden_bootstrap.step);

    const regen_protocol_opt = b.option(bool, "regen-protocol", "Regenerate protocol code before build/test") orelse false;
    if (regen_protocol_opt) {
        test_step.dependOn(&run_gen.step);
        test_golden_strict_step.dependOn(&run_gen.step);
        integration_step.dependOn(&run_gen.step);
        integration_strict_step.dependOn(&run_gen.step);
        integration_multi_step.dependOn(&run_gen.step);
    }
}

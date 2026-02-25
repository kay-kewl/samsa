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
    run_unit_tests_reliability.setEnvironmentVariable("SAMSA_REQUIRE_POLICY_STRICT", "1");

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
    const integration_strict_step = b.step("test-integration-strict", "Run required signle-broker Docker/Kafka integration tests");
    integration_strict_step.dependOn(&run_integration_tests_strict.step);

    const run_integration_tests_multi = b.addRunArtifact(integration_tests);
    run_integration_tests_multi.setEnvironmentVariable("SAMSA_INTEGRATION_REQUIRED", "1");
    run_integration_tests_multi.setEnvironmentVariable("SAMSA_MULTI_BROKER_REQUIRED", "1");

    const integration_multi_step = b.step("test-integration-multi", "Run required multi-broker Docker/Kafka integration tests");
    integration_multi_step.dependOn(&run_integration_tests_multi.step);

    const release_step = b.step("test-release", "Run release gate");
    release_step.dependOn(test_reliability_step);
    release_step.dependOn(integration_strict_step);

    const release_full_step = b.step("test-release-full", "Run full release gate");
    release_full_step.dependOn(release_step);
    release_full_step.dependOn(integration_multi_step);

    const fetch_schemas_exe = b.addExecutable(.{
        .name = "fetch_schemas",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/fetch_schemas.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_fetch_schemas = b.addRunArtifact(fetch_schemas_exe);
    const fetch_step = b.step("fetch-schemas", "Download pinned Kafka schemas");
    fetch_step.dependOn(&run_fetch_schemas.step);

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

    const demo_exe = b.addExecutable(.{ .name = "samsa_demo", .root_module = b.createModule(.{
        .root_source_file = b.path("tools/demo.zig"),
        .target = target,
        .optimize = optimize,
    }) });
    demo_exe.root_module.addImport("kafka", b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    }));

    const run_demo = b.addRunArtifact(demo_exe);
    if (b.args) |args| {
        run_demo.addArgs(args);
    }

    const demo_step = b.step("demo", "Run samsa demo producer and consumer");
    demo_step.dependOn(&run_demo.step);

    const bench_produce_exe = b.addExecutable(.{
        .name = "samsa_bench_produce",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/bench_produce.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    bench_produce_exe.root_module.addImport("kafka", b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    }));

    const run_bench_produce = b.addRunArtifact(bench_produce_exe);
    if (b.args) |args| {
        run_bench_produce.addArgs(args);
    }

    const bench_produce_step = b.step("bench-produce", "Run Samsa producer benchmark");
    bench_produce_step.dependOn(&run_bench_produce.step);

    const bench_consume_exe = b.addExecutable(.{
        .name = "samsa_bench_consume",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/bench_consume.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    bench_consume_exe.root_module.addImport("kafka", b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    }));

    const run_bench_consume = b.addRunArtifact(bench_consume_exe);
    if (b.args) |args| {
        run_bench_consume.addArgs(args);
    }

    const bench_consume_step = b.step("bench-consume", "Run Samsa consumer benchmark");
    bench_consume_step.dependOn(&run_bench_consume.step);
}

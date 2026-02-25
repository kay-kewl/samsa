# Samsa — Zig-native Kafka Implementation

Samsa is an experimental Kafka client implementation written in Zig. It is motivated by the absence of a mature native Zig Kafka client comparable to official clients in other ecosystems.

The project focuses on a native Zig implementation of the Kafka wire protocol and a minimal Producer and Consumer runtime for Apache Kafka 4.0.1. The current version is not intended to replace mature production Kafka clients. It is a correctness-first implementation with protocol generation, runtime API negotiation, metadata routing, produce and fetch support, record batch parsing, and integration tests against real Kafka brokers.

## Project goals

The main goal of Samsa is to implement the core of a Kafka client in Zig:

- generate Kafka protocol request and response structs from official Kafka schemas;
- connect to Kafka 4.0.1 using the binary Kafka protocol;
- negotiate supported API versions at runtime;
- fetch cluster metadata and route requests to partition leaders;
- produce records to Kafka topics;
- consume uncompressed record batches from Kafka topics;
- validate record batch CRC32C;
- test protocol correctness using golden fixtures, fake broker tests, and Docker-based integration tests.

## Current scope

Kafka target:

- Apache Kafka 4.0.1
- KRaft mode, no ZooKeeper dependency

APIs currently used in runtime negotiation:

- `ApiVersions`: v4, fallback to v2, with fallback body decode as v0
- `Metadata`: v12
- `Produce`: v12
- `Fetch`: v12
- `ListOffsets`: v10

Current client features:

- TCP transport with Kafka frame encoding and decoding
- request and response correlation id validation
- metadata cache and partition leader routing
- bootstrap endpoint fallback
- per-broker connection pool
- retry and backoff paths for selected transport and broker errors
- Producer with `acks=none`, `acks=leader`, `acks=all`
- manual Consumer assignment by topic and partition
- earliest and latest initial offset resolution through `ListOffsets`
- uncompressed Kafka record batch parsing
- CRC32C validation for record batches
- local statistics for Producer, Consumer, Cluster, and Connection

Current limitations:

- Producer and Consumer are not thread-safe
- Producer currently sends records synchronously
- no producer batching yet
- no async pipeline or multiple in-flight requests yet
- no compression support yet
- Consumer currently supports uncompressed record batches only
- no consumer groups yet
- no offset commit API yet
- no idempotent producer or transactions yet
- no SASL/SSL support yet

## Repository layout

```text
src/
  protocol/                     Kafka protocol primitives
  generated/                    Generated Kafka API structs
  transport/                    TCP framing, connection state, connection pool
  cluster/                      Metadata cache, version registry, broker routing
  client/                       Public Producer and Consumer API

tools/
  fetch_schemas.sh              Downloads pinned Kafka 4.0.1 schemas
  protocol_generator.zig        Generates Zig structs from Kafka schemas
  bootstrap_golden_fixtures.zig Generates bootstrap protocol fixtures

kafka-profile/
  manifest.json                 Pinned schema checksums

tests/
  protocol_*                    Protocol/golden/negative tests
  fake_broker_*                 Scripted fake broker tests
  cluster_unit.zig              Cluster and metadata cache tests
  client_unit.zig               Client API tests
  integration_main.zig          Docker/Kafka integration tests

docker-compose.kafka.yml        Single-broker Kafka 4.0.1 setup
docker-compose.kafka.multi.yml  Multi-broker Kafka 4.0.1 setup
````

## Requirements

* Zig 0.15.2
* Docker and Docker Compose
* Bash, curl, jq, sha256sum for schema fetching

## Quick start

Fetch official Kafka 4.0.1 schemas and generate Zig protocol files:

```bash
tools/fetch_schemas.sh
zig build gen
zig build gen-golden-fixtures
```

Run unit and protocol tests:

```bash
zig build test
zig build test-golden-strict
zig build test-fake-broker
zig build test-reliability
```

Run single-broker integration tests:

```bash
docker compose -f docker-compose.kafka.yml up -d
zig build test-integration
docker compose -f docker-compose.kafka.yml down -v
```

Run multi-broker integration tests:

```bash
docker compose -f docker-compose.kafka.multi.yml up -d
zig build test-integration-multi
docker compose -f docker-compose.kafka.multi.yml down -v
```

## Build steps

The project defines several Zig build steps:

```bash
zig build test
```

Runs unit tests and module tests.

```bash
zig build test-golden-strict
```

Runs protocol golden tests and requires protocol fixtures.

```bash
zig build test-fake-broker
```

Runs scripted fake broker tests.

```bash
zig build test-reliability
```

Runs the strict reliability gate: generated code check, golden checks, exact response checks, fake broker tests, and strict policy tests.

```bash
zig build test-integration
```

Runs Docker and Kafka integration tests for a single broker.

```bash
zig build test-integration-strict
```

Runs integration tests with required infrastructure mode enabled.

```bash
zig build test-integration-multi
```

Runs multi-broker integration tests.

```bash
zig build test-release
```

Runs the release gate.

```bash
zig build test-release-full
```

Runs the full release gate, including multi-broker integration tests.

## Basic Producer example

```zig
const std = @import("std");
const kafka = @import("kafka");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var producer = try kafka.client.Producer.init(
        allocator,
        .{
            .bootstrap_host = "127.0.0.1",
            .bootstrap_port = 9092,
            .tcp_nodelay = true,
        },
        .{
            .acks = .all,
            .retries_max_attempts = 5,
        },
    );
    defer producer.deinit();

    const result = try producer.send(
        "events",
        "demo-key",
        "hello-from-samsa",
    );

    std.debug.print(
        "sent topic={s} partition={d} offset={d}\n",
        .{ result.topic, result.partition, result.base_offset },
    );
}
```

## Basic Consumer example

```zig
const std = @import("std");
const kafka = @import("kafka");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var consumer = try kafka.client.Consumer.init(
        allocator,
        .{
            .bootstrap_host = "127.0.0.1",
            .bootstrap_port = 9092,
            .tcp_nodelay = true,
        },
        .{
            .start_position = .earliest,
            .crc_validation_enabled = true,
        },
    );
    defer consumer.deinit();

    try consumer.assign("events", 0);

    const records = try consumer.poll(1000);

    for (records) |record| {
        std.debug.print(
            "record topic={s} partition={d} offset={d} key={?s} value={?s}\n",
            .{
                record.topic,
                record.partition,
                record.offset,
                record.key,
                record.value,
            },
        );
    }
}
```

## Manual interoperability demo

Start Kafka:

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Create an uncompressed topic:

```bash
export TOPIC=samsa-demo-$(date +%s)

docker exec samsa-kafka-4-0-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1 \
  --config compression.type=uncompressed
```

Produce with Samsa and consume with the official Kafka CLI:

```bash
zig build demo -- produce 127.0.0.1 9092 "$TOPIC" demo-key hello-from-samsa

docker exec samsa-kafka-4-0-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages 1 \
  --property print.key=true \
  --property key.separator=" = "
```

Produce with the official Kafka CLI and consume with Samsa:

```bash
export TOPIC2=samsa-demo-consume-$(date +%s)

docker exec samsa-kafka-4-0-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC2" \
  --partitions 1 \
  --replication-factor 1 \
  --config compression.type=uncompressed

echo "cli-key:hello-from-kafka-cli" | docker exec -i samsa-kafka-4-0-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC2" \
  --property parse.key=true \
  --property key.separator=":" \
  --producer-property compression.type=none

zig build demo -- consume 127.0.0.1 9092 "$TOPIC2" 10
```

Stop Kafka:

```bash
docker compose -f docker-compose.kafka.yml down -v
```

## Benchmark notes

The benchmark results below were collected on a local Kafka 4.0.1 broker running in Docker. They should be treated as a basic performance sanity check.

Samsa Producer, synchronous mode, no batching:

```text
messages: 10000
record_size_bytes: 100
acks: all
compression: none / uncompressed topic

throughput_msg_sec: 2482
payload_kib_sec: 242
latency_avg: 402 us
latency_p50: 256 us
latency_p95: 1054 us
latency_p99: 1242 us
produce_errors: 0
produce_retries: 0
```

Samsa Consumer, one partition, uncompressed batches:

```text
records_read: 100000
payload_bytes_read: 10000000
total_time_ms: 3204
throughput_records_sec: 31206
payload_kib_sec: 3047
poll_calls: 1
poll_errors: 0
poll_retries: 0
crc_mismatch_count: 0
record_decode_error_count: 0
```

Official Kafka tools on the same local setup:

```text
Official Kafka producer perf test:
100000 records sent
151975.7 records/sec
14.49 MB/sec

Official Kafka consumer perf test:
100000 records consumed
207039.3 records/sec
19.74 MB/sec
```

Interpretation:

* The official Producer is much faster in throughput because it uses mature batching, buffering, and pipelining.
* Samsa Producer currently sends records synchronously.
* The official Consumer is faster because it is a mature optimized implementation for high-throughput reads.
* Samsa Consumer fully parses record batches, validates CRC32C, materializes records, and returns them through a simple safe API.
* The current benchmark confirms basic correctness and baseline performance, not production-level optimization.

## Testing strategy

Samsa uses several layers of tests:

1. Unit tests for codec, headers, tagged fields, record batches, routing, metadata cache, and client configuration.
2. Generated protocol roundtrip tests.
3. Golden fixture tests for Kafka request/response binary compatibility.
4. Negative protocol tests for truncation, invalid tags, duplicate tags, oversized tagged fields, and invalid fallback shapes.
5. Scripted fake broker tests for retry and broker error behavior.
6. Docker integration tests against Apache Kafka 4.0.1.
7. Multi-broker Docker tests for basic failover scenarios.

## Design overview

Samsa is split into five main layers:

```text
Public API
  Producer and Consumer

Client layer
  retries, record batch handling, poll and send APIs

Cluster layer
  metadata cache, broker routing, API version registry

Transport layer
  TCP connection, Kafka frame read/write, connection pool

Protocol layer
  codec, headers, tagged fields, generated request and response structs
```

The protocol layer is generated from official Kafka JSON schemas. Runtime code selects supported API versions through `ApiVersions` and then uses the generated request and response structs for the selected versions.

## Future work

Planned areas for future development:

* Producer batching by topic-partition
* internal producer queue
* linger/batch-size based flushing
* async pipeline and multiple in-flight requests
* compression support: gzip, lz4, zstd, snappy
* consumer groups: JoinGroup, SyncGroup, Heartbeat, OffsetCommit, OffsetFetch
* zero-copy or near-zero-copy Consumer API
* streaming/iterator-based poll API
* fetch sessions
* idempotent producer
* transactions
* SASL/SSL support
* additional Kafka APIs and protocol versions
* broader performance benchmarking

## License

See `LICENSE`.


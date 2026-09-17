# Kafka Producer / Consumer / Streams Plan

CS-E4780 course project — Detecting Trading Trends in Financial Tick Data.

This document describes the plan for adding the Kafka layer on top of the
existing ingestion prototype, and comments on how the architecture sketched in
`plan.png` should evolve to satisfy the assignment.

## Context

The repo already has a Scala 3 / sbt ingestion prototype
(`src/ingestion/main/scala/IngestionApp.scala`) that reads the DEBS 2022 trading
CSV and writes newline-delimited JSON (NDJSON) to `/output/events.ndjson`, one
object per line:

```json
{"symbol":"RDSA.NL","securityType":"E","price":123.45,"timestamp":"2021-11-08T01:01:07.000"}
```

`INGESTION.md` states that NDJSON is "the local handoff boundary for a future
Kafka sink" — that seam is what this step builds on. There is currently **no
Kafka anywhere**: no broker in `docker-compose.yml`, no `org.apache.kafka`
dependencies in `build.sbt`.

The goal is to stand up the three pieces sketched in `plan.png` as a working
end-to-end skeleton:

1. **Producer** — reads the ingestion NDJSON and publishes to a Kafka topic.
2. **Consumer** — subscribes to a topic and prints events (a "fake consumer" for
   testing, per the plan.png TODO list).
3. **Kafka Streams** app — consumes, adds one field with the value
   `"processed by kafka streams"`, and re-emits. This is the deliberate
   placeholder for the real Query 1 (EMA) and Query 2 (crossover advisories)
   logic in the final project.

Design decisions: **all three components in Scala** (uniform JVM stack; Kafka
Streams is JVM-only anyway) and a **single topic keyed by symbol** (not
one-topic-per-symbol).

## Target architecture

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for Mermaid diagrams (data flow,
Compose deployment, build flow, module dependencies).

```
CSV ──▶ ingestion ──▶ events.ndjson ──▶ producer ──▶ [trading-events] ──▶ streams
                                                                              │
                                                                              ▼
        consumer / UI ◀────────────────────── [trading-events-processed] ◀───┘
```

- **Topics**: `trading-events` (raw) and `trading-events-processed` (after the
  streams marker). A future `advisories` topic carries buy/sell events.
- **Keying**: message key = `symbol`, so all events for a symbol land on one
  partition. This preserves per-symbol ordering and enables per-symbol stateful
  windowing downstream.

## Module layout (multi-module sbt build)

The current single-project build becomes an aggregate with reusable modules:

- `common` (`src/common/main/scala`) — the shared `Event` model (moved from
  ingestion) plus an `EventJson` codec (`render`, `parse`, `symbolOf`) reusing
  `ujson`. Depended on by all other modules.
- `ingestion` — existing app, now `.dependsOn(common)`.
- `producer` (`src/producer/main/scala`) — `kafka-clients`.
- `consumer` (`src/consumer/main/scala`) — `kafka-clients`.
- `streams` (`src/streams/main/scala`) — `kafka-streams` (Java API).
- `root` — aggregates all of the above.

**Version caveat:** `kafka-streams-scala` is only published for Scala 2.13, so
the streams app uses the **Java Kafka Streams API directly from Scala 3**
(`StreamsBuilder`, `KStream`, `Produced`, …). Kafka pinned to a current 3.x for
both `kafka-clients` and `kafka-streams`.

### Producer
Reads the NDJSON file line by line (bounded memory; supports `-` for stdin so it
can be piped from ingestion later). For each line it sends a `ProducerRecord`
with `key = symbol` and `value =` the raw NDJSON line forwarded verbatim, using
`StringSerializer` for both.

### Consumer
Subscribes to `trading-events-processed` with `auto.offset.reset=earliest` and
prints each record (`key -> value`). This is the test/demo consumer.

### Streams
Topology: `stream(trading-events) → mapValues(addMarker) → to(trading-events-processed)`.
`addMarker` is a pure, unit-testable function that adds
`"processedBy":"processed by kafka streams"` and preserves the four original
fields. This stateless `mapValues` is the explicit seam that later becomes the
EMA windowing and crossover logic.

### Docker
A single-node Apache Kafka service in **KRaft mode** (no ZooKeeper) with topic
auto-create enabled for the demo. The `Dockerfile` is generalized with
`ARG MODULE` / `ARG MAIN_CLASS` so one image definition builds all four app
jars; `producer`/`streams`/`consumer` services join the existing `ingestion`
service, with the producer mounting the shared `./output` volume.

## Running the pipeline

### With Docker Compose (recommended)

Everything runs as containers on one Compose network. App images copy in the fat
jars, so build the jars first.

```bash
# 1. Build the app jars (the Docker images COPY these in)
sbt ingestion/assembly producer/assembly consumer/assembly streams/assembly

# 2. Start the broker, the stream processor and the consumer
docker compose up -d --build kafka streams consumer

# 3. Generate NDJSON into ./output (needs the DEBS CSV in ./data — see
#    scripts/download-data.sh). Skip if ./output/events.ndjson already exists.
docker compose run --rm ingestion

# 4. Publish the NDJSON to the trading-events topic
docker compose run --rm producer

# 5. Watch the processed output
docker compose logs -f consumer
```

Topics are auto-created on first use. Inside the Compose network the apps reach
the broker at `kafka:19092`; from the host it's `localhost:9092`.

### Locally with sbt

The apps default to `localhost:9092`, so with only the broker in Docker
(`docker compose up -d kafka`) you can run any component from the host. Quote the
task so its arguments attach to `run`, and pass a path you can read (the
container-written `./output` is root-owned):

```bash
sbt streams/run                              # stream processor (stays running)
sbt "producer/run samples/events.ndjson"     # publish
sbt consumer/run                             # read processed output
```

### Consumer log levels

`CONSUMER_LOG_LEVEL` controls verbosity (default `SUMMARY`):

- `ALL` — print every record as `key -> value`. Use this to eyeball that the
  `processedBy` field is present.
- `SUMMARY` — periodic throughput stats only (`consumed`, `recordsPerSecond`,
  `avgRecordsPerSecond`), mirroring the ingestion app's rows/second logging.
  Cadence set by `CONSUMER_SUMMARY_INTERVAL_MS` (default 2000). A final
  `[consumer] closed ...` line prints in both modes on shutdown.

```bash
# Docker: flip the level for a one-off run
docker compose run --rm -e CONSUMER_LOG_LEVEL=ALL consumer
# Local:
CONSUMER_LOG_LEVEL=ALL sbt consumer/run
```

### Resetting topics

`scripts/reset-topics.sh` stops streams (container **and** any stray local
`sbt streams/run`), deletes and recreates the data topics (plus any
`trading-streams-*` internal topics), then restarts streams as a container. Pass
`--build` to rebuild the streams jar + image first (needed after changing streams
code). Tunables: `PARTITIONS` (default 6), `KAFKA_SVC`, `STREAMS_SVC`,
`BOOTSTRAP`, `APP_ID`.

```bash
scripts/reset-topics.sh --build
```

## plan.png → assignment: how the design should evolve

- **"Kafka Connector" box → a plain producer.** Kafka Connect is operational
  overkill for this project; a producer keyed by symbol is enough and is what we
  build. Connect can be noted as an alternative in the report.
- **"one topic per symbol?" → single topic partitioned by symbol key.** 5504
  topics is heavy broker overhead and awkward for Streams. Keying by symbol on a
  single topic gives the same per-symbol grouping (assignment relaxation 2) with
  clean scaling. This directly answers the diagram's open question.
- **"queue? / event processor" box = the Kafka Streams app.** Today it only
  stamps the marker field. To satisfy the assignment it must grow to:
  - **Event time, not processing time** — a `TimestampExtractor` over the
    `timestamp` field, with windows aligned so `w0` starts at 00:00 CEST.
  - **Query 1 (EMA)** — 5-minute **tumbling** windows per symbol; take the last
    price in each window as `Close_{s,wi}`; keep the previous window's EMA in a
    state store; compute EMA for `j = 38` and `j = 100`; evaluate window `wi`
    once `wi+1` starts (window-close / suppression semantics).
  - **Query 2 (crossovers)** — track both EMAs per symbol; emit a **buy**
    advisory on a 38-over-100 crossover and a **sell** advisory on a 100-over-38
    crossover, to an `advisories` topic.
- **"Kafka Consumer subscribes to topics" + subset of symbols** maps to
  assignment relaxation 3 (compute for all `S`, deliver only the subscribed
  subset `S' ⊊ S`): the consumer subscribes to / filters a subset of symbols.
- **"UI" box** — the consumer feeds a dashboard → the bonus Smart Visualization.
- **Persistence note in the diagram** — the Kafka log is fine as the event
  store; for the report, mention compacted topics or an external sink for the
  UI's query needs.

## Verification

1. **Unit test** `addMarker` without a broker (munit): asserts the marker field
   is added and the four original fields are preserved. Optionally a
   `TopologyTestDriver` in-memory topology test.
2. **`sbt compile`** across all modules; **`sbt test`**.
3. **End-to-end demo** (small sample, no full 5 GB run needed):
   - `docker compose up kafka` (or a local broker).
   - Create a tiny `output/events.ndjson` (a few lines, shape from
     `IngestionAppSuite`).
   - Run producer → streams → consumer.
   - **Success** = the consumer prints records on `trading-events-processed` that
     contain `"processed by kafka streams"`, with original fields intact and the
     key equal to the symbol.
4. Confirm ingestion still builds and tests unchanged after the `Event.scala`
   move into `common`.

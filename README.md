# CS-E4780 project

## Running the pipeline

### With Docker Compose (recommended)

The live pipeline reads CSV files directly in the producer; there is no separate
ingestion run or NDJSON handoff step. Compose starts it together with Kafka,
Streams, and the UI.

```bash
# Download the entire week of datasets (08-11-21 to 14-11-21):
./scripts/download-all-data.sh

# Or download just the sample single day (08-11-21):
./scripts/download-data.sh

# Build app jars and start the whole pipeline.
./scripts/run-pipeline.sh

# Open the UI at http://localhost:8501. Follow service logs with:
docker compose logs -f producer streams
```

The producer discovers top-level `*.csv` files in `data/`, orders them by the
first valid event timestamp in each file, and publishes directly to Kafka. Rows
inside each file retain their source order. The default replay is unpaced and
runs at maximum throughput. Set another speed before starting the pipeline to
pace events against their source timestamps:

```bash
REPLAY_SPEED=1 ./scripts/run-pipeline.sh  # real-time event-time pacing
```

To replay only part of the data, set `REPLAY_FROM` and/or `REPLAY_TO`. They take
Europe/Amsterdam local time and select events in the half-open range
`[from, to)`. The producer seeks to the start instead of parsing everything
before it:

```bash
REPLAY_FROM=2021-11-11T11:30:00 REPLAY_TO=2021-11-11T14:00:00 ./scripts/run-pipeline.sh
```

Stop the services with `docker compose down`.

### Manual step-by-step startup

The launcher above performs these steps for convenience. To start each part
separately, first make sure CSV files are in `data/` (the download script gets
the sample day), then:

```bash
# Build the app jars copied into the Docker images.
sbt -batch producer/assembly streams/assembly

# Start Kafka, create topics, and start the downstream applications.
docker compose up -d --build kafka kafka-init streams ui

# Start the CSV replay producer (unpaced by default). Set REPLAY_SPEED=1 for
# real-time playback.
docker compose up -d --build producer

# Follow logs and open the UI.
docker compose logs -f producer streams
```

The producer can also be started before the other app services; Compose will
start its Kafka and topic-initialization dependencies.

See [INGESTION.md](INGESTION.md) for CSV field mapping, event validation, and
replay ordering and pacing details.

`kafka-init` creates the topics with 6 partitions and makes `symbols`
log-compacted; auto-creation can do neither. Inside the Compose network the apps
reach the broker at `kafka:19092`; from the host it's `localhost:9092`.

## Web UI

A Streamlit container (`ui/`) on <http://localhost:8501>:

- **search box** — filters the symbol list;
- **symbol list** — every symbol observed in the data, read from the compacted
  `symbols` topic and cached; click one to chart it;
- **chart** — EMA38 and EMA100 per closed 5-minute window, with BUY (▲) and
  SELL (▼) markers at the crossovers.

It gets the chart data from the streams app's interactive-query endpoint rather
than by consuming a topic, so the state is materialised once no matter how many
UI replicas run. Useful knobs (env vars read by `ui/app.py`, defaults in
parentheses): `UI_REFRESH_SECONDS` (5), `SYMBOL_CACHE_TTL` (60),
`UI_MAX_POINTS` (5000), `UI_MAX_LISTED_SYMBOLS` (200) and
`REGISTRY_READ_BUDGET_SECONDS` (15). Compose sets only the first three; add the
others to the `ui` service's environment to override them.

## Scaling

The streams app is stateful but partition-parallel, so it scales up to the
partition count (6 by default):

```bash
docker compose up -d --scale streams=3 streams
```

Any replica can answer any query: `QueryService` looks up which instance owns a
symbol's partition and proxies the request there. `streams` therefore uses
`expose`, not `ports` — publishing a host port would block scaling.

## Topics

| Topic | Contents | Cleanup |
|---|---|---|
| `trading-events` | raw tick events, key = symbol | delete |
| `window-results` | one close and EMA result per closed symbol-window, with optional signal | delete |
| `advisories` | one record per EMA crossover (`signal` = BUY/SELL) | delete |
| `symbols` | registry of observed symbols, key = symbol | **compact** |

## Interactive query API

Served by the streams app on port 7070 (Compose-internal):

```
GET /health                                   -> {"state":"RUNNING","host":"..."}
GET /ema?symbol=<s>&from=<ms>&to=<ms>&limit=N -> {"symbol":...,"points":[...]}
```

Each point is `{windowStart, windowEnd, close, ema38, ema100, signal}` with
`signal` either `null`, `"BUY"` or `"SELL"`.

## Tests

```bash
sbt test
```

`StreamsTopologySuite` runs the real topology through a `TopologyTestDriver`:
one window result per closed window, one EMA step per window, crossover-only
advisories, and symbol-registry deduplication. `StreamsAppSuite`,
`ProducerAppSuite` (including replay ranges) and `CsvEventParserSuite` cover the
rest of the streams app, the producer and the CSV parser.

The end-to-end test (real Compose stack vs. an independent reference of the
assignment) and the performance and replay benchmarks are described in
[`tests/TESTS.md`](tests/TESTS.md).

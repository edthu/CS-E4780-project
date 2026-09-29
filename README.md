# CS-E4780 project — detecting trading trends in financial tick data

Kafka pipeline over the DEBS 2022 trading data set: 5-minute tumbling-window
EMAs per symbol (Query 1), buy/sell crossover advisories (Query 2), and a
Streamlit terminal for the bonus Smart Visualization.

See the Mermaid diagrams and the motivation behind each architectural choice in
[`ai_docs/ARCHITECTURE.md`](ai_docs/ARCHITECTURE.md).

## Running the pipeline

### With Docker Compose (recommended)

The live pipeline reads CSV files directly in the producer; there is no separate
ingestion run or NDJSON handoff step. The consumer remains a separate downstream
service, and Compose starts it together with Kafka, Streams, and the UI.

```bash
# Download the sample day (large file); add any other daily CSVs under data/.
./scripts/download-data.sh

# Build app jars and start the whole pipeline.
./scripts/run-pipeline.sh

# Open the UI at http://localhost:8501. Follow service logs with:
docker compose logs -f producer streams consumer
```

The producer discovers top-level `*.csv` files in `data/`, orders them by the
first valid event timestamp in each file, and publishes directly to Kafka. Rows
inside each file retain their source order. The default replay is unpaced and
runs at maximum throughput. Set another speed before starting the pipeline to
pace events against their source timestamps:

```bash
REPLAY_SPEED=1 ./scripts/run-pipeline.sh  # real-time event-time pacing
```

Consumer summary logging is the default so a large replay does not print every
event. For a small sample, `CONSUMER_LOG_LEVEL=ALL ./scripts/run-pipeline.sh`
prints each processed record. Stop the services with `docker compose down`.

### Manual step-by-step startup

The launcher above performs these steps for convenience. To start each part
separately, first make sure CSV files are in `data/` (the download script gets
the sample day), then:

```bash
# Build the app jars copied into the Docker images.
sbt -batch producer/assembly streams/assembly consumer/assembly

# Start Kafka, create topics, and start the downstream applications.
docker compose up -d --build kafka kafka-init streams consumer ui

# Start the CSV replay producer (unpaced by default). Set REPLAY_SPEED=1 for
# real-time playback.
docker compose up -d --build producer

# Follow logs and open the UI.
docker compose logs -f producer streams consumer
```

The producer can also be started before the other app services; Compose will
start its Kafka and topic-initialization dependencies. The consumer itself only
reads processed Kafka events and does not trigger CSV ingestion.

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
UI replicas run. Useful knobs (all Compose env vars): `UI_REFRESH_SECONDS`,
`SYMBOL_CACHE_TTL`, `UI_MAX_POINTS`, `UI_MAX_LISTED_SYMBOLS`.

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
| `trading-events-processed` | every event plus per-tick `ema38` / `ema100` | delete |
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
EMA enrichment, one EMA step per *closed* window, crossover advisories, and
symbol-registry deduplication.

The end-to-end test (real Compose stack vs. an independent reference of the
assignment) and the performance harness (latency, CPU, memory and storage I/O
time per container) are described in [`tests/TESTS.md`](tests/TESTS.md).

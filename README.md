# CS-E4780 project — detecting trading trends in financial tick data

Kafka pipeline over the DEBS 2022 trading data set: 5-minute tumbling-window
EMAs per symbol (Query 1), buy/sell crossover advisories (Query 2), and a
Streamlit terminal for the bonus Smart Visualization.

See the Mermaid diagrams and the motivation behind each architectural choice in
[`ai_docs/ARCHITECTURE.md`](ai_docs/ARCHITECTURE.md).

## Running the pipeline

### With Docker Compose (recommended)

Everything runs as containers on one Compose network. The JVM app images copy in
the fat jars, so build the jars first — the images never compile.

```bash
# 1. Build the app jars (the Docker images COPY these in)
sbt ingestion/assembly producer/assembly consumer/assembly streams/assembly

# 2. Start the broker, topic init, the stream processor, the consumer and the UI
docker compose up -d --build kafka kafka-init streams consumer ui

# 3. Generate NDJSON into ./output (needs the DEBS CSV in ./data — see
#    scripts/download-data.sh). Skip if ./output/events.ndjson already exists.
docker compose run --rm ingestion

# 4. Publish the NDJSON to the trading-events topic
docker compose up -d producer

# 5. Open the UI
open http://localhost:8501

# Watch the processed output instead
docker compose logs -f consumer

# Reset events with
./scripts/reset-topics.sh
```

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

## Streaming ingestion prototype

This prototype reads the DEBS 2022 trading CSV incrementally, maps rows with all required fields to events, and writes newline-delimited JSON. Invalid rows are dropped and counted to avoid producing a multi-gigabyte diagnostic file. The parser uses bounded memory and does not include Kafka yet; the NDJSON output is the local handoff boundary for a future Kafka sink.

The source mapping is:

- `ID` -> `symbol`
- `SecType` -> `securityType`
- `Last` -> `price`
- `Trading date` + `Trading time` -> ISO-8601 `timestamp`

The parser emits only `Last` price events. Rows without a `Last` value are expected non-price events and are counted as skipped, not treated as malformed. Invalid relevant rows are dropped and counted as rejected. Prices must be finite and greater than zero. The parser ignores the other source columns and processes the CSV incrementally, so the full 5 GB file is not loaded into memory.

### Test run

Tests create small temporary CSV fixtures and do not require the real dataset:

```sh
sbt test
```

### Real-data run

Download the full source file outside Docker. The script resumes an interrupted `.part` file and skips a completed existing file:

```sh
./scripts/download-data.sh
sbt assembly
docker compose up --build
```

The real-data Compose command reads:

`data/debs2022-gc-trading-day-08-11-21.csv`

Results are written to `output/events.ndjson`. The application reports total rows, accepted events, skipped non-price rows, rejected rows, elapsed time, and input rows per second.

### Progress logs

When running the real-data container, progress is printed at startup, every one million input rows, and completion:

```text
[ingestion] starting input=... size=4.40 GiB progressIntervalRows=1000000
[ingestion] progress rows=1000000 accepted=... skippedNonPrice=... rejected=... elapsedMillis=... rowsPerSecond=...
[ingestion] completed elapsedMillis=...
```

Change the checkpoint interval in `docker-compose.yml`, or override it for a one-off run:

```sh
INGESTION_PROGRESS_INTERVAL_ROWS=100000 docker compose up --build
```

Follow logs from another terminal with:

```sh
docker compose logs -f ingestion
```
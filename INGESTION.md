## CSV ingestion and replay

The Kafka producer reads daily CSV files incrementally, maps valid price rows to
events, and publishes them directly to `trading-events`. It does not create an
intermediate NDJSON file. Parsing lives in the shared `common` module and is
covered by producer tests.

The source mapping is:

- `ID` -> `symbol`
- `SecType` -> `securityType`
- `Last` -> `price`
- `Trading date` + `Trading time` -> ISO-8601 `timestamp`

Only rows with a valid `Last` price are published. Rows without `Last` are
counted as expected non-price rows; malformed relevant rows are counted as
rejected. Prices must be finite and greater than zero. Parsing and publishing
are incremental, so memory use does not grow with the CSV size.

The producer accepts one or more CSV paths or a directory. For a directory it
discovers top-level `*.csv` files and orders them by the first valid event
timestamp in each file. Rows within each file retain source order, so files
should be internally chronological and cover non-overlapping days for a
globally chronological replay.

Replay pacing follows event timestamp deltas in the `Europe/Amsterdam` time
zone. The default is unpaced maximum throughput (`--speed 0`). For paced replay,
timing is scheduled against the first event so processing overhead does not
accumulate as drift. Use `--speed 1` for real-time playback. In Compose, set
`REPLAY_SPEED` before starting the producer.

To download the sample source data and start the full app, see the run
instructions in [README.md](README.md). To run only the producer locally, start
Kafka first and then use:

```sh
sbt "producer/run data"
```
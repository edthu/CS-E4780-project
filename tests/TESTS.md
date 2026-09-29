# Tests

Three levels, from fastest to slowest.

| Level | What it checks | Needs |
|---|---|---|
| Unit / topology | EMA maths, crossovers, windowing, symbol registry, via `TopologyTestDriver` | sbt |
| End-to-end | The real Compose stack against an independent reference of the assignment | Docker, Python |
| Performance | Latency, CPU, memory, storage I/O time per container | Docker, Python |

## 1. Unit and topology tests

```bash
sbt test
```

## Setup for e2e and perf (once)

Linux with cgroup v2 and Docker Compose ≥ 2.24. The harness reads
`/sys/fs/cgroup` and reaches containers by their bridge IP.

```bash
python3 -m venv tests/.venv
tests/.venv/bin/pip install -r tests/requirements.txt
tests/jmx/fetch-agent.sh                                   # Prometheus JMX exporter agent
sbt producer/assembly consumer/assembly streams/assembly   # jars the images copy in
```

Both harnesses run the stack under their own Compose project (`trading-e2e`,
`trading-perf`) with `tests/docker-compose.test.yml` on top: Kafka on
`localhost:29092`, UI on `localhost:18501`. Your normal stack and its volumes
are left alone. The two harnesses share those ports, so run them one at a time.

## 2. End-to-end test

```bash
tests/.venv/bin/pytest tests/e2e -v
```

This builds the jars and images, starts kafka, streams, producer, consumer and
the UI, and appends a crafted dataset to the file the producer follows:
4 symbols × 80 windows, including BUY→SELL→BUY and BUY→SELL crossovers, an
out-of-order tick, and a late tick. It then checks the following against
`tests/harness/reference.py`, which is written from the assignment and not from
the Scala code:

- `/ema` for every symbol and window: close, EMA38, EMA100, signal;
- the `advisories` topic;
- the `symbols` topic, with each symbol exactly once;
- `trading-events-processed`, with one record per input event;
- UI health;
- after `--scale streams=2`, each replica answers for every symbol (proxying and changelog restore).

| Env var | Effect |
|---|---|
| `E2E_SKIP_BUILD=1` | skip `sbt assembly` (the images are still rebuilt from the existing jars) |
| `E2E_KEEP_STACK=1` | leave the stack running afterwards for poking around |

Takes about 2–3 minutes.

## 3. Performance test

```bash
tests/.venv/bin/python tests/perf/run_perf.py --rates 1000,5000,20000 --replicas 1,2,3
tests/.venv/bin/python tests/perf/plot.py tests/results/<timestamp>
```

Each (replicas, rate) pair gets a fresh stack. Synthetic events for `--symbols`
symbols are appended to the producer's input file at `--rates` events/s. Event
time runs `--speedup` times faster than wall time, so at the default of 60 a
5-minute window closes every 5 s. Every run lasts `--duration` s, and the first
`--warmup` s are excluded from the summary. Use `--no-build` to skip sbt and
image builds. A quick smoke run:

```bash
tests/.venv/bin/python tests/perf/run_perf.py --rates 1000 --replicas 1 --duration 60 --no-build
```

Output: `tests/results/<timestamp>/<R>r-<N>eps/` holds `summary.json`,
`samples.csv` (1 s samples per container), `latency.csv`, `lag.csv` and
`timeseries.png`. Figures across runs are written to the timestamp folder.

### What is measured

**Latency** runs from the moment the event that closes a 5-minute window is
fsynced to the producer's input file to the moment that window's point is
returned by `GET /ema`, the call the UI makes. The UI then shows it within
`UI_REFRESH_SECONDS` (5 s), which is reported as a separate bound. The input
file is the ingestion → producer handoff; ingestion itself is not measured. The
test override sets the producer's file poll to 20 ms, down from 1000 ms.

**Per container** (cgroup v2, all containers):

| Metric | Source |
|---|---|
| CPU % (100 = one core) | `cpu.stat` |
| memory | `memory.current`, `memory.stat` anon/file |
| disk read/write MB/s | `io.stat` |
| I/O stall time (ms per s) | `io.pressure` (PSI "some") |
| network MB/s | `/proc/<pid>/net/dev` |

**Storage write/read time** (JMX). Which storage each container writes to:

| Container | Storage | Metrics |
|---|---|---|
| kafka | **Not a database**: Kafka's own append-only commit log, i.e. segment files per partition under `log.dirs` (`/var/lib/kafka/data` in tests; the normal stack uses Kafka's default `/tmp/kafka-logs` in the container layer) | `jmx_log_append_*`: time appending produced batches to the log (`LocalTimeMs{Produce}`); `jmx_log_read_*`: the same for fetches; `jmx_log_flush_*`: segment fsync time; broker bytes in/out |
| streams | **RocksDB**, one instance per state store and task, backed up to changelog topics | `jmx_state_write_ms_per_s` / `jmx_state_read_ms_per_s` / `jmx_state_flush_ms_per_s`: ms per second spent in store puts / gets+fetches+ranges / flushes; RocksDB bytes written/read, memtable flush time, write stalls |
| producer | none; reads the input file and writes to Kafka | `jmx_record_send_rate` (**events/s**), request latency, network I/O time. The container log also prints `recordsPerSecond` every 2 s |
| consumer | none | records consumed/s, fetch latency |
| ui | none | OS metrics only |

`_per_s` values are rates over the run. `_avg_ms` is the average per broker
request. `{mean, max}` pairs are gauges sampled every second.

**Throughput**: the achieved input rate, producer `record-send-rate`, streams
`process-rate`, and consumer-group lag in `lag.csv`. The lag uses *committed*
offsets, so it rises and falls with the 30 s commit interval. Use `process-rate`
to judge whether Streams keeps up.

## Troubleshooting

- `JMX agent missing`: run `tests/jmx/fetch-agent.sh`.
- A run left containers behind (e.g. after Ctrl-C): `docker compose -p trading-perf -f docker-compose.yml -f tests/docker-compose.test.yml down -v`. Use `trading-e2e` for the e2e project.
- Disk bytes read 0 for Kafka: this happens if its log dir is in the container layer on a fuse-overlayfs Docker host, because the FUSE daemon does the writes outside the container's cgroup. The test override puts it on a volume for this reason.

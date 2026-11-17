# Full benchmark run

The steps below produce the numbers for the report's Performance Evaluation. Run
them in order, because the speeds in steps 3 and 4 come from the result of step 2.
[TESTS.md](TESTS.md) explains how each number is measured.

Only one harness can run at a time, and the machine should be otherwise idle.

| Step | Test | Estimated time |
|---|---|---|
| 1 | Unit and end-to-end tests | 5 min |
| 2 | Capacity and scaling | 1.5 h |
| 3 | Latency against load | 1.5 h |
| 4 | Sustainability and CPU per event | 1.5 h |
| 5 | Latency at real speed | 4.5 h |
| 6 | Figures | 1 min |

These times are estimates from the smoke runs and include warm-ups. Run step 5
overnight.

## Setup

Run this once if `tests/.venv` or the JMX agent is missing:

```bash
python3 -m venv tests/.venv
tests/.venv/bin/pip install -r tests/requirements.txt
tests/jmx/fetch-agent.sh
```

Set these in every shell you use for the steps below:

```bash
PY=tests/.venv/bin/python
OUT=tests/results/final
```

Check free memory before step 2. The harness needs about 1.5 GB per replica plus
2 GB, so 6 replicas need about 11 GB of `MemAvailable`. With less, it skips that
replica count and prints a warning.

```bash
grep MemAvailable /proc/meminfo
```

## 1. Unit and end-to-end tests

```bash
sbt test
tests/.venv/bin/pytest tests/e2e -v
```

## 2. Capacity and scaling

Drains a backlog of 3.76M events (10 Nov 15:00-18:00) with 1, 2, 3 and 6 replicas
at 1 CPU each, with the broker limited to 2 CPUs. Each point is repeated 3 times.
This step also builds the jars and images, so the later steps use `--no-build`.

```bash
$PY tests/perf/run_replay.py capacity --replicas 1,2,3,6 --repeats 3 --out $OUT
```

Then calculate the speeds for steps 3 and 4:

```bash
$PY - "$OUT" <<'EOF'
import json, sys
from collections import defaultdict
from pathlib import Path
from statistics import mean
runs = defaultdict(list)
for p in Path(sys.argv[1]).glob("capacity-*/summary.json"):
    s = json.loads(p.read_text())
    runs[s["replicas"]].append(s["throughput_eps"])
for n, values in sorted(runs.items()):
    c = mean(values)
    print(f"{n} replicas: capacity {c:.0f} eps, sustain --speed {0.9 * c / 348:.1f}")
    if n == 1:
        print("latency vs load --speed " + ",".join(f"{f * c / 345:.1f}" for f in (.25, .5, .75, .9)))
EOF
```

The commands below use speeds from the smoke run, so replace them with the printed
values. Speeds for 3 and 6 replicas have no smoke result and are written as
`<S3>` and `<S6>`.

## 3. Latency against load

Replays the average slice with 1 replica, at 25, 50, 75 and 90% of the 1-replica
capacity.

```bash
$PY tests/perf/run_replay.py latency --scenario average --profile tuned --speed 5.2,10.3,15.5,18.6 --no-build --out $OUT
```

## 4. Sustainability and CPU per event

Runs paced at 90% of each replica count's capacity, without probes. The system
keeps up if `lag.max` stays bounded and `lag.final` returns close to 0.

```bash
$PY tests/perf/run_replay.py latency --scenario capacity --no-probes --profile tuned --replicas 1 --speed 18.4 --no-build --out $OUT
$PY tests/perf/run_replay.py latency --scenario capacity --no-probes --profile tuned --replicas 2 --speed 37.3 --no-build --out $OUT
$PY tests/perf/run_replay.py latency --scenario capacity --no-probes --profile tuned --replicas 3 --speed <S3> --no-build --out $OUT
$PY tests/perf/run_replay.py latency --scenario capacity --no-probes --profile tuned --replicas 6 --speed <S6> --no-build --out $OUT
```

## 5. Latency at real speed

Runs both slices at 1×, once as shipped (30 s commit, 10 MB cache) and once tuned
(1 s commit, no cache). The `large` runs go to their own folder, so the closing-auction
burst is not drawn as a point on the latency-against-load curve.

```bash
$PY tests/perf/run_replay.py latency --scenario average --profile shipped --no-build --out $OUT
$PY tests/perf/run_replay.py latency --scenario average --profile tuned --no-build --out $OUT
$PY tests/perf/run_replay.py latency --scenario large --profile shipped --no-build --out $OUT-large
$PY tests/perf/run_replay.py latency --scenario large --profile tuned --no-build --out $OUT-large
```

## 6. Figures

```bash
$PY tests/perf/plot.py $OUT
$PY tests/perf/plot.py $OUT-large
```

| Output in `tests/results/final` | Shows |
|---|---|
| `table_capacity.png` | Every number of every capacity run |
| `table_paced.png` | Every number of every latency and sustainability run |
| `scaling.png` | Drain throughput and speedup against replicas, with the partition-skew limit. For efficiency, divide speedup by replicas |
| `capacity_resources.png` | % of the CPU limit and CPU time per event for Streams and the broker, against replicas |
| `latency_vs_load.png` | p50 and p95 latency against input rate |
| `latency_runs.png` | Every probe latency of every latency run, with p50 and p95 |
| `sustain_lag.png` | Consumer lag over time in each sustainability run |
| `<run>/timeseries.png` | One run over time: input and processing rate, lag, probe latency, CPU and memory per service, with the measured interval shaded |

Each run's numbers are also in `<run>/summary.json`.

## Before using the results

For each capacity run in `<run>/summary.json`:

- `final_lag` is 0.
- `duplicate_fraction` is close to 0.
- `resources.kafka.pct_of_limit` is well below 100. If it is near 100, the broker
  is the limit, and any plateau in scaling comes from the broker.
- Compare the measured speedup with `max_speedup_from_partition_skew`, the best
  speedup the partitioning allows.

For each latency run, check that `latency_ms.timeouts` is 0. Percentiles cover
completed probes only.

## Not covered: correctness on real data

No automated test compares the system's output on the real dataset with an
independent reference yet. `tests/e2e` checks correctness on a crafted dataset only.
The latency runs above can't be used for this, because their EMAs start from a
partial day.

## If a run is interrupted

Remove what it left behind, then rerun that step:

```bash
docker compose -p trading-replay -f docker-compose.yml -f tests/docker-compose.test.yml -f tests/docker-compose.replay.yml down -v
```

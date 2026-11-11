"""Benchmarks that replay slices of the real data through the containerised pipeline.

  capacity  Loads a slice into Kafka unpaced, then starts Streams and measures how
            fast it drains the backlog. With --replicas this is the scaling test.
  latency   Warms the state up, then replays a slice paced at --speed with probe
            events, measuring window close -> visible on GET /ema. With --no-probes
            it is a sustainability and CPU run: no probe polling, only lag and CPU.

Results go to tests/results/replay-<timestamp>/<run>/; perf/plot.py draws figures.
All rates are price events, the only rows the producer publishes.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

import requests
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

from run_perf import LagSampler, pct, write_csv  # also puts tests/ on sys.path

from harness.metrics import Sampler  # noqa: E402
from harness.stack import KAFKA_BOOTSTRAP, REPLAY_COMPOSE_FILE, ROOT, WORK_DIR, Stack  # noqa: E402

PROJECT = "trading-replay"
PROBE_DIR = WORK_DIR / "probe"
INPUT_TOPIC = "trading-events"
STREAMS_GROUP = "trading-streams"
CONSUMED = "jmx_records_consumed_total"
BROKER_CPUS = 2.0
REPLICA_MEMORY_GB = 1.5
HEADROOM_GB = 2.0

SCENARIOS = {
    "average": ("2021-11-11T11:30:00", "2021-11-11T12:30:00"),
    "large": ("2021-11-10T17:20:00", "2021-11-10T17:50:00"),
    "capacity": ("2021-11-10T15:00:00", "2021-11-10T18:00:00"),
}
PROFILES = {
    "shipped": {},
    "tuned": {"STREAMS_COMMIT_INTERVAL_MS": "1000", "STREAMS_CACHE_MAX_BYTES": "0"},
}


# -- producer and Kafka helpers ----------------------------------------------

def replay_env(start: str, end: str) -> dict[str, str]:
    return {"PRODUCER_FROM": start, "PRODUCER_TO": end}


def load_unpaced(stack: Stack, start: str, end: str) -> dict:
    """Publish [start, end) as fast as the producer can and wait for it to finish."""
    began = time.time()
    done = subprocess.run(stack.producer_cmd(["/data", "--speed", "0"], replay_env(start, end)),
                          cwd=ROOT, env=stack.compose_env(), capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"producer failed:\n{done.stdout[-2000:]}\n{done.stderr[-2000:]}")
    sent = re.search(r"completed sent=(\d+)", done.stdout)
    return {"sent": int(sent.group(1)) if sent else None, "seconds": round(time.time() - began, 1)}


def end_offsets() -> dict[int, int]:
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, group_id=None)
    try:
        tps = [TopicPartition(INPUT_TOPIC, p) for p in consumer.partitions_for_topic(INPUT_TOPIC)]
        return {tp.partition: offset for tp, offset in consumer.end_offsets(tps).items()}
    finally:
        consumer.close()


def group_lag(ends: dict[int, int]) -> int | None:
    """Records not yet committed by Streams; None until the group has committed."""
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        committed = admin.list_consumer_group_offsets(STREAMS_GROUP)
    finally:
        admin.close()
    if not committed:
        return None
    done = {tp.partition: meta.offset for tp, meta in committed.items() if tp.topic == INPUT_TOPIC}
    return sum(end - done.get(p, 0) for p, end in ends.items())


def wait_zero_lag(timeout: float) -> None:
    ends = end_offsets()
    Stack.wait_for(lambda: group_lag(ends) == 0, "streams lag to reach 0", timeout=timeout, interval=2)


def wait_producer_container(stack: Stack, timeout: float = 120) -> None:
    Stack.wait_for(lambda: stack.containers("producer"), "producer container", timeout=timeout)


# -- working with sampled counters -------------------------------------------

def by_container(rows: list[dict], key: str, service: str | None = None) -> dict[str, list[tuple[float, float]]]:
    series: dict[str, list[tuple[float, float]]] = {}
    for r in rows:
        if key in r and (service is None or r["service"] == service):
            series.setdefault(r["container"], []).append((r["t"], float(r[key])))
    return series


def value_at(series: list[tuple[float, float]], t: float) -> float:
    """Linear interpolation of a cumulative counter; flat outside its range."""
    if t <= series[0][0]:
        return series[0][1]
    for (t_a, a), (t_b, b) in zip(series, series[1:]):
        if t <= t_b:
            return a + (b - a) * (t - t_a) / (t_b - t_a) if t_b > t_a else b
    return series[-1][1]


def consumed_total(rows: list[dict]) -> list[tuple[float, float]]:
    """Summed records consumed across replicas, on the sampler's own time ticks."""
    series = by_container(rows, CONSUMED, "streams")
    ticks = sorted({t for points in series.values() for t, _ in points})
    return [(t, sum(value_at(points, t) for points in series.values() if points[0][0] <= t)) for t in ticks]


def time_reaching(series: list[tuple[float, float]], target: float) -> float | None:
    for (t_a, a), (t_b, b) in zip(series, series[1:]):
        if b >= target:
            return t_a + (t_b - t_a) * (target - a) / (b - a) if b > a else t_b
    return None


def resource_usage(rows: list[dict], start: float, end: float, events: float, cpu_limits: dict[str, float]) -> dict:
    """CPU time per event and % of the CPU limit, per service, over [start, end]."""
    seconds = end - start
    out: dict[str, dict] = {}
    for service in sorted({r["service"] for r in rows}):
        series = by_container(rows, "cpu_usage_us", service)
        memory = by_container(rows, "mem_bytes", service)
        cpu_s = sum((value_at(s, end) - value_at(s, start)) / 1e6 for s in series.values())
        stats = {
            "containers": len(series),
            "cpu_seconds": round(cpu_s, 2),
            "cpu_us_per_event": round(cpu_s * 1e6 / events, 2) if events else None,
            "mean_cores": round(cpu_s / seconds, 3),
            "mem_mb_max": round(max((v for s in memory.values() for t, v in s if start <= t <= end), default=0) / 2**20, 1),
        }
        limit = cpu_limits.get(service)
        if limit:
            stats["cpu_limit_cores"] = round(limit * len(series), 2)
            stats["pct_of_limit"] = round(100 * cpu_s / (seconds * limit * len(series)), 1)
        out[service] = stats
    return out


# -- capacity ----------------------------------------------------------------

def mem_available_gb() -> float:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) / 2**20
    return 0.0


def run_capacity(stack: Stack, out: Path, start: str, end: str, replicas: int, args, build: bool) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    print(f"==> capacity replicas={replicas} {start} .. {end} -> {out}", flush=True)
    stack.up_kafka(build_images=build)
    load = load_unpaced(stack, start, end)
    ends = end_offsets()
    total = sum(ends.values())
    print(f"    loaded {total} records in {load['seconds']} s", flush=True)

    stack.up_streams(replicas)
    Stack.wait_for(lambda: len(stack.containers("streams")) == replicas, "streams containers")
    sampler = Sampler(stack, interval=1.0, services=("kafka", "streams"))
    sampler.start()
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        series = consumed_total(list(sampler.rows))
        if series and series[-1][1] >= total:
            break
        time.sleep(1)
    else:
        sampler.stop()
        write_csv(out / "samples.csv", sampler.rows)
        raise TimeoutError(f"streams did not drain {total} records in {args.timeout} s")
    wait_zero_lag(args.commit_wait)
    time.sleep(2)
    sampler.stop()

    series = consumed_total(sampler.rows)
    t10, t90 = time_reaching(series, total * 0.1), time_reaching(series, total * 0.9)
    busiest = max(ends.values())
    limits = {"streams": float(args.cpus), "kafka": BROKER_CPUS}
    summary = {
        "kind": "capacity",
        "replicas": replicas,
        "profile": args.profile,
        "from": start, "to": end,
        "cpus_per_replica": float(args.cpus),
        "records": total,
        "partition_records": ends,
        "max_speedup_from_partition_skew": round(total / busiest, 3),
        "producer_load": load,
        "throughput_eps": round(total * 0.8 / (t90 - t10), 1),
        "records_consumed": series[-1][1],
        "duplicate_fraction": round(series[-1][1] / total - 1, 5),
        "final_lag": group_lag(ends),
        "measured_seconds": round(t90 - t10, 1),
        "resources": resource_usage(sampler.rows, t10, t90, total * 0.8, limits),
        "sampler_errors": sampler.errors,
    }
    write_csv(out / "samples.csv", sampler.rows)
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    streams = summary["resources"]["streams"]
    print(f"    throughput={summary['throughput_eps']} eps, {streams['cpu_us_per_event']} us/event, "
          f"{streams['pct_of_limit']}% of limit, final lag={summary['final_lag']}", flush=True)
    return summary


# -- latency -----------------------------------------------------------------

class ProbePoller(threading.Thread):
    """Follows the producer's probe log and polls /ema until each point is visible.
    Polling is one round over all outstanding probes, so a slow probe does not
    delay the measurement of the others."""

    def __init__(self, url: str, log_path: Path, timeout_s: float):
        super().__init__(daemon=True)
        self.url, self.log_path, self.timeout_s = url, log_path, timeout_s
        self.pending: list[dict] = []
        self.samples: list[dict] = []
        self._offset = 0
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()
        self.join()

    def idle(self) -> bool:
        return not self.pending and self.log_path.stat().st_size == self._offset

    def _read_log(self) -> None:
        with self.log_path.open("rb") as f:
            f.seek(self._offset)
            data = f.read()
        complete = data[: data.rfind(b"\n") + 1]
        self._offset += len(complete)
        for line in complete.decode().splitlines():
            window, symbol, t0_ms = line.split(",")
            self.pending.append({"window_start": int(window), "symbol": symbol, "t0_ms": int(t0_ms)})

    def run(self) -> None:
        session = requests.Session()
        while not self._stop.is_set():
            self._read_log()
            waiting = []
            for probe in self.pending:
                params = {"symbol": probe["symbol"], "from": probe["window_start"], "to": probe["window_start"]}
                now_ms = time.time() * 1000
                seen = False
                try:
                    r = session.get(f"{self.url}/ema", params=params, timeout=5)
                    seen = r.ok and bool(r.json().get("points"))
                except requests.RequestException:
                    pass
                if seen:
                    self.samples.append({**probe, "t1_ms": now_ms, "latency_ms": now_ms - probe["t0_ms"], "timed_out": 0})
                elif now_ms - probe["t0_ms"] > self.timeout_s * 1000:
                    self.samples.append({**probe, "t1_ms": "", "latency_ms": "", "timed_out": 1})
                else:
                    waiting.append(probe)
            self.pending = waiting
            self._stop.wait(0.01)


def run_latency(stack: Stack, out: Path, name: str, start: str, end: str, replicas: int, speed: float, args, build: bool) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    print(f"==> latency {name} replicas={replicas} speed={speed} profile={args.profile} -> {out}", flush=True)
    stack.up_kafka(build_images=build)
    stack.up_streams(replicas)
    stack.wait_streams_running(replicas)

    if not args.no_warmup:
        warm_from = start[:10] + "T07:00:00"
        print(f"    warming up from {warm_from}", flush=True)
        load_unpaced(stack, warm_from, start)
        wait_zero_lag(args.timeout)
    produced_before = sum(end_offsets().values())

    probe_env: dict[str, str] = {}
    log_path = PROBE_DIR / f"{out.name}.csv"
    if not args.no_probes:
        log_path.write_text("")
        log_path.chmod(0o666)
        probe_env = {"PRODUCER_PROBES": str(args.probes_per_partition), "PRODUCER_PROBE_LOG": f"/probe/{log_path.name}"}

    producer_log = (out / "producer.log").open("w")
    producer = subprocess.Popen(
        stack.producer_cmd(["/data", "--speed", str(speed)], {**replay_env(start, end), **probe_env}),
        cwd=ROOT, env=stack.compose_env(), stdout=producer_log, stderr=subprocess.STDOUT)
    wait_producer_container(stack)
    sampler = Sampler(stack, interval=1.0, services=("kafka", "streams", "producer"))
    lag = LagSampler()
    poller = None if args.no_probes else ProbePoller(stack.streams_urls()[0], log_path, args.probe_timeout)
    for t in filter(None, (sampler, lag, poller)):
        t.start()

    if producer.wait() != 0:
        raise RuntimeError(f"producer failed, see {out / 'producer.log'}")
    producer_log.close()
    if poller:
        deadline = time.monotonic() + args.probe_timeout + 5
        while time.monotonic() < deadline and not poller.idle():
            time.sleep(0.5)
        poller.stop()
    time.sleep(2)
    lag.stop()
    sampler.stop()

    # Measured from the first record of the slice reaching Kafka until the last.
    moving = [r for r in lag.rows if r["produced"] > produced_before]
    t_start, t_end = moving[0]["t"], next(r["t"] for r in moving if r["produced"] == moving[-1]["produced"])
    events = moving[-1]["produced"] - moving[0]["produced"]
    limits = {"streams": float(args.cpus), "kafka": BROKER_CPUS}
    lags = [r["lag"] for r in lag.rows if t_start <= r["t"]]

    summary = {
        "kind": "latency" if poller else "sustain",
        "scenario": name,
        "replicas": replicas,
        "profile": args.profile,
        "from": start, "to": end,
        "speed": speed,
        "cpus_per_replica": float(args.cpus),
        "warmed_up": not args.no_warmup,
        "events": events,
        "seconds": round(t_end - t_start, 1),
        "achieved_eps": round(events / (t_end - t_start), 1) if t_end > t_start else None,
        "lag": {"max": max(lags, default=None), "final": lags[-1] if lags else None},
        "resources": resource_usage(sampler.rows, t_start, t_end, events, limits) if t_end > t_start else {},
        "sampler_errors": sampler.errors,
    }
    if poller:
        first_window = min((s["window_start"] for s in poller.samples), default=0)
        kept = [s for s in poller.samples if s["window_start"] > first_window]
        values = [s["latency_ms"] for s in kept if not s["timed_out"]]
        timeouts = sum(s["timed_out"] for s in kept)
        summary["latency_ms"] = {"count": len(values), "timeouts": timeouts, "windows": len({s["window_start"] for s in kept}),
                                 "p50": pct(values, .5), "p95": pct(values, .95), "p99": pct(values, .99),
                                 "max": round(max(values), 1) if values else None}
        write_csv(out / "latency.csv", poller.samples)
        log_path.unlink(missing_ok=True)
    write_csv(out / "samples.csv", sampler.rows)
    write_csv(out / "lag.csv", lag.rows)
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"    achieved={summary['achieved_eps']} eps, max lag={summary['lag']['max']}, "
          f"final lag={summary['lag']['final']}, latency={summary.get('latency_ms')}", flush=True)
    return summary


# -- entry point -------------------------------------------------------------

def slice_for(args) -> tuple[str, str, str]:
    if args.start and args.end:
        return "custom", args.start, args.end
    return (args.scenario, *SCENARIOS[args.scenario])


def replica_counts(args) -> list[int]:
    wanted = [int(x) for x in args.replicas.split(",")]
    available = mem_available_gb()
    usable = []
    for n in wanted:
        needed = n * REPLICA_MEMORY_GB + HEADROOM_GB
        if needed > available:
            print(f"WARNING: skipping {n} replicas: needs about {needed:.1f} GB, {available:.1f} GB available", flush=True)
        else:
            usable.append(n)
    return usable


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("experiment", choices=("capacity", "latency"))
    p.add_argument("--scenario", choices=sorted(SCENARIOS), default=None, help="default: capacity for capacity runs, average for latency")
    p.add_argument("--from", dest="start", help="custom slice start, e.g. 2021-11-11T11:30:00")
    p.add_argument("--to", dest="end", help="custom slice end")
    p.add_argument("--replicas", default="1", help="comma-separated streams replica counts")
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--speed", default="1", help="latency only: comma-separated replay speeds")
    p.add_argument("--profile", choices=sorted(PROFILES), default="shipped", help="shipped: Kafka defaults; tuned: 1 s commit, no cache")
    p.add_argument("--cpus", default="1.0", help="CPU limit per streams replica")
    p.add_argument("--no-probes", action="store_true", help="latency only: measure lag and CPU without probes")
    p.add_argument("--no-warmup", action="store_true", help="latency only: start the slice on empty state")
    p.add_argument("--probes-per-partition", type=int, default=2)
    p.add_argument("--probe-timeout", type=float, default=120, help="seconds before a probe counts as timed out")
    p.add_argument("--timeout", type=float, default=3600, help="seconds allowed for a drain or warm-up")
    p.add_argument("--commit-wait", type=float, default=120, help="seconds allowed for the final commit after a drain")
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--no-build", action="store_true", help="skip sbt assembly and image builds")
    p.add_argument("--keep-stack", action="store_true", help="leave the last stack running")
    args = p.parse_args()
    if args.scenario is None:
        args.scenario = "capacity" if args.experiment == "capacity" else "average"
    if bool(args.start) != bool(args.end):
        p.error("--from and --to must be given together")

    out = args.out or ROOT / "tests" / "results" / f"replay-{datetime.now():%Y%m%d-%H%M%S}"
    # Before any Compose call, or Docker creates the bind-mount source as root.
    PROBE_DIR.mkdir(parents=True, exist_ok=True)
    stack = Stack(PROJECT, extra_files=(REPLAY_COMPOSE_FILE,))
    stack.env = {"STREAMS_CPUS": args.cpus, "KAFKA_CPUS": str(BROKER_CPUS), **PROFILES[args.profile]}
    if not args.no_build:
        stack.build_jars()
    name, start, end = slice_for(args)
    build = not args.no_build
    summaries = []
    try:
        for replicas in replica_counts(args):
            for repeat in range(1, args.repeats + 1):
                if args.experiment == "capacity":
                    run_dir = out / f"capacity-{replicas}r-{args.profile}-{repeat}"
                    summaries.append(run_capacity(stack, run_dir, start, end, replicas, args, build))
                    build = False
                    continue
                for speed in (float(x) for x in args.speed.split(",")):
                    kind = "sustain" if args.no_probes else "latency"
                    run_dir = out / f"{kind}-{name}-{replicas}r-{speed:g}x-{args.profile}-{repeat}"
                    summaries.append(run_latency(stack, run_dir, name, start, end, replicas, speed, args, build))
                    build = False
    finally:
        if not args.keep_stack:
            stack.down()
    out.mkdir(parents=True, exist_ok=True)
    (out / "summary.json").write_text(json.dumps(summaries, indent=2))
    print(f"==> results in {out}")


if __name__ == "__main__":
    main()

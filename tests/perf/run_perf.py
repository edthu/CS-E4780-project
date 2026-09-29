"""Performance runs over the containerised pipeline.

For every (streams replicas, input rate) pair this starts a fresh stack, drives
synthetic load through the real producer -> Kafka -> Streams path, and records:

  * latency: from the moment the event that closes a 5-minute window is on the
    producer's input file, until that window's point is readable on GET /ema
    (what the UI polls). The UI adds up to UI_REFRESH_SECONDS on top.
  * per container: CPU, memory, disk and network I/O, I/O stall time (cgroup v2)
  * per JVM container: storage write/read times and throughput from JMX
    (see harness/metrics.py JMX_SERIES).

Results go to tests/results/<timestamp>/<replicas>r-<rate>eps/ as samples.csv,
latency.csv and summary.json; perf/plot.py draws the figures.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import queue
import random
import statistics
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import requests
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness.events import EventWriter, event_line, format_ts  # noqa: E402
from harness.metrics import Sampler  # noqa: E402
from harness.reference import CEST, WINDOW_MS, to_epoch_ms  # noqa: E402
from harness.stack import EVENTS_FILE, KAFKA_BOOTSTRAP, ROOT, Stack  # noqa: E402

PROJECT = "trading-perf"
PROBE = "PROBE.T"
SIM_START_MS = to_epoch_ms("2021-11-08T09:00:00.000")
STREAMS_GROUP = "trading-streams"
UI_REFRESH_SECONDS = 5  # docker-compose.yml default; reported as a bound, not measured
TICK_S = 0.01


def sim_timestamp(epoch_ms: float) -> str:
    """Epoch ms -> the CEST wall-clock string the ingestion format uses."""
    dt = datetime.fromtimestamp(epoch_ms / 1000, CEST).replace(tzinfo=None)
    return format_ts(dt)


# -- load generator ----------------------------------------------------------

class Generator(threading.Thread):
    """Writes `rate` events/s over `symbols` symbols; event time runs `speedup`
    times faster than wall time, so a 5-minute window lasts 300/speedup s.

    The first batch past a window boundary contains the events that close the
    previous window on every partition, plus one PROBE.T event so the probe
    symbol has a point in every window. That batch is fsynced and its write
    time is the latency start for the window that just ended."""

    def __init__(self, rate: int, symbols: int, speedup: float, pending: queue.Queue):
        super().__init__(daemon=True)
        self.rate, self.speedup, self.pending = rate, speedup, pending
        rng = random.Random(42)
        exchanges = ("FR", "NL", "ETR")
        self.symbols = [f"S{i:04d}.{exchanges[i % 3]}" for i in range(symbols)]
        self.prices = {s: rng.uniform(10, 200) for s in self.symbols}
        self.rng = rng
        self.written = 0
        self.started_at = self.stopped_at = 0.0
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()
        self.join()

    def run(self) -> None:
        writer = EventWriter(EVENTS_FILE)
        self.started_at = time.time()
        owed = 0.0
        last_window = None
        while not self._stop.is_set():
            now = time.time()
            sim_ms = SIM_START_MS + (now - self.started_at) * self.speedup * 1000
            ts = sim_timestamp(sim_ms)
            window = int(sim_ms - sim_ms % WINDOW_MS)

            owed += self.rate * TICK_S
            n = int(owed)
            owed -= n
            lines = []
            for _ in range(n):
                s = self.rng.choice(self.symbols)
                self.prices[s] = max(0.01, self.prices[s] * (1 + self.rng.gauss(0, 0.001)))
                lines.append(event_line(s, round(self.prices[s], 4), ts))

            crossed = last_window is not None and window != last_window
            if crossed or last_window is None:
                lines.append(event_line(PROBE, 100.0, ts))
            t_written = writer.write(lines, sync=crossed)
            self.written += n
            if crossed:
                self.pending.put((last_window, t_written))
            last_window = window
            self._stop.wait(max(0.0, TICK_S - (time.time() - now)))
        self.stopped_at = time.time()
        writer.close()


class Prober(threading.Thread):
    """Polls /ema for each closed probe window until it is visible."""

    def __init__(self, url: str, pending: queue.Queue, timeout_s: float = 60):
        super().__init__(daemon=True)
        self.url, self.pending, self.timeout_s = url, pending, timeout_s
        self.samples: list[dict] = []
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()
        self.join()

    def run(self) -> None:
        session = requests.Session()
        while not self._stop.is_set():
            try:
                window, t0 = self.pending.get(timeout=0.2)
            except queue.Empty:
                continue
            params = {"symbol": PROBE, "from": window, "to": window}
            t1 = math.nan
            while time.time() < t0 + self.timeout_s and not self._stop.is_set():
                try:
                    r = session.get(f"{self.url}/ema", params=params, timeout=5)
                    if r.ok and r.json().get("points"):
                        t1 = time.time()
                        break
                except requests.RequestException:
                    pass
                time.sleep(0.01)
            self.samples.append({"window_start": window, "t0": t0, "t1": t1,
                                 "latency_ms": (t1 - t0) * 1000 if t1 == t1 else math.nan})


# -- consumer lag ------------------------------------------------------------

class LagSampler(threading.Thread):
    """Streams consumer-group lag on trading-events, once per second."""

    def __init__(self):
        super().__init__(daemon=True)
        self.rows: list[dict] = []
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()
        self.join()

    def run(self) -> None:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, group_id=None)
        tps = [TopicPartition("trading-events", p)
               for p in consumer.partitions_for_topic("trading-events")]
        while not self._stop.is_set():
            try:
                end = consumer.end_offsets(tps)
                committed = admin.list_consumer_group_offsets(STREAMS_GROUP)
                done = sum(committed[tp].offset for tp in tps if tp in committed)
                produced = sum(end.values())
                self.rows.append({"t": time.time(), "produced": produced,
                                  "committed": done, "lag": produced - done})
            except Exception:  # noqa: BLE001 - group may not exist for the first seconds
                pass
            self._stop.wait(1.0)
        admin.close()
        consumer.close()


# -- one run -----------------------------------------------------------------

def run_once(stack: Stack, out: Path, replicas: int, rate: int, args, build: bool) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    print(f"==> run replicas={replicas} rate={rate} eps -> {out}", flush=True)
    stack.up(streams_replicas=replicas, build_images=build)

    pending: queue.Queue = queue.Queue()
    sampler = Sampler(stack, interval=1.0)
    lag = LagSampler()
    prober = Prober(stack.streams_urls()[0], pending)
    gen = Generator(rate, args.symbols, args.speedup, pending)
    for t in (sampler, lag, prober, gen):
        t.start()
    time.sleep(args.duration)
    gen.stop()
    # Let the last windows (and the backlog, if the system fell behind) drain.
    drain_deadline = time.time() + args.drain
    while time.time() < drain_deadline and not pending.empty():
        time.sleep(0.5)
    time.sleep(2)
    for t in (prober, lag, sampler):
        t.stop()

    write_csv(out / "samples.csv", sampler.rows)
    write_csv(out / "latency.csv", prober.samples)
    write_csv(out / "lag.csv", lag.rows)
    summary = summarise(replicas, rate, args, gen, prober.samples, sampler.rows, lag.rows)
    summary["sampler_errors"] = sampler.errors
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    lat = summary["latency_ms"]
    print(f"    latency p50={lat['p50']} p95={lat['p95']} max={lat['max']} ms "
          f"(n={lat['count']}, timeouts={lat['timeouts']}); final lag={summary['lag']['final']}",
          flush=True)
    if not args.keep_stack:
        stack.down()
    return summary


def write_csv(path: Path, rows: list[dict]) -> None:
    columns = list(dict.fromkeys(k for r in rows for k in r))
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)


def pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    return round(values[lo] + (values[hi] - values[lo]) * (k - lo), 1)


def summarise_jmx(rows: list[dict]) -> dict:
    """Cumulative `_total` columns become per-second rates over the run (and,
    for broker histograms, an average per request); gauges get mean/max."""
    out: dict[str, object] = {}
    cols = sorted({k for r in rows for k in r if k.startswith("jmx_")})
    for col in cols:
        vals = [(r["t"], r[col]) for r in rows if col in r]
        if not vals:
            continue
        if col.endswith("_total"):
            (t_a, a), (t_b, b) = vals[0], vals[-1]
            out[col.removesuffix("_total") + "_per_s"] = round((b - a) / (t_b - t_a), 4) if t_b > t_a else None
            out[col.removesuffix("_total") + "_run_delta"] = round(b - a, 3)
        else:
            nums = [v for _, v in vals]
            out[col] = {"mean": round(statistics.fmean(nums), 4), "max": round(max(nums), 4)}
    # e.g. jmx_log_append_ms / jmx_log_append_count -> average ms per produce request
    for col in [c for c in out if c.endswith("_ms_run_delta")]:
        count = out.get(col.replace("_ms_run_delta", "_count_run_delta"))
        if count:
            out[col.replace("_ms_run_delta", "_avg_ms")] = round(out[col] / count, 3)
    return out


def summarise(replicas, rate, args, gen, latency, samples, lag) -> dict:
    warm = gen.started_at + args.warmup
    lat = [s["latency_ms"] for s in latency if s["t0"] >= warm and s["latency_ms"] == s["latency_ms"]]
    timeouts = sum(1 for s in latency if s["latency_ms"] != s["latency_ms"])

    containers: dict[str, dict] = {}
    by_container: dict[str, list[dict]] = {}
    for r in samples:
        if r["t"] >= warm and "cpu_usage_us" in r:
            by_container.setdefault(r["container"], []).append(r)
    for name, rows in by_container.items():
        stats: dict[str, object] = {"service": rows[0]["service"]}
        span = rows[-1]["t"] - rows[0]["t"] if len(rows) > 1 else 0
        if span > 0:
            def per_s(key, scale=1.0):
                return round((rows[-1][key] - rows[0][key]) / span * scale, 3)
            stats["cpu_pct_mean"] = per_s("cpu_usage_us", 100 / 1e6)
            stats["disk_write_mb_s"] = per_s("disk_write_bytes", 1 / 2**20)
            stats["disk_read_mb_s"] = per_s("disk_read_bytes", 1 / 2**20)
            stats["io_stall_ms_per_s"] = per_s("io_stall_us", 1 / 1000)
            stats["net_rx_mb_s"] = per_s("net_rx_bytes", 1 / 2**20)
            stats["net_tx_mb_s"] = per_s("net_tx_bytes", 1 / 2**20)
            cpu = [(b["cpu_usage_us"] - a["cpu_usage_us"]) / (b["t"] - a["t"]) / 1e4
                   for a, b in zip(rows, rows[1:]) if b["t"] > a["t"]]
            stats["cpu_pct_max"] = round(max(cpu), 1) if cpu else None
        mem = [r["mem_bytes"] / 2**20 for r in rows]
        stats["mem_mb_mean"] = round(statistics.fmean(mem), 1)
        stats["mem_mb_max"] = round(max(mem), 1)
        stats.update(summarise_jmx(rows))
        containers[name] = stats

    lags = [r["lag"] for r in lag]
    run_s = (gen.stopped_at - gen.started_at) or 1
    return {
        "replicas": replicas,
        "target_rate_eps": rate,
        "achieved_input_rate_eps": round(gen.written / run_s, 1),
        "events_written": gen.written,
        "symbols": args.symbols,
        "speedup": args.speedup,
        "window_wall_s": 300 / args.speedup,
        "duration_s": args.duration,
        "latency_ms": {"count": len(lat), "timeouts": timeouts, "p50": pct(lat, .5),
                       "p95": pct(lat, .95), "p99": pct(lat, .99),
                       "max": round(max(lat), 1) if lat else None},
        "ui_refresh_bound_s": UI_REFRESH_SECONDS,
        "lag": {"max": max(lags) if lags else None, "final": lags[-1] if lags else None},
        "containers": containers,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rates", default="1000,5000,20000", help="comma-separated input events/s")
    p.add_argument("--replicas", default="1,2,3", help="comma-separated streams replica counts")
    p.add_argument("--symbols", type=int, default=500)
    p.add_argument("--duration", type=float, default=120, help="seconds of load per run")
    p.add_argument("--speedup", type=float, default=60, help="event time vs wall time")
    p.add_argument("--warmup", type=float, default=15, help="seconds excluded from the summary")
    p.add_argument("--drain", type=float, default=60, help="max seconds to wait for pending probes")
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--no-build", action="store_true", help="skip sbt assembly and image builds")
    p.add_argument("--keep-stack", action="store_true", help="leave the last stack running")
    args = p.parse_args()

    out = args.out or ROOT / "tests" / "results" / datetime.now().strftime("%Y%m%d-%H%M%S")
    stack = Stack(PROJECT)
    if not args.no_build:
        stack.build_jars()
    summaries = []
    first = not args.no_build
    for replicas in (int(x) for x in args.replicas.split(",")):
        for rate in (int(x) for x in args.rates.split(",")):
            summaries.append(run_once(stack, out / f"{replicas}r-{rate}eps", replicas, rate, args, first))
            first = False
    (out / "summary.json").write_text(json.dumps(summaries, indent=2))
    print(f"==> results in {out}")


if __name__ == "__main__":
    main()

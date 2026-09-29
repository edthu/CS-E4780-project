"""Per-container resource sampler for perf runs.

OS level (every container, from cgroup v2 and /proc; no agent needed):
  cpu_pct             CPU use over the sample interval, 100 = one full core
  mem_bytes           memory.current (anon + page cache charged to the container)
  mem_anon_bytes      anonymous memory (heap, RocksDB block cache, ...)
  disk_read/write_bytes  block I/O charged to the container (io.stat)
  io_stall_us         time at least one task was stalled on I/O (io.pressure "some")
  net_rx/tx_bytes     network traffic (/proc/<pid>/net/dev, excluding lo)
Counters are cumulative; perf/run_perf.py turns them into per-interval rates.

JMX level (JVM containers, scraped from the Prometheus JMX exporter agent):
see JMX_SERIES. These separate *writing to storage* from generic I/O. The
broker's storage is its own append-only commit log (segment files under
log.dirs, not a database); Streams' storage is RocksDB.
"""

from __future__ import annotations

import re
import threading
import time
from pathlib import Path

import requests

from .stack import JMX_PORT, Container, Stack

# Metric specs per service: column -> (kind, metric name(s), label filter).
# Names are the JMX exporter's lowercased catch-all form. A label value of None
# means "label must be absent" (e.g. client-level rather than per-topic series).
# Kinds:
#   sum / max    aggregate over matching series (per task, store, client ...)
#   yammer_ms    broker histogram: mean * count, i.e. cumulative milliseconds
#   ratetime     sum over series of rate(ops/s) * latency_avg(ns) -> ms spent per s
# Columns ending in _total are cumulative; run_perf.py turns them into rates.
_BROKER_LOCAL = "kafka_network_requestmetrics_{}"
_STATE = "kafka_streams_stream_state_metrics_{}"
JMX_SERIES: dict[str, dict[str, tuple]] = {
    "kafka": {
        # Time the broker spends appending produced records to the partition
        # log (its storage), and serving fetches from it.
        "jmx_log_append_ms_total": ("yammer_ms", "kafka_network_requestmetrics", {"name": "LocalTimeMs", "request": "Produce"}),
        "jmx_log_append_count_total": ("sum", "kafka_network_requestmetrics_count", {"name": "LocalTimeMs", "request": "Produce"}),
        "jmx_log_read_ms_total": ("yammer_ms", "kafka_network_requestmetrics", {"name": "LocalTimeMs", "request": "Fetch"}),
        "jmx_log_read_count_total": ("sum", "kafka_network_requestmetrics_count", {"name": "LocalTimeMs", "request": "Fetch"}),
        # fsync of log segments (on segment roll / flush; the OS does the rest).
        "jmx_log_flush_ms_total": ("yammer_ms", "kafka_log_logflushstats", {"name": "LogFlushRateAndTimeMs"}),
        "jmx_log_flush_count_total": ("sum", "kafka_log_logflushstats_count", {"name": "LogFlushRateAndTimeMs"}),
        "jmx_bytes_in_total": ("sum", "kafka_server_brokertopicmetrics_count", {"name": "BytesInPerSec", "topic": None}),
        "jmx_bytes_out_total": ("sum", "kafka_server_brokertopicmetrics_count", {"name": "BytesOutPerSec", "topic": None}),
    },
    "streams": {
        # RocksDB state stores (needs STREAMS_METRICS_LEVEL=DEBUG).
        "jmx_state_write_ms_per_s": ("ratetime", ["put", "put_all", "put_if_absent", "delete"], {}),
        "jmx_state_read_ms_per_s": ("ratetime", ["get", "fetch", "range", "prefix_scan"], {}),
        "jmx_state_flush_ms_per_s": ("ratetime", ["flush"], {}),
        "jmx_rocksdb_bytes_written_per_s": ("sum", _STATE.format("bytes_written_rate"), {}),
        "jmx_rocksdb_bytes_read_per_s": ("sum", _STATE.format("bytes_read_rate"), {}),
        "jmx_rocksdb_memtable_flush_time_avg_us": ("max", _STATE.format("memtable_flush_time_avg"), {}),
        "jmx_rocksdb_write_stall_duration_avg_us": ("max", _STATE.format("write_stall_duration_avg"), {}),
        "jmx_process_rate": ("sum", "kafka_streams_stream_thread_metrics_process_rate", {}),
        "jmx_consumer_fetch_latency_avg_ms": ("max", "kafka_consumer_consumer_fetch_manager_metrics_fetch_latency_avg", {"topic": None}),
        "jmx_producer_request_latency_avg_ms": ("max", "kafka_producer_producer_metrics_request_latency_avg", {}),
    },
    "producer": {
        "jmx_record_send_rate": ("sum", "kafka_producer_producer_metrics_record_send_rate", {}),
        "jmx_request_latency_avg_ms": ("max", "kafka_producer_producer_metrics_request_latency_avg", {}),
        "jmx_network_io_ms_total": ("sum", "kafka_producer_producer_metrics_io_time_ns", {}),
    },
    "consumer": {
        "jmx_records_consumed_rate": ("sum", "kafka_consumer_consumer_fetch_manager_metrics_records_consumed_rate", {"topic": None}),
        "jmx_fetch_latency_avg_ms": ("max", "kafka_consumer_consumer_fetch_manager_metrics_fetch_latency_avg", {"topic": None}),
    },
}


def _read_kv(path: Path) -> dict[str, int]:
    out = {}
    for line in path.read_text().splitlines():
        k, _, v = line.partition(" ")
        if v.strip().lstrip("-").isdigit():
            out[k] = int(v)
    return out


def os_sample(c: Container) -> dict[str, float]:
    cg = c.cgroup_dir
    cpu = _read_kv(cg / "cpu.stat")
    mem = _read_kv(cg / "memory.stat")
    row = {
        "cpu_usage_us": cpu["usage_usec"],
        "mem_bytes": int((cg / "memory.current").read_text()),
        "mem_anon_bytes": mem.get("anon", 0),
        "mem_file_bytes": mem.get("file", 0),
        "disk_read_bytes": 0, "disk_write_bytes": 0, "disk_read_ios": 0, "disk_write_ios": 0,
    }
    for line in (cg / "io.stat").read_text().splitlines():
        fields = dict(f.split("=") for f in line.split()[1:])
        row["disk_read_bytes"] += int(fields.get("rbytes", 0))
        row["disk_write_bytes"] += int(fields.get("wbytes", 0))
        row["disk_read_ios"] += int(fields.get("rios", 0))
        row["disk_write_ios"] += int(fields.get("wios", 0))
    for line in (cg / "io.pressure").read_text().splitlines():
        kind, *parts = line.split()
        total = int(dict(p.split("=") for p in parts)["total"])
        row["io_stall_us" if kind == "some" else "io_stall_full_us"] = total
    rx = tx = 0
    for line in Path(f"/proc/{c.pid}/net/dev").read_text().splitlines()[2:]:
        iface, data = line.split(":", 1)
        if iface.strip() == "lo":
            continue
        cols = data.split()
        rx, tx = rx + int(cols[0]), tx + int(cols[8])
    row["net_rx_bytes"], row["net_tx_bytes"] = rx, tx
    return row


_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)')
_LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def parse_prometheus(text: str) -> list[tuple[str, dict[str, str], float]]:
    out = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _LINE.match(line)
        if not m:
            continue
        try:
            value = float(m.group(3))
        except ValueError:
            continue
        out.append((m.group(1), dict(_LABEL.findall(m.group(2) or "")), value))
    return out


def _matches(labels: dict[str, str], want: dict) -> bool:
    return all((k not in labels) if v is None else labels.get(k) == v for k, v in want.items())


def jmx_sample(c: Container) -> dict[str, float]:
    specs = JMX_SERIES.get(c.service)
    if not specs:
        return {}
    text = requests.get(f"http://{c.ip}:{JMX_PORT}/metrics", timeout=5).text
    by_name: dict[str, list[tuple[dict, float]]] = {}
    for name, labels, value in parse_prometheus(text):
        if value == value:  # drop NaN (an average with no samples yet)
            by_name.setdefault(name, []).append((labels, value))

    def select(name: str, want: dict) -> list[tuple[dict, float]]:
        return [(l, v) for l, v in by_name.get(name, []) if _matches(l, want)]

    row: dict[str, float] = {}
    for column, (kind, name, want) in specs.items():
        if kind in ("sum", "max"):
            values = [v for _, v in select(name, want)]
            if values:
                row[column] = (sum if kind == "sum" else max)(values)
        elif kind == "yammer_ms":
            means = select(f"{name}_mean", want)
            counts = select(f"{name}_count", want)
            if means and counts:
                row[column] = sum(m * n for (_, m), (_, n) in zip(means, counts))
        elif kind == "ratetime":
            total_ns = 0.0
            for op in name:
                rates = {tuple(sorted(l.items())): v for l, v in select(f"kafka_streams_stream_state_metrics_{op}_rate", want)}
                for l, avg in select(f"kafka_streams_stream_state_metrics_{op}_latency_avg", want):
                    total_ns += rates.get(tuple(sorted(l.items())), 0.0) * avg
            row[column] = total_ns / 1e6
    if "jmx_network_io_ms_total" in row:
        row["jmx_network_io_ms_total"] /= 1e6  # exported in ns
    return row


class Sampler(threading.Thread):
    """Samples every container of a stack every `interval` seconds into `rows`."""

    def __init__(self, stack: Stack, interval: float = 1.0, services: tuple[str, ...] | None = None):
        super().__init__(daemon=True)
        self.stack = stack
        self.interval = interval
        self.services = services
        self.rows: list[dict] = []
        self.errors: list[str] = []
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()
        self.join()

    def run(self) -> None:
        containers = [c for c in self.stack.containers()
                      if self.services is None or c.service in self.services]
        while not self._stop.is_set():
            started = time.time()
            for c in containers:
                row = {"t": started, "service": c.service, "container": c.name}
                try:
                    row.update(os_sample(c))
                    row.update(jmx_sample(c))
                except Exception as exc:  # noqa: BLE001 - one bad scrape must not end the run
                    if len(self.errors) < 20:
                        self.errors.append(f"{c.name}: {exc}")
                self.rows.append(row)
            self._stop.wait(max(0.0, self.interval - (time.time() - started)))

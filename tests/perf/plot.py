"""Figures for the report from a run_perf.py results directory.

    python tests/perf/plot.py tests/results/<timestamp>

Writes PNGs next to the data:
  latency_cdf.png          latency CDF per run
  latency_vs_rate.png      p50/p95 latency vs input rate, one line per replica count
  cpu_mem_by_container.png mean CPU and max memory per service, per run
  <run>/timeseries.png     CPU and memory over time for each container of that run
"""

from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def load_runs(results: Path) -> list[tuple[Path, dict]]:
    runs = []
    for d in sorted(p for p in results.iterdir() if p.is_dir()):
        summary = d / "summary.json"
        if summary.exists():
            runs.append((d, json.loads(summary.read_text())))
    return runs


def read_csv(path: Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


def latency_cdf(runs, out: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 4))
    for d, s in runs:
        lat = sorted(float(r["latency_ms"]) for r in read_csv(d / "latency.csv")
                     if r["latency_ms"] not in ("", "nan"))
        if lat:
            ax.plot(lat, [(i + 1) / len(lat) for i in range(len(lat))],
                    label=f"{s['replicas']} replica(s), {s['target_rate_eps']} ev/s")
    ax.set_xlabel("event on input file -> visible on /ema (ms)")
    ax.set_ylabel("fraction of windows")
    ax.grid(alpha=.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "latency_cdf.png", dpi=150)
    plt.close(fig)


def latency_vs_rate(runs, out: Path) -> None:
    by_replicas = defaultdict(list)
    for _, s in runs:
        by_replicas[s["replicas"]].append(s)
    fig, ax = plt.subplots(figsize=(7, 4))
    for replicas, items in sorted(by_replicas.items()):
        items.sort(key=lambda s: s["target_rate_eps"])
        rates = [s["achieved_input_rate_eps"] for s in items]
        for key, style in (("p50", "-o"), ("p95", "--s")):
            ax.plot(rates, [s["latency_ms"][key] for s in items], style,
                    label=f"{replicas} replica(s) {key}")
    ax.set_xlabel("input rate (events/s)")
    ax.set_ylabel("latency (ms)")
    ax.grid(alpha=.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "latency_vs_rate.png", dpi=150)
    plt.close(fig)


def cpu_mem_by_container(runs, out: Path) -> None:
    labels = [f"{s['replicas']}r/{s['target_rate_eps']}" for _, s in runs]
    services = sorted({c["service"] for _, s in runs for c in s["containers"].values()})
    fig, (ax_cpu, ax_mem) = plt.subplots(1, 2, figsize=(12, 4))
    width = 0.8 / max(1, len(services))
    for i, svc in enumerate(services):
        cpu, mem = [], []
        for _, s in runs:
            cs = [c for c in s["containers"].values() if c["service"] == svc]
            cpu.append(sum(c.get("cpu_pct_mean") or 0 for c in cs))
            mem.append(sum(c.get("mem_mb_max") or 0 for c in cs))
        xs = [x + i * width for x in range(len(runs))]
        ax_cpu.bar(xs, cpu, width, label=svc)
        ax_mem.bar(xs, mem, width, label=svc)
    for ax, title in ((ax_cpu, "mean CPU (% of one core, summed over replicas)"),
                      (ax_mem, "max memory (MiB, summed over replicas)")):
        ax.set_xticks([x + 0.4 - width / 2 for x in range(len(runs))], labels, rotation=30)
        ax.set_title(title, fontsize=10)
        ax.grid(axis="y", alpha=.3)
    ax_cpu.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "cpu_mem_by_container.png", dpi=150)
    plt.close(fig)


def timeseries(run_dir: Path) -> None:
    rows = [r for r in read_csv(run_dir / "samples.csv") if r.get("cpu_usage_us")]
    by_container = defaultdict(list)
    for r in rows:
        by_container[r["container"]].append(r)
    t0 = min(float(r["t"]) for r in rows)
    fig, (ax_cpu, ax_mem) = plt.subplots(2, 1, figsize=(9, 6), sharex=True)
    for name, rs in sorted(by_container.items()):
        ts = [float(r["t"]) - t0 for r in rs]
        cpu = [(float(b["cpu_usage_us"]) - float(a["cpu_usage_us"])) / (float(b["t"]) - float(a["t"])) / 1e4
               for a, b in zip(rs, rs[1:])]
        ax_cpu.plot(ts[1:], cpu, label=name)
        ax_mem.plot(ts, [float(r["mem_bytes"]) / 2**20 for r in rs], label=name)
    ax_cpu.set_ylabel("CPU (% of one core)")
    ax_mem.set_ylabel("memory (MiB)")
    ax_mem.set_xlabel("seconds since start")
    for ax in (ax_cpu, ax_mem):
        ax.grid(alpha=.3)
    ax_cpu.legend(fontsize=7, ncol=2)
    fig.tight_layout()
    fig.savefig(run_dir / "timeseries.png", dpi=150)
    plt.close(fig)


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    results = Path(sys.argv[1])
    runs = load_runs(results)
    if not runs:
        sys.exit(f"no runs with summary.json under {results}")
    latency_cdf(runs, results)
    latency_vs_rate(runs, results)
    cpu_mem_by_container(runs, results)
    for d, _ in runs:
        timeseries(d)
    print(f"figures written to {results}")


if __name__ == "__main__":
    main()

"""Figures for the report from a run_perf.py or run_replay.py results directory.

    python tests/perf/plot.py tests/results/<timestamp>

Replay results (run_replay.py) produce:
  scaling.png              throughput and speedup vs streams replicas
  latency_vs_load.png      p50/p95 latency vs achieved input rate, per Streams profile
  cpu_per_event.png        CPU time per event vs achieved input rate, per service
  cpu_fit.json             per service: idle CPU cores and marginal CPU time per event

run_perf.py results produce PNGs next to the data:
  latency_cdf.png          latency CDF per run
  latency_vs_rate.png      p50/p95 latency vs input rate, one line per replica count
  cpu_mem_by_container.png mean CPU and max memory per service, per run
  <run>/timeseries.png     CPU and memory over time for each container of that run
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter, NullLocator  # noqa: E402


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


# Validated categorical order; identity is fixed per entity across figures.
PROFILE_COLOR = {"shipped": "#2a78d6", "tuned": "#eb6834"}
SERVICE_COLOR = {"streams": "#2a78d6", "kafka": "#eb6834", "producer": "#1baf7a"}
SURFACE, INK, MUTED, GRID = "#fcfcfb", "#2b2a27", "#6b6a64", "#e4e3de"


def style(ax, xlabel: str, ylabel: str, title: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_xlabel(xlabel, color=MUTED)
    ax.set_ylabel(ylabel, color=MUTED)
    ax.set_title(title, color=INK, fontsize=11, loc="left")
    ax.grid(color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(colors=MUTED, length=0)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)


def log_axes(ax) -> None:
    """Log scale on both axes, labelled in plain numbers at 1, 2 and 5 per decade."""
    ax.set_xscale("log")
    ax.set_yscale("log")
    for axis in (ax.xaxis, ax.yaxis):
        axis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
        axis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.10g}"))
        axis.set_minor_locator(NullLocator())
        axis.set_minor_formatter(NullFormatter())


def line(ax, xs, ys, color, label, dashed=False) -> None:
    ax.plot(xs, ys, color=color, linewidth=2, linestyle="--" if dashed else "-", marker="o",
            markersize=7, markeredgecolor=SURFACE, markeredgewidth=1.5, label=label)


def scaling(runs, out: Path) -> None:
    by_replicas = defaultdict(list)
    for _, s in runs:
        if s.get("kind") == "capacity":
            by_replicas[s["replicas"]].append(s)
    if not by_replicas:
        return
    replicas = sorted(by_replicas)
    mean = [statistics.fmean(s["throughput_eps"] for s in by_replicas[n]) for n in replicas]
    low = [m - min(s["throughput_eps"] for s in by_replicas[n]) for n, m in zip(replicas, mean)]
    high = [max(s["throughput_eps"] for s in by_replicas[n]) - m for n, m in zip(replicas, mean)]
    ceiling = [min(n, statistics.fmean(s["max_speedup_from_partition_skew"] for s in by_replicas[n]))
               for n in replicas]
    base = mean[0] / replicas[0]

    fig, (ax_t, ax_s) = plt.subplots(1, 2, figsize=(10, 4), facecolor=SURFACE)
    ax_t.errorbar(replicas, mean, yerr=[low, high], color=SERVICE_COLOR["streams"], linewidth=2, marker="o",
                  markersize=7, markeredgecolor=SURFACE, capsize=4)
    style(ax_t, "streams replicas (1 CPU each)", "price events/s", "Backlog drain throughput (min-max of repeats)")
    ax_t.set_xticks(replicas)
    ax_t.set_ylim(bottom=0)

    line(ax_s, replicas, [m / base for m in mean], SERVICE_COLOR["streams"], "measured")
    line(ax_s, replicas, ceiling, SERVICE_COLOR["kafka"], "limit from partition skew", dashed=True)
    ax_s.plot(replicas, replicas, color=MUTED, linewidth=1, linestyle=":", label="linear")
    style(ax_s, "streams replicas (1 CPU each)", "speedup vs one replica", "Speedup")
    ax_s.set_xticks(replicas)
    ax_s.set_ylim(bottom=0)
    ax_s.legend(frameon=False, fontsize=9, labelcolor=INK)
    fig.tight_layout()
    fig.savefig(out / "scaling.png", dpi=200, facecolor=SURFACE)
    plt.close(fig)


def latency_vs_load(runs, out: Path) -> None:
    by_profile = defaultdict(list)
    for _, s in runs:
        if s.get("kind") == "latency" and s["latency_ms"]["count"]:
            by_profile[s["profile"]].append(s)
    if not by_profile:
        return
    fig, ax = plt.subplots(figsize=(7, 4), facecolor=SURFACE)
    for profile, items in sorted(by_profile.items()):
        items.sort(key=lambda s: s["achieved_eps"])
        rates = [s["achieved_eps"] for s in items]
        color = PROFILE_COLOR.get(profile, MUTED)
        line(ax, rates, [s["latency_ms"]["p50"] / 1000 for s in items], color, f"{profile} p50")
        line(ax, rates, [s["latency_ms"]["p95"] / 1000 for s in items], color, f"{profile} p95", dashed=True)
    style(ax, "achieved input rate (price events/s, log scale)", "seconds, log scale", "Window close to visible on /ema")
    log_axes(ax)
    ax.legend(frameon=False, fontsize=9, labelcolor=INK)
    fig.tight_layout()
    fig.savefig(out / "latency_vs_load.png", dpi=200, facecolor=SURFACE)
    plt.close(fig)


def cpu_points(runs):
    """(service, seconds, events, cpu_seconds, rate, kind) for probe-free runs."""
    for _, s in runs:
        if s.get("kind") == "capacity":
            seconds, events = s["measured_seconds"], s["records"] * 0.8
        elif s.get("kind") == "sustain":
            seconds, events = s["seconds"], s["events"]
        else:
            continue
        for service, stats in s["resources"].items():
            yield service, seconds, events, stats["cpu_seconds"], events / seconds, s["kind"]


def cpu_per_event(runs, out: Path) -> None:
    by_service = defaultdict(list)
    paced = defaultdict(list)
    for service, seconds, events, cpu_s, rate, kind in cpu_points(runs):
        by_service[service].append((seconds, events, cpu_s, rate))
        if kind == "sustain":
            paced[service].append((seconds, events, cpu_s, rate))
    if not by_service:
        return
    (out / "cpu_fit.json").write_text(json.dumps({k: fit_cpu(v) for k, v in by_service.items()}, indent=2))
    if not paced:
        return
    fig, ax = plt.subplots(figsize=(7, 4), facecolor=SURFACE)
    for service in sorted(paced, key=list(SERVICE_COLOR).index):
        points = sorted(paced[service], key=lambda p: p[3])
        line(ax, [p[3] for p in points], [p[2] * 1e6 / p[1] for p in points], SERVICE_COLOR[service], service)
    style(ax, "input rate (price events/s, log scale)", "CPU time per event (µs, log scale)", "CPU time per event, paced runs")
    log_axes(ax)
    ax.legend(frameon=False, fontsize=9, labelcolor=INK)
    fig.tight_layout()
    fig.savefig(out / "cpu_per_event.png", dpi=200, facecolor=SURFACE)
    plt.close(fig)


def fit_cpu(points) -> dict:
    """Least squares for cpu_seconds = idle_cores * seconds + marginal * events."""
    ss = sum(p[0] ** 2 for p in points)
    ee = sum(p[1] ** 2 for p in points)
    se = sum(p[0] * p[1] for p in points)
    sc = sum(p[0] * p[2] for p in points)
    ec = sum(p[1] * p[2] for p in points)
    det = ss * ee - se * se
    if len(points) < 3 or det <= 1e-9 * ss * ee:
        return {"runs": len(points), "idle_cores": None, "marginal_us_per_event": None}
    return {"runs": len(points),
            "idle_cores": round((sc * ee - ec * se) / det, 4),
            "marginal_us_per_event": round((ss * ec - se * sc) / det * 1e6, 2)}


def replay_figures(runs, out: Path) -> None:
    scaling(runs, out)
    latency_vs_load(runs, out)
    cpu_per_event(runs, out)


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    results = Path(sys.argv[1])
    runs = load_runs(results)
    if not runs:
        sys.exit(f"no runs with summary.json under {results}")
    if any("kind" in s for _, s in runs):
        replay_figures(runs, results)
        print(f"figures written to {results}")
        return
    latency_cdf(runs, results)
    latency_vs_rate(runs, results)
    cpu_mem_by_container(runs, results)
    for d, _ in runs:
        timeseries(d)
    print(f"figures written to {results}")


if __name__ == "__main__":
    main()

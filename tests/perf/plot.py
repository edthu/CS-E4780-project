"""Figures for the report from a run_perf.py or run_replay.py results directory.

    python tests/perf/plot.py tests/results/<timestamp>

Replay results (run_replay.py) produce:
  scaling.png              throughput and speedup vs streams replicas
  latency_vs_load.png      p50/p95 latency vs achieved input rate, per Streams profile
  capacity_resources.png   % of CPU limit and CPU time per event, per service, vs replicas
  sustain_lag.png          consumer lag over time in each sustainability run
  latency_runs.png         every probe latency of every latency run, with p50 and p95
  table_capacity.png       the numbers of every capacity run
  table_paced.png          the numbers of every latency and sustainability run
  <run>/timeseries.png     rates, lag, probe latency, CPU and memory over time

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
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.transforms import Bbox  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, MultipleLocator, NullFormatter, NullLocator  # noqa: E402


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


# -- replay: per-run time series ---------------------------------------------

CONSUMED = "jmx_records_consumed_total"
BROKER_CPUS = 2.0
# Ordinal blue ramp for replica counts (validated as ordinal on the light surface).
REPLICA_COLOR = {1: "#86b6ef", 2: "#3987e5", 3: "#1c5cab", 6: "#0d366b"}
CRITICAL = "#d03b3b"
MEASURED_BAND = "#f0efec"


def per_second(rows, key: str, service: str, t0: float, counter: bool) -> tuple[list[int], list[float]]:
    """A per-second series summed over the service's containers: the rate of a
    cumulative counter, or the level of a gauge carried forward between samples."""
    by = defaultdict(list)
    for r in rows:
        if r["service"] == service and r.get(key) not in ("", None):
            by[r["container"]].append((float(r["t"]) - t0, float(r[key])))
    total: dict[int, float] = defaultdict(float)
    for points in by.values():
        if counter:
            for (ta, a), (tb, b) in zip(points, points[1:]):
                if tb > ta and b >= a:
                    for sec in range(round(ta) + 1, round(tb) + 1):
                        total[sec] += (b - a) / (tb - ta)
        else:
            level = {round(t): v for t, v in points}
            last = None
            for sec in range(min(level), max(level) + 1):
                last = level.get(sec, last)
                total[sec] += last
    xs = sorted(total)
    return xs, [total[x] for x in xs]


def smooth(ys: list[float], k: int = 5, how=statistics.fmean) -> list[float]:
    half = k // 2
    return [how(ys[max(0, i - half): i + half + 1]) for i in range(len(ys))]


def resample(xs, ys, k: int, how=statistics.fmean) -> tuple[list[float], list[float]]:
    """A rolling window for short runs; k-second buckets for long ones, so a
    one-hour run is drawn with about 150 points instead of 3,600."""
    if k <= 5:
        return list(xs), smooth(list(ys), k, how)
    buckets = defaultdict(list)
    for x, y in zip(xs, ys):
        buckets[int(x // k)].append(y)
    keys = sorted(buckets)
    return [(b + 0.5) * k for b in keys], [how(buckets[b]) for b in keys]


def measured_span(s: dict, rows, lag_rows, t0: float) -> tuple[float, float] | None:
    """The interval the run's summary numbers are taken over."""
    if s["kind"] == "capacity":
        xs, level = per_second(rows, CONSUMED, "streams", t0, counter=False)
        hits = [next((x for x, v in zip(xs, level) if v >= f * s["records"]), None) for f in (0.1, 0.9)]
        return tuple(hits) if None not in hits else None
    if not lag_rows:
        return None
    base = int(lag_rows[0]["produced"])
    moving = [r for r in lag_rows if int(r["produced"]) > base]
    if not moving:
        return None
    last = moving[-1]["produced"]
    end = next(r for r in moving if r["produced"] == last)
    return float(moving[0]["t"]) - t0, float(end["t"]) - t0


def describe(s: dict) -> str:
    if s["kind"] == "capacity":
        return (f"backlog drain, {s['replicas']} replica(s), {s['profile']}: "
                f"{s['throughput_eps']:,.0f} events/s, final lag {s['final_lag']}")
    text = (f"{s['scenario']} slice at {s['speed']:g}x, {s['replicas']} replica(s), {s['profile']}: "
            f"{s['achieved_eps']:,.0f} events/s, max lag {s['lag']['max']:,}, final lag {s['lag']['final']:,}")
    if s.get("latency_ms"):
        lat = s["latency_ms"]
        text += f"\nprobe latency p50 {lat['p50'] / 1000:.2f} s, p95 {lat['p95'] / 1000:.2f} s, {lat['timeouts']} timeouts"
    return text


def replay_timeseries(run_dir: Path, s: dict) -> None:
    rows = read_csv(run_dir / "samples.csv")
    if not rows:
        return
    lag_rows = read_csv(run_dir / "lag.csv") if (run_dir / "lag.csv").exists() else []
    probes = read_csv(run_dir / "latency.csv") if (run_dir / "latency.csv").exists() else []
    t0 = min(float(r["t"]) for r in rows)
    services = [x for x in SERVICE_COLOR if any(r["service"] == x for r in rows)]
    panels = ["rate", "lag"] + (["latency"] if probes else []) + ["cpu", "mem"]
    fig, axes = plt.subplots(len(panels), 1, figsize=(10, 2.5 * len(panels) + 0.8), sharex=True, facecolor=SURFACE)
    ax = dict(zip(panels, axes))
    span = measured_span(s, rows, lag_rows, t0)
    duration = max(float(r["t"]) for r in rows) - t0
    k = max(5, round(duration / 150))  # about 150 points across the run
    if k > 5:
        # The real feed bursts about every 15 s; other bucket sizes alias with it.
        k = 15 * -(-k // 15)

    xs, ys = per_second(rows, CONSUMED, "streams", t0, counter=True)
    if lag_rows:
        pts = [(float(r["t"]) - t0, int(r["produced"])) for r in lag_rows]
        rx = [b[0] for a, b in zip(pts, pts[1:]) if b[0] > a[0]]
        ry = [(b[1] - a[1]) / (b[0] - a[0]) for a, b in zip(pts, pts[1:]) if b[0] > a[0]]
        ax["rate"].plot(*resample(rx, ry, k), color=SERVICE_COLOR["producer"], linewidth=2, label="sent to Kafka (producer)")
    ax["rate"].plot(*resample(xs, ys, k), color=SERVICE_COLOR["streams"], linewidth=2, label="consumed by Streams")
    style(ax["rate"], "", f"events/s ({k} s mean)", "Input and processing rate")
    ax["rate"].set_ylim(bottom=0)

    if lag_rows:
        ax["lag"].plot(*resample([float(r["t"]) - t0 for r in lag_rows], [int(r["lag"]) for r in lag_rows], k, max),
                       color=SERVICE_COLOR["streams"], linewidth=2)
        style(ax["lag"], "", f"records (max over {k} s)" if k > 5 else "records", "Consumer lag (committed offsets)")
    else:
        lx, level = per_second(rows, CONSUMED, "streams", t0, counter=False)
        ax["lag"].plot(lx, [max(0.0, s["records"] - v) for v in level], color=SERVICE_COLOR["streams"], linewidth=2)
        style(ax["lag"], "", "records", "Records left to process")
    ax["lag"].set_ylim(bottom=0)

    if probes:
        done = [p for p in probes if p["timed_out"] == "0"]
        ax["latency"].scatter([int(p["t0_ms"]) / 1000 - t0 for p in done], [float(p["latency_ms"]) / 1000 for p in done],
                              s=14, color=PROFILE_COLOR.get(s["profile"], MUTED), edgecolors=SURFACE, linewidths=0.5,
                              label="probe")
        lost = [p for p in probes if p["timed_out"] != "0"]
        if lost:
            top = max([float(p["latency_ms"]) / 1000 for p in done] + [1.0])
            ax["latency"].scatter([int(p["t0_ms"]) / 1000 - t0 for p in lost], [top] * len(lost), marker="x",
                                  color=CRITICAL, label="timed out")
        ax["latency"].set_yscale("log")
        ax["latency"].yaxis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
        ax["latency"].yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.10g}"))
        ax["latency"].yaxis.set_minor_locator(NullLocator())
        ax["latency"].yaxis.set_minor_formatter(NullFormatter())
        style(ax["latency"], "", "seconds, log scale", "Probe latency: window close to visible on /ema (by send time)")

    for service in services:
        cx, cy = per_second(rows, "cpu_usage_us", service, t0, counter=True)
        ax["cpu"].plot(*resample(cx, [v / 1e6 for v in cy], k), color=SERVICE_COLOR[service], linewidth=2, label=service)
        mx, my = per_second(rows, "mem_bytes", service, t0, counter=False)
        ax["mem"].plot(*resample(mx, [v / 2**20 for v in my], k, max), color=SERVICE_COLOR[service], linewidth=2, label=service)
    limits = {"streams": s["replicas"] * s.get("cpus_per_replica", 1.0), "kafka": BROKER_CPUS}
    for service, limit in limits.items():
        if service in services:
            ax["cpu"].axhline(limit, color=SERVICE_COLOR[service], linewidth=1, linestyle=":",
                              label=f"{service} limit ({limit:g})")
    style(ax["cpu"], "", f"CPU cores ({k} s mean)", "CPU per service (summed over replicas)")
    ax["cpu"].set_ylim(bottom=0)
    style(ax["mem"], "seconds since sampling started", "MiB", "Memory per service (summed over replicas)")
    ax["mem"].set_ylim(bottom=0)

    if duration > 900:
        step = 600 if duration > 3000 else 300
        axes[-1].xaxis.set_major_locator(MultipleLocator(step))
        axes[-1].xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 60:g}"))
        axes[-1].set_xlabel("minutes since sampling started", color=MUTED)
    for key in ("rate", "lag", "mem"):
        ax[key].yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    for a in axes:
        if span:
            a.axvspan(*span, color=MEASURED_BAND, zorder=0, label="measured interval" if a is axes[0] else None)
        if a.get_legend_handles_labels()[0]:
            a.legend(frameon=False, fontsize=8, labelcolor=INK, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    fig.suptitle(f"{run_dir.name}\n{describe(s)}", x=0.01, ha="left", color=INK, fontsize=10)
    fig.tight_layout()
    fig.savefig(run_dir / "timeseries.png", dpi=150, facecolor=SURFACE)
    plt.close(fig)


# -- replay: cross-run figures and tables --------------------------------------

def capacity_resources(runs, out: Path) -> None:
    by_replicas = defaultdict(list)
    for _, s in runs:
        if s["kind"] == "capacity":
            by_replicas[s["replicas"]].append(s)
    if not by_replicas:
        return
    replicas = sorted(by_replicas)
    labels = {"streams": "Streams (1 CPU per replica)", "kafka": "Kafka broker (2 CPUs)"}
    fig, (ax_p, ax_u) = plt.subplots(1, 2, figsize=(11, 4), facecolor=SURFACE)
    width = 0.38
    for i, service in enumerate(labels):
        pct_ = [statistics.fmean(s["resources"][service]["pct_of_limit"] for s in by_replicas[n]) for n in replicas]
        us = [statistics.fmean(s["resources"][service]["cpu_us_per_event"] for s in by_replicas[n]) for n in replicas]
        xs = [k + (i - 0.5) * width for k in range(len(replicas))]
        for a, values, fmt in ((ax_p, pct_, "{:.0f}%"), (ax_u, us, "{:.0f}")):
            a.bar(xs, values, width, color=SERVICE_COLOR[service], edgecolor=SURFACE, linewidth=2, label=labels[service])
            for x, v in zip(xs, values):
                a.text(x, v, fmt.format(v), ha="center", va="bottom", fontsize=8, color=INK)
    ax_p.axhline(100, color=MUTED, linewidth=1, linestyle=":")
    style(ax_p, "streams replicas", "% of the service's CPU limit", "CPU use during the drain (mean of repeats)")
    style(ax_u, "streams replicas", "µs of CPU per event", "CPU time per event during the drain")
    for a in (ax_p, ax_u):
        a.set_xticks(range(len(replicas)), [str(n) for n in replicas])
        a.grid(axis="x", visible=False)
    fig.legend(*ax_p.get_legend_handles_labels(), frameon=False, fontsize=9, labelcolor=INK,
               loc="lower center", ncol=2)
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(out / "capacity_resources.png", dpi=200, facecolor=SURFACE)
    plt.close(fig)


def slice_rate(s: dict) -> float:
    seconds = (datetime.fromisoformat(s["to"]) - datetime.fromisoformat(s["from"])).total_seconds()
    return s["events"] / seconds


def sustain_lag(runs, out: Path) -> None:
    items = sorted(((d, s) for d, s in runs if s["kind"] == "sustain"), key=lambda r: r[1]["replicas"])
    if not items:
        return
    fig, axes = plt.subplots(1, len(items), figsize=(3.4 * len(items), 3.6), facecolor=SURFACE, squeeze=False)
    for a, (d, s) in zip(axes[0], items):
        lag_rows = read_csv(d / "lag.csv")
        t0 = float(lag_rows[0]["t"])
        a.plot([float(r["t"]) - t0 for r in lag_rows], [int(r["lag"]) / 1000 for r in lag_rows],
               color=REPLICA_COLOR.get(s["replicas"], SERVICE_COLOR["streams"]), linewidth=2)
        target = s["speed"] * slice_rate(s)
        style(a, "seconds", "lag (thousand records)",
              f"{s['replicas']} replica(s)\n{s['achieved_eps']:,.0f} of {target:,.0f} events/s")
        a.set_ylim(bottom=0)
    fig.suptitle("Sustainability: consumer lag during paced replay (each panel has its own scale)",
                 x=0.01, ha="left", color=INK, fontsize=11)
    fig.tight_layout()
    fig.savefig(out / "sustain_lag.png", dpi=200, facecolor=SURFACE)
    plt.close(fig)


def latency_runs(runs, out: Path) -> None:
    items = [(d, s) for d, s in runs if s["kind"] == "latency" and (d / "latency.csv").exists()]
    if not items:
        return
    items.sort(key=lambda r: (r[1]["scenario"], r[1]["speed"], r[1]["profile"]))
    fig, ax = plt.subplots(figsize=(10, 0.7 * len(items) + 1.6), facecolor=SURFACE)
    labels = []
    for row, (d, s) in enumerate(items):
        probes = read_csv(d / "latency.csv")
        first = min(int(p["window_start"]) for p in probes)
        values = [float(p["latency_ms"]) / 1000 for p in probes if int(p["window_start"]) > first and p["timed_out"] == "0"]
        color = PROFILE_COLOR.get(s["profile"], MUTED)
        ax.scatter(values, [row + 0.25 * ((i % 7) - 3) / 3 for i in range(len(values))], s=12, color=color,
                   alpha=0.7, edgecolors="none")
        lat = s["latency_ms"]
        ax.plot([lat["p50"] / 1000] * 2, [row - 0.35, row + 0.35], color=INK, linewidth=2)
        ax.plot([lat["p95"] / 1000] * 2, [row - 0.35, row + 0.35], color=INK, linewidth=1.5, linestyle="--")
        labels.append(f"{s['scenario']} {s['speed']:g}x, {s['profile']}\n{s['achieved_eps']:,.0f} ev/s, "
                      f"{lat['count']} probes, {lat['timeouts']} timeouts")
    ax.set_yticks(range(len(items)), labels, fontsize=8)
    ax.invert_yaxis()
    ax.set_xscale("log")
    ax.xaxis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.10g}"))
    ax.xaxis.set_minor_locator(NullLocator())
    style(ax, "seconds from window close to visible on /ema (log scale)", "", "Probe latency per run (one dot per probe)")
    ax.grid(axis="y", visible=False)
    handles = [plt.Line2D([], [], marker="o", linestyle="", color=c, label=p) for p, c in PROFILE_COLOR.items()
               if any(s["profile"] == p for _, s in items)]
    handles += [plt.Line2D([], [], color=INK, linewidth=2, label="p50"),
                plt.Line2D([], [], color=INK, linewidth=1.5, linestyle="--", label="p95")]
    ax.legend(handles=handles, frameon=False, fontsize=8, labelcolor=INK, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    fig.tight_layout()
    fig.savefig(out / "latency_runs.png", dpi=200, facecolor=SURFACE)
    plt.close(fig)


def table_image(headers: list[str], rows: list[list[str]], title: str, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(min(1.15 * len(headers) + 2, 19), 0.32 * len(rows) + 1.1), facecolor=SURFACE)
    ax.axis("off")
    table = ax.table(cellText=rows, colLabels=headers, loc="upper left", cellLoc="right", colLoc="right")
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.auto_set_column_width(list(range(len(headers))))
    for (r, c), cell in table.get_celld().items():
        cell.set_edgecolor(GRID)
        cell.set_text_props(color=INK, fontweight="bold" if r == 0 else "normal")
        cell.set_facecolor("#f0efec" if r == 0 else (SURFACE if r % 2 else "#f6f6f4"))
        if c == 0:
            cell.set_text_props(ha="left")
            cell._loc = "left"
    heading = ax.set_title(title, color=INK, fontsize=11, loc="left")
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    extent = Bbox.union([table.get_window_extent(renderer), heading.get_window_extent(renderer)])
    fig.savefig(path, dpi=200, facecolor=SURFACE,
                bbox_inches=extent.transformed(fig.dpi_scale_trans.inverted()).padded(0.15))
    plt.close(fig)


def tables(runs, out: Path) -> None:
    def res(s, service, key):
        v = s["resources"].get(service, {}).get(key)
        return "" if v is None else f"{v:,.1f}"

    capacity = sorted(((d, s) for d, s in runs if s["kind"] == "capacity"), key=lambda r: (r[1]["replicas"], r[0].name))
    if capacity:
        table_image(
            ["run", "records", "events/s", "measured s", "skew limit", "final lag", "duplicates",
             "streams % limit", "streams µs/event", "kafka % limit", "kafka µs/event", "streams max MiB"],
            [[d.name, f"{s['records']:,}", f"{s['throughput_eps']:,.0f}", f"{s['measured_seconds']:g}",
              f"{s['max_speedup_from_partition_skew']:.2f}x", f"{s['final_lag']}", f"{s['duplicate_fraction']:.3%}",
              res(s, "streams", "pct_of_limit"), res(s, "streams", "cpu_us_per_event"),
              res(s, "kafka", "pct_of_limit"), res(s, "kafka", "cpu_us_per_event"), res(s, "streams", "mem_mb_max")]
             for d, s in capacity],
            "Capacity runs (backlog drain); rate and CPU over the 10-90% interval", out / "table_capacity.png")

    paced = sorted(((d, s) for d, s in runs if s["kind"] in ("latency", "sustain")),
                   key=lambda r: (r[1]["kind"], r[1]["scenario"], r[1]["replicas"], r[1]["speed"], r[1]["profile"]))
    if paced:
        def ms(s, key):
            lat = s.get("latency_ms")
            return "" if not lat or lat.get(key) is None else f"{lat[key] / 1000:.2f}"
        table_image(
            ["run", "target ev/s", "achieved ev/s", "max lag", "final lag", "p50 s", "p95 s", "max s", "timeouts",
             "streams % limit", "streams µs/event", "kafka % limit", "producer cores"],
            [[d.name, f"{s['speed'] * slice_rate(s):,.0f}", f"{s['achieved_eps']:,.0f}", f"{s['lag']['max']:,}",
              f"{s['lag']['final']:,}", ms(s, "p50"), ms(s, "p95"), ms(s, "max"),
              str(s["latency_ms"]["timeouts"]) if s.get("latency_ms") else "",
              res(s, "streams", "pct_of_limit"), res(s, "streams", "cpu_us_per_event"),
              res(s, "kafka", "pct_of_limit"), res(s, "producer", "mean_cores")]
             for d, s in paced],
            "Paced runs (latency and sustainability); rate, lag and CPU over the slice", out / "table_paced.png")


def replay_figures(runs, out: Path) -> None:
    scaling(runs, out)
    latency_vs_load(runs, out)
    capacity_resources(runs, out)
    sustain_lag(runs, out)
    latency_runs(runs, out)
    tables(runs, out)
    for d, s in runs:
        replay_timeseries(d, s)


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

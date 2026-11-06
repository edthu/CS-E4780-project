"""Analyze late price events and their estimated EMA impact in a daily CSV."""

from __future__ import annotations

import csv
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .reference import WINDOW_MS, ema, partition_of, to_epoch_ms

CEST = ZoneInfo("Europe/Amsterdam")


def parse_row(row: dict[str, str]) -> tuple[str, str, float, datetime] | None:
    normalized = {key.strip().lower(): value or "" for key, value in row.items() if key}
    symbol = normalized.get("id", "").strip()
    security_type = normalized.get("sectype", "").strip()
    price_text = normalized.get("last", "").strip()
    if not symbol or not security_type or not price_text:
        return None

    try:
        price = float(price_text)
        if not math.isfinite(price) or price <= 0:
            return None
        trading_date = normalized.get("trading date", "").strip()
        trading_time = normalized.get("trading time", "").strip()
        if not trading_date or not trading_time:
            trading_date = normalized.get("date", "").strip()
            trading_time = normalized.get("time", "").strip()
        try:
            timestamp = datetime.strptime(
                f"{trading_date} {trading_time}", "%d-%m-%Y %H:%M:%S.%f"
            )
        except ValueError:
            timestamp = datetime.strptime(
                f"{trading_date} {trading_time}", "%d-%m-%Y %H:%M:%S"
            )
    except ValueError:
        return None
    return symbol, security_type, price, timestamp


def _csv_rows(path: Path):
    source = path.open(encoding="utf-8-sig", newline="")
    rows = (line for line in source if not line.startswith("#"))
    return source, csv.DictReader(rows)


def _compute_points(
    closes: dict[tuple[str, int], tuple[int, float]],
    stream_time: dict[int, int],
    partitions: int,
) -> dict[tuple[str, int], dict]:
    points: dict[tuple[str, int], dict] = {}
    ema_by_symbol: dict[str, tuple[float, float]] = defaultdict(lambda: (0.0, 0.0))
    for (symbol, start), (_, close) in sorted(closes.items()):
        partition = partition_of(symbol, partitions)
        if start + WINDOW_MS > stream_time[partition]:
            continue
        previous_fast, previous_slow = ema_by_symbol[symbol]
        fast, slow = ema(previous_fast, close, 38), ema(previous_slow, close, 100)
        signal = None
        if fast > slow and previous_fast <= previous_slow:
            signal = "BUY"
        elif fast < slow and previous_fast >= previous_slow:
            signal = "SELL"
        points[(symbol, start)] = {
            "close": close,
            "ema38": fast,
            "ema100": slow,
            "signal": signal,
        }
        ema_by_symbol[symbol] = (fast, slow)
    return points


def analyze_full_day_csv(source_path: Path, partitions: int = 6) -> dict:
    """Analyze the whole CSV in source order using the no-grace partition watermark."""
    watermarks: dict[int, int] = defaultdict(lambda: -1)
    actual_closes: dict[tuple[str, int], tuple[int, float]] = {}
    all_closes: dict[tuple[str, int], tuple[int, float]] = {}
    events_per_window: Counter[int] = Counter()
    late_per_window: Counter[int] = Counter()
    late_symbols_per_window: dict[int, set[str]] = defaultdict(set)
    late_lags_ms: list[int] = []
    hour_counts: Counter[int] = Counter()
    first_event_ms: int | None = None
    max_elapsed_from_first_ms = 0
    event_count = 0
    rows_seen = 0

    source, reader = _csv_rows(source_path)
    try:
        if not reader.fieldnames:
            raise ValueError(f"No CSV header found in {source_path}")
        for row in reader:
            rows_seen += 1
            parsed = parse_row(row)
            if parsed is None:
                continue
            symbol, _, price, timestamp = parsed
            timestamp_ms = to_epoch_ms(timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f"))
            partition = partition_of(symbol, partitions)
            start = timestamp_ms - timestamp_ms % WINDOW_MS
            window_end = start + WINDOW_MS
            key = (symbol, start)
            event_count += 1
            events_per_window[start] += 1
            hour_counts[timestamp.hour] += 1
            if first_event_ms is None:
                first_event_ms = timestamp_ms
            max_elapsed_from_first_ms = max(
                max_elapsed_from_first_ms, timestamp_ms - first_event_ms
            )

            previous = all_closes.get(key)
            if previous is None or timestamp_ms >= previous[0]:
                all_closes[key] = (timestamp_ms, price)

            if window_end <= watermarks[partition]:
                late_per_window[start] += 1
                late_symbols_per_window[start].add(symbol)
                late_lags_ms.append(watermarks[partition] - window_end)
            else:
                previous = actual_closes.get(key)
                if previous is None or timestamp_ms >= previous[0]:
                    actual_closes[key] = (timestamp_ms, price)
            watermarks[partition] = max(watermarks[partition], timestamp_ms)
    finally:
        source.close()

    final_stream_time = dict(watermarks)
    actual_points = _compute_points(actual_closes, final_stream_time, partitions)
    all_points = _compute_points(all_closes, final_stream_time, partitions)
    keys = actual_points.keys() | all_points.keys()
    close_changes = 0
    ema_changes = 0
    signal_changes = 0
    missing_actual_points = 0
    fast_deltas: list[float] = []
    slow_deltas: list[float] = []
    for key in keys:
        actual = actual_points.get(key)
        ideal = all_points.get(key)
        if actual is None:
            missing_actual_points += 1
            signal_changes += ideal is not None and ideal["signal"] is not None
            continue
        if ideal is None:
            continue
        close_changes += actual["close"] != ideal["close"]
        fast_delta = abs(actual["ema38"] - ideal["ema38"])
        slow_delta = abs(actual["ema100"] - ideal["ema100"])
        fast_deltas.append(fast_delta)
        slow_deltas.append(slow_delta)
        ema_changes += fast_delta > 1e-12 or slow_delta > 1e-12
        signal_changes += actual["signal"] != ideal["signal"]

    late_total = sum(late_per_window.values())
    sorted_lags = sorted(late_lags_ms)
    percentile = lambda fraction: (
        sorted_lags[round((len(sorted_lags) - 1) * fraction)] if sorted_lags else 0
    )
    grace_recovery = {
        grace_ms: sum(lag < grace_ms for lag in late_lags_ms)
        for grace_ms in (1_000, 10_000, 60_000, 300_000, 900_000, 3_600_000)
    }
    return {
        "rows_seen": rows_seen,
        "event_count": event_count,
        "first_event_ms": first_event_ms,
        "max_elapsed_from_first_ms": max_elapsed_from_first_ms,
        "events_per_window": events_per_window,
        "late_per_window": late_per_window,
        "late_symbols_per_window": late_symbols_per_window,
        "hour_counts": hour_counts,
        "late_total": late_total,
        "late_rate": late_total / event_count if event_count else 0.0,
        "late_lags_ms": late_lags_ms,
        "late_lag_p50_ms": percentile(0.50),
        "late_lag_p95_ms": percentile(0.95),
        "late_lag_p99_ms": percentile(0.99),
        "late_lag_max_ms": max(late_lags_ms, default=0),
        "grace_recovery": grace_recovery,
        "late_windows": sum(count > 0 for count in late_per_window.values()),
        "late_symbols": len(
            set().union(*late_symbols_per_window.values())
            if late_symbols_per_window
            else set()
        ),
        "actual_point_count": len(actual_points),
        "all_events_point_count": len(all_points),
        "close_changes": close_changes,
        "ema_changes": ema_changes,
        "signal_changes": signal_changes,
        "missing_actual_points": missing_actual_points,
        "mean_abs_ema38_delta": (
            sum(fast_deltas) / len(fast_deltas) if fast_deltas else 0.0
        ),
        "max_abs_ema38_delta": max(fast_deltas, default=0.0),
        "mean_abs_ema100_delta": (
            sum(slow_deltas) / len(slow_deltas) if slow_deltas else 0.0
        ),
        "max_abs_ema100_delta": max(slow_deltas, default=0.0),
        "actual_points": actual_points,
        "all_events_points": all_points,
    }


def render_full_day_report(result: dict) -> str:
    first_event_ms = result["first_event_ms"]
    first_event = (
        datetime.fromtimestamp(first_event_ms / 1000, CEST).isoformat()
        if first_event_ms is not None
        else "n/a"
    )
    rows = [
        "Full-day no-grace lateness analysis (source-order CSV model)",
        f"CSV rows scanned: {result['rows_seen']:,}",
        f"Valid price events: {result['event_count']:,}",
        f"First valid event: {first_event} Europe/Amsterdam",
        "Speed-1 replay span from first event to latest event: "
        f"{result['max_elapsed_from_first_ms'] / 3_600_000:.2f} hours "
        "(producer pacing only; excludes startup/processing overhead)",
        f"Late events: {result['late_total']:,} "
        f"({result['late_rate']:.6%}); distinct affected symbols: {result['late_symbols']:,}",
        f"Five-minute windows with late events: {result['late_windows']:,}",
        "Arrival lateness (partition watermark minus window end), p50/p95/p99/max: "
        f"{result['late_lag_p50_ms'] / 1000:.3f}s / "
        f"{result['late_lag_p95_ms'] / 1000:.3f}s / "
        f"{result['late_lag_p99_ms'] / 1000:.3f}s / "
        f"{result['late_lag_max_ms'] / 1000:.3f}s",
        "Late events that would fall within grace (1s/10s/1m/5m/15m/1h): "
        + " / ".join(
            f"{count:,}"
            for count in result["grace_recovery"].values()
        ),
        f"Closed actual / all-events counterfactual EMA points: "
        f"{result['actual_point_count']:,} / {result['all_events_point_count']:,}",
        f"Changed closes / EMA points / signals: {result['close_changes']:,} / "
        f"{result['ema_changes']:,} / {result['signal_changes']:,}",
        f"Missing actual EMA points vs counterfactual: {result['missing_actual_points']:,}",
        f"Mean |dEMA38| / max: {result['mean_abs_ema38_delta']:.6g} / "
        f"{result['max_abs_ema38_delta']:.6g}",
        f"Mean |dEMA100| / max: {result['mean_abs_ema100_delta']:.6g} / "
        f"{result['max_abs_ema100_delta']:.6g}",
        "",
        "Hour (Amsterdam)  price events  late events  late rate",
    ]
    for hour in sorted(result["hour_counts"]):
        events = sum(
            count
            for start, count in result["events_per_window"].items()
            if datetime.fromtimestamp(start / 1000, CEST).hour == hour
        )
        late = sum(
            count
            for start, count in result["late_per_window"].items()
            if datetime.fromtimestamp(start / 1000, CEST).hour == hour
        )
        rows.append(
            f"{hour:>4}:00            {events:>12,} {late:>12,} "
            f"{(late / events if events else 0):>10.6%}"
        )
    return "\n".join(rows)


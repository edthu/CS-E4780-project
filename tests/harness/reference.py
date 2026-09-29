"""Independent reference for Query 1 and Query 2, written from the assignment.

Semantics (assignment Sec. 3.1-3.3, plus the Kafka Streams facts the system is
built on):
  * events are grouped by symbol; the producer keys by symbol, so a symbol's
    partition is murmur2(symbol) % partitions, like Kafka's default partitioner;
  * 5-minute tumbling windows [start, start+5min) on event time, where
    timestamps are CEST wall-clock strings (Europe/Amsterdam);
  * Close_{s,w} is the price of the event with the latest timestamp in w;
  * windows are evaluated once the next one starts: a window closes when its
    partition's stream time (max event time seen) reaches its end, and an event
    whose window has already closed is dropped (no grace period);
  * EMA^j_{s,w_i} = Close * 2/(1+j) + EMA^j_{s,w_{i-1}} * (1 - 2/(1+j)), EMA_{s,w0} = 0;
  * BUY iff EMA38 > EMA100 now and EMA38 <= EMA100 in the previous window,
    SELL iff EMA38 < EMA100 now and EMA38 >= EMA100 in the previous window.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from kafka.partitioner.default import murmur2

WINDOW_MS = 5 * 60 * 1000
CEST = ZoneInfo("Europe/Amsterdam")


def to_epoch_ms(timestamp: str) -> int:
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=CEST)
    return round(dt.timestamp() * 1000)


def partition_of(symbol: str, partitions: int) -> int:
    return (murmur2(symbol.encode("utf-8")) & 0x7FFFFFFF) % partitions


def ema(previous: float, close: float, j: int) -> float:
    alpha = 2.0 / (1 + j)
    return close * alpha + previous * (1 - alpha)


def compute(lines: list[str], partitions: int = 6) -> dict[str, list[dict]]:
    """Return, per symbol, the closed windows in order with close/EMAs/signal."""
    stream_time: dict[int, int] = defaultdict(lambda: -1)
    # (symbol, windowStart) -> (latest ts, close); insertion order is irrelevant.
    open_windows: dict[tuple[str, int], tuple[int, float]] = {}

    for line in lines:
        e = json.loads(line)
        symbol, ts = e["symbol"], to_epoch_ms(e["timestamp"])
        part = partition_of(symbol, partitions)
        stream_time[part] = max(stream_time[part], ts)
        start = ts - ts % WINDOW_MS
        if start + WINDOW_MS <= stream_time[part]:
            continue  # window already closed: late event, dropped
        key = (symbol, start)
        prev = open_windows.get(key)
        if prev is None or ts >= prev[0]:
            open_windows[key] = (ts, e["price"])

    result: dict[str, list[dict]] = {}
    for (symbol, start), (_, close) in sorted(open_windows.items()):
        if start + WINDOW_MS > stream_time[partition_of(symbol, partitions)]:
            continue  # still open at the end of the input: never evaluated
        points = result.setdefault(symbol, [])
        prev_fast = points[-1]["ema38"] if points else 0.0
        prev_slow = points[-1]["ema100"] if points else 0.0
        fast, slow = ema(prev_fast, close, 38), ema(prev_slow, close, 100)
        signal = None
        if fast > slow and prev_fast <= prev_slow:
            signal = "BUY"
        elif fast < slow and prev_fast >= prev_slow:
            signal = "SELL"
        points.append({"windowStart": start, "windowEnd": start + WINDOW_MS,
                       "close": close, "ema38": fast, "ema100": slow, "signal": signal})
    return result

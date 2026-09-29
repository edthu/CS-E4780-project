"""End-to-end test of the containerised pipeline.

A crafted dataset is appended to the file the producer follows; the results
are read back the way real users get them (the streams /ema query API, the
advisories/symbols/processed topics, the UI health endpoint) and compared with
tests/harness/reference.py, which implements the assignment's definitions
independently of the Scala code.
"""

from __future__ import annotations

import json
import math
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import requests
from kafka import KafkaConsumer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import reference  # noqa: E402
from harness.events import EventWriter, event_line  # noqa: E402
from harness.stack import EVENTS_FILE, KAFKA_BOOTSTRAP, UI_URL, Stack  # noqa: E402

PROJECT = "trading-e2e"
TIMEOUT = 90
WINDOW = timedelta(minutes=5)
START = datetime(2021, 11, 8, 8, 0, 0)  # CEST wall clock, like the data set
WINDOWS = 80

# Close price per window. The EMAs start from 0, so EMA38 > EMA100 from the
# first window (BUY for everyone); a crossover later on needs a long run-up for
# EMA100 to catch up, then a crash (SELL) and, for DOWNUP, a recovery (BUY).
SERIES = {
    "DOWNUP.NL": [100.0] * 60 + [1.0] * 12 + [200.0] * 8,   # BUY, SELL, BUY
    "UPDOWN.FR": [100.0] * 60 + [150.0] * 8 + [1.0] * 12,   # BUY, SELL
    "FLAT.ETR": [50.0] * WINDOWS,                            # BUY only
    "ORDER.FR": [20.0 + (i % 3) for i in range(WINDOWS)],    # ordering edge cases
}
OUT_OF_ORDER_WINDOW = 3   # ORDER.FR: an earlier-timestamped tick arrives last
LATE_WINDOW = 5           # ORDER.FR: a tick for this window arrives much later


def build_dataset() -> list[str]:
    """Ticks in global event-time order, plus the two ORDER.FR edge cases."""
    ticks = []  # (timestamp, symbol, price)
    for symbol, closes in SERIES.items():
        for w, close in enumerate(closes):
            base = START + w * WINDOW
            ticks += [
                (base + timedelta(seconds=30), symbol, round(close * 1.01, 4)),
                (base + timedelta(seconds=150), symbol, round(close * 0.99, 4)),
                (base + timedelta(seconds=270), symbol, close),  # the window close
            ]
    ticks.sort()
    lines = [event_line(s, p, t) for t, s, p in ticks]

    # Out of order: arrives after the window's closing tick but carries an
    # earlier timestamp, so it must not become Close.
    ooo_base = START + OUT_OF_ORDER_WINDOW * WINDOW
    ooo_after = event_line("ORDER.FR", SERIES["ORDER.FR"][OUT_OF_ORDER_WINDOW],
                           ooo_base + timedelta(seconds=270))
    lines.insert(lines.index(ooo_after) + 1,
                 event_line("ORDER.FR", 999.0, ooo_base + timedelta(seconds=60)))

    # Late: a tick for LATE_WINDOW sent once its window has long closed.
    late_ts = START + LATE_WINDOW * WINDOW + timedelta(seconds=290)
    anchor = event_line("ORDER.FR", SERIES["ORDER.FR"][LATE_WINDOW + 10],
                        START + (LATE_WINDOW + 10) * WINDOW + timedelta(seconds=270))
    lines.insert(lines.index(anchor) + 1, event_line("ORDER.FR", 555.0, late_ts))

    # Closers: one tick per symbol in the next window pushes every partition's
    # stream time past the last data window, so all WINDOWS windows close.
    closer = START + WINDOWS * WINDOW + timedelta(seconds=30)
    lines += [event_line(s, SERIES[s][-1], closer) for s in SERIES]
    return lines


DATASET = build_dataset()
EXPECTED = reference.compute(DATASET)


# -- fixtures ----------------------------------------------------------------

@pytest.fixture(scope="module")
def stack():
    s = Stack(PROJECT)
    s.build_jars()
    s.up(streams_replicas=1)
    writer = EventWriter(EVENTS_FILE)
    writer.write(DATASET, sync=True)
    writer.close()
    yield s
    if os.getenv("E2E_KEEP_STACK") != "1":
        s.down()


# -- helpers -----------------------------------------------------------------

def fetch_points(base_url: str, symbol: str, local: bool = False) -> list[dict]:
    params = {"symbol": symbol, "from": 0, "limit": 10000}
    if local:
        params["local"] = 1
    r = requests.get(f"{base_url}/ema", params=params, timeout=10)
    r.raise_for_status()
    return r.json()["points"]


def wait_points(base_url: str, symbol: str) -> list[dict]:
    want = len(EXPECTED[symbol])
    return Stack.wait_for(
        lambda: (p := fetch_points(base_url, symbol)) and len(p) >= want and p,
        f"{want} windows for {symbol} on {base_url}", timeout=TIMEOUT,
    )


def assert_points_match(symbol: str, actual: list[dict]) -> None:
    expected = EXPECTED[symbol]
    assert len(actual) == len(expected), f"{symbol}: window count"
    for a, e in zip(actual, expected):
        where = f"{symbol} window {datetime.fromtimestamp(e['windowStart'] / 1000)}"
        assert a["windowStart"] == e["windowStart"], where
        assert a["windowEnd"] == e["windowEnd"], where
        assert a["close"] == e["close"], where
        assert math.isclose(a["ema38"], e["ema38"], rel_tol=1e-9, abs_tol=1e-9), where
        assert math.isclose(a["ema100"], e["ema100"], rel_tol=1e-9, abs_tol=1e-9), where
        assert a["signal"] == e["signal"], where


def read_topic(topic: str, min_records: int) -> list:
    """All records currently on `topic`, waiting until at least `min_records`."""
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, group_id=None,
                             enable_auto_commit=False, consumer_timeout_ms=1000)
    try:
        tps = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
        consumer.assign(tps)

        def poll_all():
            consumer.seek_to_beginning(*tps)
            end = consumer.end_offsets(tps)
            records = []
            while any(consumer.position(tp) < end[tp] for tp in tps):
                for batch in consumer.poll(timeout_ms=1000).values():
                    records += batch
            return records if len(records) >= min_records else None

        return Stack.wait_for(poll_all, f">= {min_records} records on {topic}", timeout=TIMEOUT)
    finally:
        consumer.close()


# -- tests -------------------------------------------------------------------

def test_reference_covers_the_edge_cases():
    """Guards the fixture itself: the dataset must exercise what it claims to."""
    signals = {s: [p["signal"] for p in pts if p["signal"]] for s, pts in EXPECTED.items()}
    assert signals["DOWNUP.NL"] == ["BUY", "SELL", "BUY"]
    assert signals["UPDOWN.FR"] == ["BUY", "SELL"]
    assert signals["FLAT.ETR"] == ["BUY"]
    order = EXPECTED["ORDER.FR"]
    assert order[OUT_OF_ORDER_WINDOW]["close"] == SERIES["ORDER.FR"][OUT_OF_ORDER_WINDOW]
    assert order[LATE_WINDOW]["close"] == SERIES["ORDER.FR"][LATE_WINDOW]
    assert all(len(pts) == WINDOWS for pts in EXPECTED.values())


@pytest.mark.parametrize("symbol", list(SERIES))
def test_ema_query_matches_reference(stack, symbol):
    url = stack.streams_urls()[0]
    assert_points_match(symbol, wait_points(url, symbol))


def test_advisories_topic_matches_reference(stack):
    want = sorted((s, p["signal"], p["windowStart"])
                  for s, pts in EXPECTED.items() for p in pts if p["signal"])
    records = read_topic("advisories", len(want))
    got = []
    for r in records:
        v = json.loads(r.value)
        assert r.key.decode() == v["symbol"]
        got.append((v["symbol"], v["signal"], v["windowStart"]))
    assert sorted(got) == want


def test_symbol_registry_has_each_symbol_once(stack):
    records = read_topic("symbols", len(SERIES))
    keys = [r.key.decode() for r in records]
    assert sorted(keys) == sorted(SERIES)


def test_every_event_reaches_processed_topic(stack):
    records = read_topic("trading-events-processed", len(DATASET))
    assert len(records) == len(DATASET)
    for r in records[:50]:
        v = json.loads(r.value)
        assert {"ema38", "ema100"} <= v.keys()


def test_ui_is_healthy(stack):
    assert requests.get(f"{UI_URL}/_stcore/health", timeout=5).ok


def test_scale_out_every_replica_answers_every_symbol(stack):
    """With two replicas each owns some partitions; QueryService must proxy
    the rest, and the moved state must be restored from the changelogs."""
    stack.scale_streams(2)
    urls = stack.streams_urls()
    assert len(urls) == 2
    owners = set()
    for symbol in SERIES:
        for url in urls:
            assert_points_match(symbol, wait_points(url, symbol))
        r = requests.get(f"{urls[0]}/ema", params={"symbol": symbol, "limit": 1}, timeout=10)
        owners.add(r.json()["host"])
    # Not guaranteed for 4 symbols over 6 partitions, but a single owner would
    # mean the proxy path was never exercised.
    if len(owners) < 2:
        pytest.skip("all fixture symbols landed on one replica; proxy path not exercised")

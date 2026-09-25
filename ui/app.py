"""Streamlit front end for the trading-trends pipeline.

Layout follows ai_docs/ui_sketch.png: a search box and a scrollable symbol list
on the left, the EMA chart on the right.

Two very different data paths, deliberately:

  * The symbol list comes from the compacted ``symbols`` Kafka topic. It is
    small (bounded by symbol cardinality, not event volume), changes rarely, and
    every replica can hold the whole thing -- so the UI reads it directly and
    caches it.

  * The EMA time series comes from the streams app's interactive-query endpoint.
    That state is already materialised and partitioned by symbol inside Kafka
    Streams; re-consuming it here would duplicate it in every UI replica and cap
    history at topic retention.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import plotly.graph_objects as go
import requests
import streamlit as st
from kafka import KafkaConsumer, TopicPartition

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SYMBOLS_TOPIC = os.getenv("SYMBOLS_TOPIC", "symbols")
STREAMS_QUERY_URL = os.getenv("STREAMS_QUERY_URL", "http://localhost:7070").rstrip("/")
UI_REFRESH_SECONDS = int(os.getenv("UI_REFRESH_SECONDS", "5"))
SYMBOL_CACHE_TTL = int(os.getenv("SYMBOL_CACHE_TTL", "60"))
REGISTRY_READ_BUDGET_S = float(os.getenv("REGISTRY_READ_BUDGET_SECONDS", "15"))
MAX_POINTS = int(os.getenv("UI_MAX_POINTS", "1000"))
MAX_LISTED_SYMBOLS = int(os.getenv("UI_MAX_LISTED_SYMBOLS", "200"))

st.set_page_config(page_title="Trading trends", layout="wide", page_icon="📈")

# Trim Streamlit's default chrome so the three panels sit like the sketch.
st.markdown(
    """
    <style>
      [data-testid="stAppViewBlockContainer"] { padding: 1.2rem 1.5rem 1rem; }
      [data-testid="stHeader"] { height: 0; }
      div[data-testid="stVerticalBlockBorderWrapper"] button { text-align: left; }
      button[kind="secondary"], button[kind="primary"] { font-family: monospace; }
    </style>
    """,
    unsafe_allow_html=True,
)


# --------------------------------------------------------------------------
# Symbol registry: replay the compacted topic, then cache it
# --------------------------------------------------------------------------
@st.cache_data(ttl=SYMBOL_CACHE_TTL, show_spinner=False)
def load_symbols() -> list[str]:
    """Replay the compacted registry topic into a sorted symbol list.

    Uses ``assign`` rather than ``subscribe``: the UI is not a consumer-group
    member, so N UI replicas never rebalance against each other and no offsets
    are committed. The read is a bounded, idempotent snapshot -- safe to repeat
    on every cache expiry.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=2000,
        key_deserializer=lambda b: b.decode("utf-8", "replace") if b else None,
    )
    try:
        partitions = consumer.partitions_for_topic(SYMBOLS_TOPIC)
        if not partitions:
            return []

        tps = [TopicPartition(SYMBOLS_TOPIC, p) for p in sorted(partitions)]
        consumer.assign(tps)
        end_offsets = consumer.end_offsets(tps)
        consumer.seek_to_beginning(*tps)

        symbols: set[str] = set()
        pending = {tp for tp in tps if end_offsets.get(tp, 0) > 0}
        deadline = time.monotonic() + REGISTRY_READ_BUDGET_S

        while pending and time.monotonic() < deadline:
            batch = consumer.poll(timeout_ms=500, max_records=5000)
            if not batch:
                # No data and nothing left to wait for on a quiet topic.
                if all(consumer.position(tp) >= end_offsets.get(tp, 0) for tp in pending):
                    break
                continue
            for tp, records in batch.items():
                for record in records:
                    # The record key *is* the symbol; the value carries metadata
                    # we don't need for the list.
                    if record.key:
                        symbols.add(record.key)
                if consumer.position(tp) >= end_offsets.get(tp, 0):
                    pending.discard(tp)

        return sorted(symbols)
    finally:
        consumer.close()


def fetch_points(symbol: str) -> list[dict]:
    """Ask the streams app for this symbol's EMA history.

    ``from=0`` with no upper bound rather than a wall-clock window: the dataset
    is a replay of November 2021, so event time has nothing to do with now.
    ``limit`` keeps the newest N windows.
    """
    response = requests.get(
        f"{STREAMS_QUERY_URL}/ema",
        params={"symbol": symbol, "from": 0, "limit": MAX_POINTS},
        timeout=15,
    )
    response.raise_for_status()
    return response.json().get("points", [])


def build_figure(symbol: str, points: list[dict]) -> go.Figure:
    times = [datetime.fromtimestamp(p["windowStart"] / 1000, tz=timezone.utc) for p in points]

    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=times,
            y=[p["ema100"] for p in points],
            name="EMA 100",
            mode="lines",
            line=dict(color="#ff7f6b", width=2),
        )
    )
    figure.add_trace(
        go.Scatter(
            x=times,
            y=[p["ema38"] for p in points],
            name="EMA 38",
            mode="lines",
            line=dict(color="#4da3ff", width=2),
        )
    )

    for signal, colour, marker, label in (
        ("BUY", "#3ddc84", "triangle-up", "Buy advisory"),
        ("SELL", "#ff5252", "triangle-down", "Sell advisory"),
    ):
        matches = [(t, p) for t, p in zip(times, points) if p.get("signal") == signal]
        if not matches:
            continue
        figure.add_trace(
            go.Scatter(
                x=[t for t, _ in matches],
                y=[p["ema38"] for _, p in matches],
                name=label,
                mode="markers",
                marker=dict(color=colour, size=13, symbol=marker,
                            line=dict(color="#000000", width=1)),
            )
        )

    figure.update_layout(
        template="plotly_dark",
        title=f"{symbol} — 5-minute tumbling-window EMAs",
        margin=dict(l=10, r=10, t=50, b=10),
        height=560,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.0, x=0),
        xaxis_title=None,
        yaxis_title="EMA",
    )
    return figure


@st.fragment(run_every=UI_REFRESH_SECONDS)
def render_chart() -> None:
    """Re-runs on its own timer, so the chart refreshes without re-reading Kafka
    for the symbol list on every tick."""
    symbol = st.session_state.get("selected_symbol")
    if not symbol:
        st.info("Select a symbol from the list to plot its EMAs.")
        return

    try:
        points = fetch_points(symbol)
    except requests.RequestException as exc:
        st.warning(f"Could not reach the query service at {STREAMS_QUERY_URL}: {exc}")
        return

    if not points:
        st.info(
            f"No closed windows for **{symbol}** yet. "
            "A 5-minute window is only evaluated once the next one starts."
        )
        return

    st.plotly_chart(build_figure(symbol, points), use_container_width=True)

    advisories = [p for p in points if p.get("signal")]
    latest = points[-1]
    left, middle, right = st.columns(3)
    left.metric("EMA 38", f"{latest['ema38']:.4f}")
    middle.metric("EMA 100", f"{latest['ema100']:.4f}")
    right.metric("Advisories in view", len(advisories))


# --------------------------------------------------------------------------
# Page
# --------------------------------------------------------------------------
st.session_state.setdefault("selected_symbol", None)

symbol_column, chart_column = st.columns([1, 3], gap="medium")

with symbol_column:
    query = st.text_input(
        "Search symbols",
        key="symbol_search",
        placeholder="search",
        label_visibility="collapsed",
    )

    try:
        all_symbols = load_symbols()
        registry_error = None
    except Exception as exc:  # noqa: BLE001 - surfaced in the UI instead
        all_symbols, registry_error = [], exc

    if registry_error is not None:
        st.error(f"Symbol registry unavailable: {registry_error}")
    elif not all_symbols:
        st.warning(f"No symbols on '{SYMBOLS_TOPIC}' yet — is the pipeline running?")

    needle = query.strip().lower()
    matches = [s for s in all_symbols if needle in s.lower()] if needle else all_symbols
    shown = matches[:MAX_LISTED_SYMBOLS]

    with st.container(height=520, border=True):
        for symbol in shown:
            if st.button(
                symbol,
                key=f"sym-{symbol}",
                use_container_width=True,
                type="primary" if symbol == st.session_state["selected_symbol"] else "secondary",
            ):
                st.session_state["selected_symbol"] = symbol
                st.rerun()

    if len(matches) > len(shown):
        st.caption(f"{len(matches) - len(shown)} more — refine your search.")
    else:
        st.caption(f"{len(matches)} of {len(all_symbols)} symbols")

with chart_column:
    render_chart()

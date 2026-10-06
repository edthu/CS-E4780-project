# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.1"]
# ///
"""Profile event rates in the DEBS 2022 trading data over time.

Aggregates every CSV in data/ by system timestamp (Date + Time columns, i.e.
when Infront received the update) and writes:

  output/profile/minute_totals.parquet   events / price events / volume per minute
  output/profile/second_totals.parquet   the same per second (for burst peaks)
  output/profile/symbol_minute.parquet   the same per (symbol, minute)
  output/profile/summary.json            peaks and top symbols for visualisation
  output/profile/dataset_profile.html    interactive page built from summary.json

A "price event" is a row with Last > 0, which is what the producer forwards.
Volume is the sum of "Last volume" over price events.

Run:  uv run scripts/profile_dataset.py [data_dir] [out_dir]
      SUMMARY_ONLY=1 uv run scripts/profile_dataset.py   # rebuild summary.json from the parquet files
"""

import json
import os
import sys
import time
from pathlib import Path

import duckdb

from render_profile import render

DATA_DIR = Path(sys.argv[1] if len(sys.argv) > 1 else "data")
OUT_DIR = Path(sys.argv[2] if len(sys.argv) > 2 else "output/profile")
TOP_SYMBOLS = 25

# The files have 11 CRLF comment lines, a header and a quoted description line
# before the data, and odd rows that break DuckDB's sniffer and parallel reader,
# so read them positionally, single-threaded, everything as text.
COLUMNS = {f"c{i:02d}": "VARCHAR" for i in range(39)}


def csv_source(path: Path) -> str:
    return f"""read_csv('{path}', skip=13, header=false, auto_detect=false,
        delim=',', quote='', escape='', comment='#', parallel=false,
        strict_mode=false, null_padding=true, ignore_errors=true,
        columns={COLUMNS!r})"""


def main() -> None:
    files = sorted(DATA_DIR.glob("debs2022-gc-trading-day-*.csv"))
    if not files:
        sys.exit(f"no CSV files in {DATA_DIR}")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    con.execute("SET preserve_insertion_order=false")
    if os.environ.get("SUMMARY_ONLY"):
        return write_summary(con)
    con.execute("""CREATE TABLE sym_sec (symbol VARCHAR, sectype VARCHAR,
        sec TIMESTAMP, events BIGINT, price_events BIGINT, volume DOUBLE)""")

    for path in files:
        started = time.time()
        # Collapse each file to (symbol, second) first; everything else derives from it.
        con.execute(f"""
            INSERT INTO sym_sec
            SELECT symbol, any_value(sectype), sec, count(*),
                   count(*) FILTER (WHERE price > 0),
                   coalesce(sum(vol) FILTER (WHERE price > 0 AND vol > 0), 0)
            FROM (
                SELECT c00 AS symbol, c01 AS sectype,
                       date_trunc('second', try_strptime(c02 || ' ' || c03, '%d-%m-%Y %H:%M:%S.%g')) AS sec,
                       try_cast(c21 AS DOUBLE) AS price,
                       try_cast(c22 AS DOUBLE) AS vol
                FROM {csv_source(path)}
            )
            WHERE sec IS NOT NULL AND symbol IS NOT NULL
            GROUP BY symbol, sec
        """)
        print(f"{path.name}: {time.time() - started:.0f}s", flush=True)

    con.execute(f"""COPY (
        SELECT sec, sum(events) events, sum(price_events) price_events, sum(volume) volume
        FROM sym_sec GROUP BY sec ORDER BY sec
    ) TO '{OUT_DIR}/second_totals.parquet'""")
    con.execute(f"""COPY (
        SELECT symbol, any_value(sectype) sectype, date_trunc('minute', sec) ts,
               sum(events) events, sum(price_events) price_events, sum(volume) volume
        FROM sym_sec GROUP BY symbol, ts ORDER BY symbol, ts
    ) TO '{OUT_DIR}/symbol_minute.parquet'""")
    con.execute(f"""COPY (
        SELECT ts, sum(events) events, sum(price_events) price_events, sum(volume) volume,
               count(DISTINCT symbol) active_symbols
        FROM '{OUT_DIR}/symbol_minute.parquet' GROUP BY ts ORDER BY ts
    ) TO '{OUT_DIR}/minute_totals.parquet'""")

    write_summary(con)


def write_summary(con: duckdb.DuckDBPyConnection) -> None:
    summary = build_summary(con)
    (OUT_DIR / "summary.json").write_text(json.dumps(summary, default=str))
    print(f"wrote {OUT_DIR}/summary.json")
    render(OUT_DIR)


def rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def build_summary(con: duckdb.DuckDBPyConnection) -> dict:
    minute = f"'{OUT_DIR}/minute_totals.parquet'"
    second = f"'{OUT_DIR}/second_totals.parquet'"
    sym_min = f"'{OUT_DIR}/symbol_minute.parquet'"

    con.execute(f"""CREATE TABLE sym_totals AS
        SELECT symbol, any_value(sectype) sectype, sum(events) events,
               sum(price_events) price_events, sum(volume) volume,
               arg_max(ts, events) peak_minute, max(events) peak_minute_events,
               arg_max(ts, price_events) peak_price_minute, max(price_events) peak_minute_price_events
        FROM {sym_min} GROUP BY symbol""")

    top_by_events = rows(con, f"SELECT * FROM sym_totals ORDER BY events DESC LIMIT {TOP_SYMBOLS}")
    top_by_price = rows(con, f"SELECT * FROM sym_totals ORDER BY price_events DESC LIMIT {TOP_SYMBOLS}")
    tracked = sorted({r["symbol"] for r in top_by_events + top_by_price})
    tracked_sql = ", ".join(f"'{s}'" for s in tracked)

    return {
        "totals": rows(con, f"""SELECT sum(events) events, sum(price_events) price_events,
            sum(volume) volume, min(ts) first_minute, max(ts) last_minute,
            (SELECT count(*) FROM sym_totals) symbols FROM {minute}""")[0],
        "by_day": rows(con, f"""SELECT CAST(ts AS DATE) AS "day", sum(events) events,
            sum(price_events) price_events, sum(volume) volume
            FROM {minute} GROUP BY 1 ORDER BY 1"""),
        "by_exchange": rows(con, f"""SELECT split_part(symbol, '.', -1) exchange, sectype,
            sum(events) events, sum(price_events) price_events, count(*) symbols
            FROM sym_totals GROUP BY ALL ORDER BY events DESC"""),
        # Full per-minute series (~10k points) for the week timeline.
        "minute_series": rows(con, f"""SELECT strftime(ts, '%Y-%m-%dT%H:%M') t,
            events e, price_events p, volume v, active_symbols a FROM {minute} ORDER BY ts"""),
        # Average shape of a trading day: events per minute-of-day, weekdays only.
        "minute_of_day": rows(con, f"""SELECT strftime(ts, '%H:%M') t,
            avg(events) e, avg(price_events) p, max(events) emax, max(price_events) pmax
            FROM {minute} WHERE dayofweek(ts) BETWEEN 1 AND 5
            GROUP BY t ORDER BY t"""),
        "peak_minutes": rows(con, f"""SELECT strftime(ts, '%Y-%m-%dT%H:%M') t, events, price_events,
            volume, active_symbols FROM {minute} ORDER BY events DESC LIMIT 20"""),
        "peak_price_minutes": rows(con, f"""SELECT strftime(ts, '%Y-%m-%dT%H:%M') t, events,
            price_events, volume, active_symbols FROM {minute} ORDER BY price_events DESC LIMIT 20"""),
        "peak_seconds": rows(con, f"""SELECT strftime(sec, '%Y-%m-%dT%H:%M:%S') t, events, price_events
            FROM {second} ORDER BY events DESC LIMIT 20"""),
        "peak_price_seconds": rows(con, f"""SELECT strftime(sec, '%Y-%m-%dT%H:%M:%S') t, events, price_events
            FROM {second} ORDER BY price_events DESC LIMIT 20"""),
        "second_percentiles": rows(con, f"""SELECT
            quantile_cont(events, [0.5, 0.9, 0.99, 0.999, 1.0]) events,
            quantile_cont(price_events, [0.5, 0.9, 0.99, 0.999, 1.0]) price_events
            FROM {second} WHERE dayofweek(sec) BETWEEN 1 AND 5
              AND hour(sec) BETWEEN 9 AND 17  -- 09:00-17:59, core trading hours"""),
        "top_by_events": top_by_events,
        "top_by_price": top_by_price,
        # Long-tail: cumulative share of events covered by the top-N symbols.
        "concentration": rows(con, """SELECT rank, events_share, price_share FROM (
            SELECT row_number() OVER (ORDER BY events DESC) rank,
                   sum(events) OVER (ORDER BY events DESC ROWS UNBOUNDED PRECEDING) / sum(events) OVER () events_share,
                   NULL price_share FROM sym_totals)
            WHERE rank IN (1,5,10,25,50,100,250,500,1000,2500,5000)
            UNION ALL SELECT rank, NULL, price_share FROM (
            SELECT row_number() OVER (ORDER BY price_events DESC) rank,
                   sum(price_events) OVER (ORDER BY price_events DESC ROWS UNBOUNDED PRECEDING) / sum(price_events) OVER () price_share
            FROM sym_totals)
            WHERE rank IN (1,5,10,25,50,100,250,500,1000,2500,5000)"""),
        # 15-minute series for the top symbols (heatmap + per-symbol lines).
        "symbol_series": rows(con, f"""SELECT symbol s,
            strftime(time_bucket(INTERVAL 15 MINUTE, ts), '%Y-%m-%dT%H:%M') t,
            sum(events) e, sum(price_events) p
            FROM {sym_min} WHERE symbol IN ({tracked_sql}) GROUP BY ALL ORDER BY s, t"""),
    }


if __name__ == "__main__":
    main()

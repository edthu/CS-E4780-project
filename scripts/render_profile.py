"""Render <out_dir>/summary.json into <out_dir>/dataset_profile.html.

profile_dataset.py calls this automatically; run it by hand only to re-render
after editing profile_page.html:
    python3 scripts/render_profile.py [out_dir]
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

TEMPLATE = Path(__file__).with_name("profile_page.html")
BUCKET = timedelta(minutes=15)


def parse(t: str) -> datetime:
    return datetime.fromisoformat(t.replace(" ", "T"))


def dense(series: list[dict], start: datetime, step: timedelta, n: int, key: str) -> list[int]:
    out = [0] * n
    for r in series:
        i = int((parse(r["t"]) - start) / step)
        if 0 <= i < n:
            out[i] += int(r[key] or 0)
    return out


def render(out_dir: Path) -> Path:
    s = json.loads((out_dir / "summary.json").read_text())

    # The data covers Mon 8 Nov - Sun 14 Nov 2021; anchor everything at Monday 00:00.
    start = parse(s["minute_series"][0]["t"]).replace(hour=0, minute=0)
    minutes = 7 * 24 * 60
    buckets = minutes // 15
    step = timedelta(minutes=1)

    tracked = {r["symbol"]: r for r in s["top_by_events"] + s["top_by_price"]}
    by_symbol: dict[str, list[dict]] = {}
    for r in s["symbol_series"]:
        by_symbol.setdefault(r["s"], []).append(r)

    data = {
        "start": start.strftime("%Y-%m-%dT%H:%M"),
        "totals": s["totals"],
        "byDay": s["by_day"],
        "byExchange": s["by_exchange"],
        "minute": {
            "e": dense(s["minute_series"], start, step, minutes, "e"),
            "p": dense(s["minute_series"], start, step, minutes, "p"),
            "a": dense(s["minute_series"], start, step, minutes, "a"),
        },
        "dayProfile": {
            "t": [r["t"] for r in s["minute_of_day"]],
            "e": [round(r["e"], 1) for r in s["minute_of_day"]],
            "p": [round(r["p"], 1) for r in s["minute_of_day"]],
        },
        "peakMinutes": s["peak_minutes"],
        "peakPriceMinutes": s["peak_price_minutes"],
        "peakSeconds": s["peak_seconds"],
        "peakPriceSeconds": s["peak_price_seconds"],
        "secondPercentiles": s["second_percentiles"][0],
        "concentration": s["concentration"],
        "symbols": [
            {
                "s": sym,
                "type": r["sectype"],
                "e": int(r["events"]),
                "p": int(r["price_events"]),
                "peakE": [str(r["peak_minute"])[:16], int(r["peak_minute_events"])],
                "peakP": [str(r["peak_price_minute"])[:16], int(r["peak_minute_price_events"])],
                "se": dense(by_symbol.get(sym, []), start, BUCKET, buckets, "e"),
                "sp": dense(by_symbol.get(sym, []), start, BUCKET, buckets, "p"),
            }
            for sym, r in tracked.items()
        ],
    }

    html = TEMPLATE.read_text().replace("/*__DATA__*/null", json.dumps(data, separators=(",", ":"), default=str))
    target = out_dir / "dataset_profile.html"
    target.write_text(html)
    print(f"wrote {target} ({len(html) / 1e6:.1f} MB)")
    return target


if __name__ == "__main__":
    render(Path(sys.argv[1] if len(sys.argv) > 1 else "output/profile"))

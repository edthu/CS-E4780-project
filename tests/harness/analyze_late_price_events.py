"""Report late price events and estimated EMA impact for a daily CSV."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness.csv_lateness_analysis import analyze_full_day_csv, render_full_day_report


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "csv",
        nargs="?",
        type=Path,
        default=root / "data" / "debs2022-gc-trading-day-08-11-21.csv",
        help="full-day trading CSV (default: November 8 sample)",
    )
    args = parser.parse_args()
    result = analyze_full_day_csv(args.csv)
    print(render_full_day_report(result))


if __name__ == "__main__":
    main()

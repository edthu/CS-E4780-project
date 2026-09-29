"""Append events to the NDJSON file the producer follows.

The file is the ingestion -> producer handoff, so the moment a line is durably
appended is where the latency clock starts.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path

TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def format_ts(dt: datetime) -> str:
    """Same shape as the ingestion output: millisecond precision, no zone."""
    return dt.strftime(TS_FORMAT)[:-3]


def event_line(symbol: str, price: float, ts: datetime | str, sec_type: str = "E") -> str:
    timestamp = ts if isinstance(ts, str) else format_ts(ts)
    return json.dumps({"symbol": symbol, "securityType": sec_type,
                       "price": price, "timestamp": timestamp}, separators=(",", ":"))


class EventWriter:
    def __init__(self, path: Path):
        self._f = open(path, "a", encoding="utf-8")

    def write(self, lines: list[str], sync: bool = False) -> float:
        """Append lines and return the wall time once they are on the file.

        `sync` fsyncs too; the producer reads through the page cache, so it is
        only needed where a line's timestamp is used as a latency start.
        """
        if lines:
            self._f.write("\n".join(lines) + "\n")
        self._f.flush()
        if sync:
            os.fsync(self._f.fileno())
        return time.time()

    def close(self) -> None:
        self._f.close()

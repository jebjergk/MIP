"""Timezone helpers for Brooks intraday (stored bar_ts is naive UTC)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def parse_bar_ts(ts: Any) -> datetime | None:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts
    s = str(ts).strip().replace("Z", "+00:00")
    if "T" not in s and " " in s:
        s = s.replace(" ", "T", 1)
    try:
        if len(s) >= 19:
            return datetime.fromisoformat(s[:19])
    except ValueError:
        return None
    return None


def bar_ts_to_ny_iso(*, bar_ts_ny: Any = None, bar_ts_utc: Any = None) -> str | None:
    """Display/storage NY wall time. Prefer explicit bar_ts_ny; else convert naive UTC bar_ts."""
    if bar_ts_ny:
        dt = parse_bar_ts(bar_ts_ny)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=NY)
            return dt.astimezone(NY).strftime("%Y-%m-%dT%H:%M:%S")
        s = str(bar_ts_ny).replace(" ", "T")
        return s[:19] if len(s) >= 19 else s
    dt = parse_bar_ts(bar_ts_utc)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(NY).strftime("%Y-%m-%dT%H:%M:%S")


def ny_hm(*, bar_ts_ny: Any = None, bar_ts_utc: Any = None) -> str:
    iso = bar_ts_to_ny_iso(bar_ts_ny=bar_ts_ny, bar_ts_utc=bar_ts_utc)
    if not iso or len(iso) < 16:
        return "—"
    return iso[11:16]

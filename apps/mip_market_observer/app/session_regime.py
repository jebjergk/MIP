"""US equity RTH opening window flag (price discovery caution)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.threshold_profile import OPENING_WINDOW_MINUTES

_NY = ZoneInfo("America/New_York")


def opening_price_discovery_window(now_utc: datetime, symbol: str) -> bool:
    sym = str(symbol or "").strip().upper()
    if "/" in sym or len(sym) == 6 and sym.isalpha() and not sym.isdigit():
        # crude FX pair detection
        if "/" in sym:
            return False
    # 6-letter could be stock — still apply US window only if "US equity" heuristic: no slash
    if "/" in symbol:
        return False
    dt = now_utc.astimezone(_NY)
    if dt.weekday() >= 5:
        return False
    h, m = dt.hour, dt.minute
    open_m = 9 * 60 + 30
    cur_m = h * 60 + m
    end_m = open_m + OPENING_WINDOW_MINUTES
    return open_m <= cur_m < end_m

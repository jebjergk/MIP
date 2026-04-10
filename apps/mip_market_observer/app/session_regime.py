"""US equity RTH opening window flag (price discovery caution)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.threshold_profile import OPENING_WINDOW_MINUTES

_NY = ZoneInfo("America/New_York")


def compute_session_regime(now_utc: datetime, symbol: str) -> str:
    """US session bucket for regime-aware interpretation (equities); FX → fx_session."""
    sym = str(symbol or "").strip().upper()
    if "/" in sym:
        return "fx_session"
    dt = now_utc.astimezone(_NY)
    if dt.weekday() >= 5:
        return "weekend"
    cur = dt.hour * 60 + dt.minute
    if cur < 4 * 60:
        return "closed"
    if cur < 9 * 60 + 30:
        return "pre_market"
    if cur < 9 * 60 + 30 + OPENING_WINDOW_MINUTES:
        return "open_window"
    if cur < 15 * 60 + 30:
        return "regular_session"
    if cur < 16 * 60:
        return "closing_window"
    if cur < 20 * 60:
        return "after_hours"
    return "closed"


def opening_price_discovery_window(now_utc: datetime, symbol: str) -> bool:
    sym = str(symbol or "").strip().upper()
    if "/" in sym:
        return False
    dt = now_utc.astimezone(_NY)
    if dt.weekday() >= 5:
        return False
    h, m = dt.hour, dt.minute
    open_m = 9 * 60 + 30
    cur_m = h * 60 + m
    end_m = open_m + OPENING_WINDOW_MINUTES
    return open_m <= cur_m < end_m

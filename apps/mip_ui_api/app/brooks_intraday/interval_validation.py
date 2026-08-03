"""Expected RTH 5-minute interval grid (bar open timestamps, America/New_York wall time)."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

from .calendar import TradingSession

TIMESTAMP_CONVENTION = "INTERVAL_START_NY_WALL_V0_1"
BAR_VALIDATION_VERSION = "BROOKS_BAR_INTERVAL_VALIDATION_V0_1"


def expected_interval_starts_ny(trading_date: date, *, bar_minutes: int = 5) -> list[datetime]:
    """Full RTH session: first bar opens 09:30, last opens 15:55 (78 bars for 5m)."""
    open_dt = datetime.combine(trading_date, time(9, 30))
    close_dt = datetime.combine(trading_date, time(16, 0))
    out: list[datetime] = []
    cur = open_dt
    while cur < close_dt:
        out.append(cur)
        cur += timedelta(minutes=bar_minutes)
    return out


def expected_interval_starts_for_session(session: TradingSession, *, bar_minutes: int = 5) -> list[datetime]:
    if session.is_early_close:
        open_dt = datetime.combine(session.trading_date, time(9, 30))
        close_dt = session.session_close_ny
        out: list[datetime] = []
        cur = open_dt
        while cur < close_dt:
            out.append(cur)
            cur += timedelta(minutes=bar_minutes)
        return out
    return expected_interval_starts_ny(session.trading_date, bar_minutes=bar_minutes)


def interval_sets_match(
    actual_ny: list[datetime],
    expected_ny: list[datetime],
) -> tuple[bool, list[str], list[str]]:
    expected_set = {dt.isoformat(timespec="minutes") for dt in expected_ny}
    actual_set = {dt.isoformat(timespec="minutes") for dt in actual_ny}
    missing = sorted(expected_set - actual_set)
    extra = sorted(actual_set - expected_set)
    ok = not missing and not extra and len(actual_ny) == len(expected_ny)
    return ok, missing, extra

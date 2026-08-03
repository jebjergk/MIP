from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable
from zoneinfo import ZoneInfo

from app.brooks_intraday.db_util import query_rows

NY_TZ = ZoneInfo("America/New_York")
BERLIN_TZ = ZoneInfo("Europe/Berlin")

RTH_OPEN = time(9, 30)
RTH_CLOSE_FULL = time(16, 0)
RTH_CLOSE_EARLY = time(13, 0)


@dataclass(frozen=True)
class TradingSession:
    trading_date: date
    session_open_utc: datetime
    session_close_utc: datetime
    session_open_ny: datetime
    session_close_ny: datetime
    is_early_close: bool
    expected_5m_bars: int


@dataclass
class WeekResolution:
    week_start: date
    status: str
    trading_dates: list[date]
    sessions: list[TradingSession]
    messages: list[str]


def _is_weekday(d: date) -> bool:
    return d.weekday() < 5


def _load_holidays(start: date, end: date) -> dict[date, dict]:
    rows = query_rows(
        """
        SELECT HOLIDAY_DATE, FULL_DAY_CLOSE, HOLIDAY_NAME
        FROM MIP.APP.US_EQUITY_HOLIDAYS
        WHERE EXCHANGE = 'NYSE'
          AND HOLIDAY_DATE BETWEEN %s AND %s
        """,
        (start, end),
    )
    out: dict[date, dict] = {}
    for row in rows:
        raw = row.get("holiday_date") or row.get("HOLIDAY_DATE")
        if isinstance(raw, datetime):
            d = raw.date()
        else:
            d = raw if isinstance(raw, date) else date.fromisoformat(str(raw)[:10])
        full_close = row.get("full_day_close")
        if full_close is None:
            full_close = row.get("FULL_DAY_CLOSE")
        out[d] = {
            "full_day_close": bool(full_close),
            "name": row.get("holiday_name") or row.get("HOLIDAY_NAME"),
        }
    return out


def _session_for_date(trading_date: date, *, early_close: bool) -> TradingSession:
    close_time = RTH_CLOSE_EARLY if early_close else RTH_CLOSE_FULL
    open_ny = datetime.combine(trading_date, RTH_OPEN, tzinfo=NY_TZ)
    close_ny = datetime.combine(trading_date, close_time, tzinfo=NY_TZ)
    minutes = int((close_ny - open_ny).total_seconds() // 60)
    expected = minutes // 5
    return TradingSession(
        trading_date=trading_date,
        session_open_utc=open_ny.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        session_close_utc=close_ny.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        session_open_ny=open_ny.replace(tzinfo=None),
        session_close_ny=close_ny.replace(tzinfo=None),
        is_early_close=early_close,
        expected_5m_bars=expected,
    )


def resolve_week_sessions(
    week_start: date,
    *,
    holidays: dict[date, dict] | None = None,
) -> WeekResolution:
    messages: list[str] = []
    if week_start.weekday() != 0:
        messages.append("Week start should be a Monday for the pilot experiment.")

    end = week_start + timedelta(days=4)
    holiday_map = holidays if holidays is not None else _load_holidays(week_start, end)

    sessions: list[TradingSession] = []
    for offset in range(5):
        d = week_start + timedelta(days=offset)
        if not _is_weekday(d):
            continue
        hol = holiday_map.get(d)
        if hol and hol.get("full_day_close"):
            messages.append(f"{d.isoformat()} is a full NYSE holiday ({hol.get('name')}).")
            continue
        if hol and not hol.get("full_day_close"):
            return WeekResolution(
                week_start=week_start,
                status="EARLY_CLOSE_REQUIRES_EXPLICIT_SUPPORT",
                trading_dates=[],
                sessions=[],
                messages=messages
                + [f"{d.isoformat()} has a partial session — not supported in v0.1."],
            )
        sessions.append(_session_for_date(d, early_close=False))

    if len(sessions) != 5:
        return WeekResolution(
            week_start=week_start,
            status="WEEK_HAS_FEWER_THAN_FIVE_SESSIONS",
            trading_dates=[s.trading_date for s in sessions],
            sessions=sessions,
            messages=messages
            + [f"Expected 5 sessions, resolved {len(sessions)}."],
        )

    return WeekResolution(
        week_start=week_start,
        status="READY",
        trading_dates=[s.trading_date for s in sessions],
        sessions=sessions,
        messages=messages,
    )


def rth_open_utc_naive(trading_date: date, *, early_close: bool = False) -> datetime:
    return _session_for_date(trading_date, early_close=early_close).session_open_utc


def berlin_offset_hours_at(dt_ny: datetime) -> float:
    aware = dt_ny.replace(tzinfo=NY_TZ) if dt_ny.tzinfo is None else dt_ny.astimezone(NY_TZ)
    berlin = aware.astimezone(BERLIN_TZ)
    return (berlin.utcoffset() or timedelta()).total_seconds() / 3600.0


def sessions_to_dict(sessions: Iterable[TradingSession]) -> list[dict]:
    out = []
    for s in sessions:
        out.append(
            {
                "trading_date": s.trading_date.isoformat(),
                "session_open_utc": s.session_open_utc.isoformat(),
                "session_close_utc": s.session_close_utc.isoformat(),
                "session_open_ny": s.session_open_ny.isoformat(),
                "session_close_ny": s.session_close_ny.isoformat(),
                "is_early_close": s.is_early_close,
                "expected_5m_bars": s.expected_5m_bars,
                "berlin_offset_hours_at_open": berlin_offset_hours_at(s.session_open_ny),
            }
        )
    return out

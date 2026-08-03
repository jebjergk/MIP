from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from .calendar import resolve_week_sessions
from .interval_validation import expected_interval_starts_ny


@dataclass(frozen=True)
class ReplayStep:
    session_index: int
    bar_index_in_session: int
    global_step_index: int
    trading_date: date
    bar_timestamp_ny: datetime
    bar_timestamp_utc: datetime | None


def _ny_to_utc_naive(ny: datetime) -> datetime:
    from zoneinfo import ZoneInfo

    ny_tz = ZoneInfo("America/New_York")
    utc = ny.replace(tzinfo=ny_tz).astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    return utc


def build_replay_schedule(week_start: date) -> list[ReplayStep]:
    week = resolve_week_sessions(week_start)
    if week.status != "READY":
        raise ValueError(week.status)
    steps: list[ReplayStep] = []
    global_idx = 0
    for session_index, td in enumerate(week.trading_dates):
        starts = expected_interval_starts_ny(td)
        for bar_index, ny in enumerate(starts):
            steps.append(
                ReplayStep(
                    session_index=session_index,
                    bar_index_in_session=bar_index,
                    global_step_index=global_idx,
                    trading_date=td,
                    bar_timestamp_ny=ny,
                    bar_timestamp_utc=_ny_to_utc_naive(ny),
                )
            )
            global_idx += 1
    return steps


def total_steps_for_week(week_start: date) -> int:
    return len(build_replay_schedule(week_start))

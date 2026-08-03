"""Neutral validation-week selection (before pipeline outcomes)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from .calendar import resolve_week_sessions
from .experiment_freeze import BASELINE_WEEK_START, PILOT_SYMBOLS

# Locked before Phase 9 pipeline execution — do not re-rank by trade outcomes.
LOCKED_VALIDATION_WEEKS: tuple[date, ...] = (
    date(2026, 7, 13),
    date(2026, 6, 22),
    date(2026, 6, 1),
)


@dataclass
class WeekCandidate:
    week_start: date
    week_end: date
    calendar_status: str
    trading_dates: list[date]
    selection_tags: list[str]
    messages: list[str]


def _baseline_monday() -> date:
    return date.fromisoformat(BASELINE_WEEK_START[:10])


def enumerate_monday_weeks_before_baseline(*, lookback_weeks: int = 12) -> list[date]:
    base = _baseline_monday()
    out: list[date] = []
    cursor = base - timedelta(days=7)
    for _ in range(lookback_weeks):
        if cursor.weekday() == 0:
            out.append(cursor)
        cursor -= timedelta(days=7)
    return out


def evaluate_week_candidate(week_start: date) -> WeekCandidate:
    res = resolve_week_sessions(week_start)
    tags: list[str] = []
    if week_start == _baseline_monday() - timedelta(days=7):
        tags.append("temporal_proximity_immediate_prior")
    elif ( _baseline_monday() - week_start).days >= 28:
        tags.append("temporal_proximity_early_sample")
    else:
        tags.append("temporal_proximity_mid_sample")
    return WeekCandidate(
        week_start=week_start,
        week_end=week_start + timedelta(days=4),
        calendar_status=res.status,
        trading_dates=list(res.trading_dates),
        selection_tags=tags,
        messages=list(res.messages),
    )


def build_week_selection_rationale() -> dict[str, Any]:
    """
    Selection criteria applied without reference to simulated P/L or entry counts.
    """
    baseline = _baseline_monday()
    candidates_scored: list[dict[str, Any]] = []
    for monday in enumerate_monday_weeks_before_baseline(lookback_weeks=14):
        wc = evaluate_week_candidate(monday)
        eligible = wc.calendar_status == "READY" and len(wc.trading_dates) == 5
        candidates_scored.append(
            {
                "week_start": monday.isoformat(),
                "week_end": wc.week_end.isoformat(),
                "eligible": eligible,
                "calendar_status": wc.calendar_status,
                "trading_dates": [d.isoformat() for d in wc.trading_dates],
                "tags": wc.selection_tags,
                "messages": wc.messages,
            }
        )

    selected: list[dict[str, Any]] = []
    for ws in LOCKED_VALIDATION_WEEKS:
        wc = evaluate_week_candidate(ws)
        if wc.calendar_status != "READY":
            raise ValueError(f"Locked week {ws} is not calendar-ready: {wc.calendar_status}")
        selected.append(
            {
                "week_start": ws.isoformat(),
                "week_end": (ws + timedelta(days=4)).isoformat(),
                "trading_dates": [d.isoformat() for d in wc.trading_dates],
                "selection_tags": wc.selection_tags,
                "behavior_hypothesis_neutral": _neutral_behavior_label(ws),
            }
        )

    return {
        "phase": "9",
        "baseline_week_start": baseline.isoformat(),
        "baseline_run_id_note": "Engineering baseline run is not modified by validation.",
        "symbols": list(PILOT_SYMBOLS),
        "selection_criteria": [
            "Monday week start with five full NYSE RTH sessions (no full-day holidays in week)",
            "No partial/early-close sessions in v0.1 calendar support",
            "Same four pilot symbols as baseline",
            "Temporal proximity tiers (immediate prior, mid, early) — not chosen by profitability",
            "20/20 bar sessions required after acquisition (78 interval-start bars per session)",
            "Week rejected if bar completeness fails; replacement must be documented separately",
        ],
        "excluded_examples": [
            "2026-06-15 week excluded: Juneteenth full close reduces session count below five",
        ],
        "locked_validation_weeks": selected,
        "candidate_scan": candidates_scored,
        "outcome_blind_lock": True,
        "locked_at_note": "Week list fixed in experiment_week_selection.LOCKED_VALIDATION_WEEKS before pipeline runs.",
    }


def _neutral_behavior_label(week_start: date) -> str:
    if week_start == date(2026, 7, 13):
        return "immediate_pre_baseline_week"
    if week_start == date(2026, 6, 22):
        return "mid_sample_week"
    if week_start == date(2026, 6, 1):
        return "early_sample_week"
    return "validation_week"

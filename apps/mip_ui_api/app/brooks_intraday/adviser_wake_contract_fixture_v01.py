"""Deterministic AMZN/MCD 2026-07-15 wake-contract fixtures (no LLM, no action expectations)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.brooks_intraday.adviser_wake_v01 import (
    WAKE_SAFETY,
    WAKE_WATCH,
    ThesisWakeState,
    evaluate_wake,
)
from app.brooks_intraday.objective_ruleset_v01 import compute_geometry

TRADING_DATE = "2026-07-15"
FROZEN_CALL_ET = "09:30"


@dataclass
class _Bar:
    open: float
    high: float
    low: float
    close: float
    volume: float = 1.0
    ts_ny: datetime | None = None


# Persisted RTH OHLC (MIP.APP.BROOKS_INTRADAY_HISTORICAL_BAR), 2026-07-15.
AMZN_BARS_0930_1025: list[_Bar] = [
    _Bar(249.75, 252.91, 249.73, 251.08, ts_ny=datetime(2026, 7, 15, 9, 30)),
    _Bar(251.08, 252.54, 251.08, 251.73, ts_ny=datetime(2026, 7, 15, 9, 35)),
    _Bar(251.7, 252.75, 250.8, 250.83, ts_ny=datetime(2026, 7, 15, 9, 40)),
    _Bar(250.8, 251.79, 250.36, 251.36, ts_ny=datetime(2026, 7, 15, 9, 45)),
    _Bar(251.37, 251.96, 251.25, 251.76, ts_ny=datetime(2026, 7, 15, 9, 50)),
    _Bar(251.8, 252.47, 251.45, 252.36, ts_ny=datetime(2026, 7, 15, 9, 55)),
    _Bar(252.31, 253.47, 252.13, 253.46, ts_ny=datetime(2026, 7, 15, 10, 0)),
    _Bar(253.49, 253.82, 253.13, 253.42, ts_ny=datetime(2026, 7, 15, 10, 5)),
    _Bar(253.44, 254.26, 253.38, 254.0, ts_ny=datetime(2026, 7, 15, 10, 10)),
    _Bar(253.98, 254.97, 253.98, 254.77, ts_ny=datetime(2026, 7, 15, 10, 15)),
    _Bar(254.82, 254.9, 253.51, 253.87, ts_ny=datetime(2026, 7, 15, 10, 20)),
    _Bar(253.87, 254.78, 253.46, 254.75, ts_ny=datetime(2026, 7, 15, 10, 25)),
]

MCD_BARS_0930_1025: list[_Bar] = [
    _Bar(268.0, 268.06, 265.45, 266.59, ts_ny=datetime(2026, 7, 15, 9, 30)),
    _Bar(266.59, 266.69, 265.75, 266.56, ts_ny=datetime(2026, 7, 15, 9, 35)),
    _Bar(266.48, 266.82, 266.1, 266.38, ts_ny=datetime(2026, 7, 15, 9, 40)),
    _Bar(266.39, 266.6, 266.1, 266.53, ts_ny=datetime(2026, 7, 15, 9, 45)),
    _Bar(266.54, 267.65, 266.42, 267.49, ts_ny=datetime(2026, 7, 15, 9, 50)),
    _Bar(267.47, 267.84, 267.3, 267.72, ts_ny=datetime(2026, 7, 15, 9, 55)),
    _Bar(267.57, 268.09, 267.42, 267.82, ts_ny=datetime(2026, 7, 15, 10, 0)),
    _Bar(267.89, 268.37, 267.48, 267.99, ts_ny=datetime(2026, 7, 15, 10, 5)),
    _Bar(267.99, 267.99, 267.36, 267.8, ts_ny=datetime(2026, 7, 15, 10, 10)),
    _Bar(267.79, 268.58, 267.78, 268.43, ts_ny=datetime(2026, 7, 15, 10, 15)),
    _Bar(268.51, 268.95, 268.1, 268.92, ts_ny=datetime(2026, 7, 15, 10, 20)),
    _Bar(268.93, 269.16, 268.69, 268.89, ts_ny=datetime(2026, 7, 15, 10, 25)),
]

AMZN_FT_PREDICATE_25291: dict[str, Any] = {
    "id": "b711dd0f6717469b",
    "type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
    "level": 252.91,
    "valid_from_bar_offset": 1,
    "valid_until_bar_offset": 3,
    "minimum_body_fraction": 0.5,
    "minimum_close_location": 0.5,
}

MCD_FT_PREDICATE_26659: dict[str, Any] = {
    "id": "68ddbf888604dfdb",
    "type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
    "level": 266.59,
    "valid_from_bar_offset": 1,
    "valid_until_bar_offset": 2,
    "minimum_body_fraction": 0.5,
    "minimum_close_location": 0.5,
}


def _et(bar: _Bar) -> str:
    return (bar.ts_ny or datetime(2000, 1, 1)).strftime("%H:%M")


def replay_watch_wakes(
    bars: list[_Bar],
    watch_predicates: list[dict[str, Any]],
    *,
    watch_issued_at_bar: int = 0,
    max_bar_index: int | None = None,
) -> dict[str, Any]:
    """Replay deterministic wake evaluator from bar 1 through max_bar_index."""
    state = ThesisWakeState(
        watch_predicates=watch_predicates,
        watch_issued_at_bar=watch_issued_at_bar,
        bars_since_meaningful_wake=0,
    )
    prev_geom = None
    watch_wakes: list[str] = []
    safety_wakes: list[str] = []
    end = len(bars) - 1 if max_bar_index is None else min(max_bar_index, len(bars) - 1)
    for i in range(1, end + 1):
        bar = bars[i]
        prev = bars[i - 1]
        recent = bars[max(0, i - 19) : i + 1]
        g = compute_geometry(
            open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume
        )
        decision = evaluate_wake(
            state,
            bar_index=i,
            bar=bar,
            g=g,
            prev_geom=prev_geom,
            recent=recent,
            prev_bar=prev,
        )
        if decision:
            if decision.reason == WAKE_WATCH:
                watch_wakes.append(_et(bar))
            elif decision.reason == WAKE_SAFETY:
                safety_wakes.append(_et(bar))
        prev_geom = {
            "direction": g.direction,
            "range": g.total_range,
            "high": bar.high,
            "low": bar.low,
        }
    return {
        "watch_wake_times_et": watch_wakes,
        "safety_wake_times_et": safety_wakes,
        "watch_blocked_already_fired": state.watch_blocked_already_fired,
    }


def amzn_20260715_ft_replay() -> dict[str, Any]:
    return replay_watch_wakes(
        AMZN_BARS_0930_1025,
        [AMZN_FT_PREDICATE_25291],
        watch_issued_at_bar=0,
        max_bar_index=11,
    )


def mcd_20260715_ft_replay() -> dict[str, Any]:
    return replay_watch_wakes(
        MCD_BARS_0930_1025,
        [MCD_FT_PREDICATE_26659],
        watch_issued_at_bar=0,
        max_bar_index=11,
    )

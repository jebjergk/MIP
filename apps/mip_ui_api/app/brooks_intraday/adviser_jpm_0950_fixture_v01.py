"""Deterministic JPM 2026-07-14 09:45→09:50 methodology fixture (no action expectation)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.brooks_intraday.adviser_methodology_guard_v01 import (
    guard_premature_failed_breakout_market_state,
)
from app.brooks_intraday.adviser_regime_v01 import (
    REGIME_STRONG_BULL_TREND,
    IntradayRegimeState,
)
from app.brooks_intraday.adviser_retrieval_query_v01 import build_retrieval_query
from app.brooks_intraday.adviser_wake_v01 import (
    WAKE_INVALIDATED,
    ThesisWakeState,
    evaluate_wake,
)
from app.brooks_intraday.objective_ruleset_v01 import compute_geometry

JPM_SYMBOL = "JPM"
JPM_TRADING_DATE = "2026-07-14"
BREAKOUT_REFERENCE = 339.99
BAR_0945_ET = "09:45"
BAR_0950_ET = "09:50"


@dataclass
class _Bar:
    open: float
    high: float
    low: float
    close: float
    volume: float = 1.0
    ts_ny: datetime | None = None


# Forensic OHLC at invalidation wake (09:50 bar; prior 09:45 for context).
JPM_BAR_0945 = _Bar(341.50, 341.90, 340.20, 340.85, ts_ny=datetime(2026, 7, 14, 9, 45))
JPM_BAR_0950 = _Bar(340.85, 340.95, 338.98, 339.58, ts_ny=datetime(2026, 7, 14, 9, 50))

PRIOR_THESIS_ECHO = (
    "Position IN_TRADE after bull breakout above session high 339.99. "
    "Normal post-breakout pullback; failed bull breakout bull trap bear pause invalidated."
)


def jpm_0950_invalidation_scenario() -> dict[str, Any]:
    """Build wake + retrieval context at 09:50 without calling LLM or Snowflake."""
    prev_bar = JPM_BAR_0945
    bar = JPM_BAR_0950
    g = compute_geometry(
        open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume
    )
    prev_geom = {
        "direction": "BEARISH",
        "range": prev_bar.high - prev_bar.low,
        "high": prev_bar.high,
        "low": prev_bar.low,
    }
    pred = {
        "id": "068a3309ff436afa",
        "type": "BREAK_BELOW_LEVEL",
        "level": BREAKOUT_REFERENCE,
        "description": "breakout reference support",
    }
    state = ThesisWakeState(invalidation_predicates=[pred])
    decision = evaluate_wake(
        state,
        bar_index=10,
        bar=bar,
        g=g,
        prev_geom=prev_geom,
        recent=[prev_bar, bar],
        prev_bar=prev_bar,
    )
    regime = IntradayRegimeState(
        regime=REGIME_STRONG_BULL_TREND,
        always_in_bias="LONG",
        session_open=337.0,
        session_high=342.47,
        session_low=335.0,
        bars_seen=11,
    )
    wake_detail = decision.detail if decision else ""
    retrieval_query = build_retrieval_query(
        wake_reason=WAKE_INVALIDATED,
        bar_direction=g.direction,
        bar_close=float(bar.close),
        position_state="IN_TRADE",
        intraday_regime=regime.regime,
        wake_detail=wake_detail,
        setup_id="7c128b190a49488a-971284b0",
        thesis_context=PRIOR_THESIS_ECHO,
    )
    guarded_state, guard_note = guard_premature_failed_breakout_market_state(
        wake_reason=WAKE_INVALIDATED,
        wake_detail=wake_detail,
        market_state="FAILED_BULL_BREAKOUT_IN_PROGRESS",
    )
    return {
        "wake_reason": decision.reason if decision else None,
        "wake_detail": wake_detail,
        "intraday_regime": regime.to_dict(),
        "retrieval_query": retrieval_query,
        "guarded_market_state": guarded_state,
        "methodology_guard_note": guard_note,
        "breakout_reference": BREAKOUT_REFERENCE,
        "bar_0950_close": float(bar.close),
    }

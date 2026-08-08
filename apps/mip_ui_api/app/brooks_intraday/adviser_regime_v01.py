"""Structured intraday Brooks regime context (not a trading signal)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .objective_ruleset_v01 import BarGeometry, compute_geometry

REGIME_TREND_FROM_OPEN = "TREND_FROM_OPEN"
REGIME_STRONG_BULL_TREND = "STRONG_BULL_TREND"
REGIME_BULL_CHANNEL = "BULL_CHANNEL"
REGIME_TRADING_RANGE = "TRADING_RANGE"
REGIME_TRANSITION = "TRANSITION"
REGIME_NEUTRAL = "NEUTRAL"

STRONG_BULL_REGIMES = frozenset(
    {
        REGIME_TREND_FROM_OPEN,
        REGIME_STRONG_BULL_TREND,
        REGIME_BULL_CHANNEL,
    }
)


@dataclass
class IntradayRegimeState:
    regime: str = REGIME_NEUTRAL
    always_in_bias: str = "NEUTRAL"
    session_open: float | None = None
    session_high: float | None = None
    session_low: float | None = None
    pullback_low: float | None = None
    bars_seen: int = 0
    bull_bar_count: int = 0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "regime": self.regime,
            "always_in_bias": self.always_in_bias,
            "session_open": self.session_open,
            "session_high": self.session_high,
            "session_low": self.session_low,
            "pullback_low": self.pullback_low,
            "bars_seen": self.bars_seen,
            "notes": self.notes[-3:],
        }


def update_intraday_regime(
    state: IntradayRegimeState,
    *,
    bar: Any,
    g: BarGeometry,
    recent: list[Any],
    bar_index: int,
) -> IntradayRegimeState:
    state.bars_seen = bar_index + 1
    if state.session_open is None:
        state.session_open = float(bar.open)
    state.session_high = max(state.session_high or bar.high, bar.high)
    state.session_low = min(state.session_low or bar.low, bar.low)

    if g.direction == "BULLISH":
        state.bull_bar_count += 1

    sess_range = (state.session_high or bar.high) - (state.session_low or bar.low)
    open_ref = state.session_open or bar.open
    drift_pct = 100.0 * (bar.close - open_ref) / open_ref if open_ref else 0.0

    if bar_index == 0:
        if g.direction == "BULLISH" and (g.body_fraction or 0) >= 0.55 and (g.close_location or 0) >= 0.65:
            state.regime = REGIME_TREND_FROM_OPEN
            state.always_in_bias = "LONG"
            state.notes.append("Opening bar supports trend-from-open context.")
        else:
            state.regime = REGIME_TRANSITION
        return state

    # Pullback low since last session high (simple running min of lows after bar 0)
    if len(recent) >= 2:
        state.pullback_low = min(float(b.low) for b in recent)

    overlap = _overlap_score(recent[-6:]) if len(recent) >= 4 else 0.0

    if drift_pct >= 1.0 and state.bull_bar_count >= max(2, state.bars_seen // 2):
        if sess_range >= 0.015 * open_ref and bar.close >= (state.session_high or bar.high) - 0.35 * sess_range:
            state.regime = REGIME_STRONG_BULL_TREND
            state.always_in_bias = "LONG"
        elif state.regime == REGIME_TREND_FROM_OPEN:
            state.regime = REGIME_STRONG_BULL_TREND
            state.always_in_bias = "LONG"
    elif state.regime == REGIME_TREND_FROM_OPEN and drift_pct > 0.3:
        state.always_in_bias = "LONG"

    if overlap >= 0.55 and sess_range > 0 and bar_index >= 4:
        if state.regime not in (REGIME_TREND_FROM_OPEN, REGIME_STRONG_BULL_TREND):
            state.regime = REGIME_TRADING_RANGE
            state.always_in_bias = "NEUTRAL"
        else:
            state.regime = REGIME_BULL_CHANNEL
            state.always_in_bias = "LONG"

    if state.regime == REGIME_NEUTRAL and bar_index >= 2:
        state.regime = REGIME_TRANSITION

    return state


def _overlap_score(bars: list[Any]) -> float:
    if len(bars) < 3:
        return 0.0
    ranges = [float(b.high - b.low) for b in bars if b.high > b.low]
    if not ranges:
        return 0.0
    avg = sum(ranges) / len(ranges)
    total = max(float(b.high) for b in bars) - min(float(b.low) for b in bars)
    if total <= 0:
        return 1.0
    return max(0.0, min(1.0, avg / total))

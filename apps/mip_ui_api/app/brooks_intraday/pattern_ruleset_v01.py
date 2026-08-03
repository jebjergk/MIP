"""BROOKS_PATTERN_RULESET_V0_1 — deterministic stateful pattern parameters (not canonical Brooks)."""

from __future__ import annotations

RULESET_VERSION = "BROOKS_PATTERN_RULESET_V0_1"
OBJECTIVE_RULESET_VERSION = "BROOKS_OBJECTIVE_RULESET_V0_1"

# Minimum price increment per pilot symbol (USD)
MIN_TICK_BY_SYMBOL: dict[str, float] = {
    "AAPL": 0.01,
    "AMZN": 0.01,
    "JPM": 0.01,
    "MCD": 0.01,
}

DEFAULT_PARAMETERS: dict[str, object] = {
    "swing_left_bars": 1,
    "swing_right_bars": 1,
    "swing_confirm_bars": 1,
    "pullback_min_bars": 2,
    "pullback_max_bars": 30,
    "h1_h2_expiry_bars": 12,
    "h2_requires_failed_h1": True,
    "double_bottom_max_bars_apart": 40,
    "double_bottom_min_bars_apart": 3,
    "micro_double_bottom_max_bars_apart": 8,
    "double_bottom_tolerance_ticks": 3,
    "micro_double_bottom_tolerance_ticks": 2,
    "double_bottom_neckline_min_ticks": 2,
    "wedge_push_min_separation_bars": 1,
    "wedge_diminish_min_ratio": 0.85,
    "two_leg_min_leg_bars": 2,
    "two_leg_min_separation_bars": 2,
    "two_leg_min_counter_move_ticks": 4,
    "breakout_min_ticks_beyond_swing": 1,
    "breakout_pullback_max_bars": 8,
    "failed_breakout_reentry_bars": 3,
    "micro_channel_min_bars": 4,
    "climax_min_consecutive_dir_bars": 3,
    "climax_large_bar_multiplier": 1.5,
    "pattern_expiry_bars_default": 15,
    "volatility_range_median_lookback": 20,
    "volatility_tolerance_multiplier": 0.15,
}

LIFECYCLE_POSSIBLE = "POSSIBLE"
LIFECYCLE_DEVELOPING = "DEVELOPING"
LIFECYCLE_CONFIRMED = "CONFIRMED"
LIFECYCLE_FAILED = "FAILED"
LIFECYCLE_EXPIRED = "EXPIRED"

ALLOWED_ACTIONS = frozenset({"OBSERVE", "WAIT"})


def tick_size(symbol: str) -> float:
    return float(MIN_TICK_BY_SYMBOL.get(symbol.upper(), 0.01))


def price_tolerance(symbol: str, bar_range: float, params: dict | None, *, micro: bool = False) -> float:
    p = dict(DEFAULT_PARAMETERS if params is None else {**DEFAULT_PARAMETERS, **params})
    tick = tick_size(symbol)
    ticks = int(p["micro_double_bottom_tolerance_ticks" if micro else "double_bottom_tolerance_ticks"])
    tick_tol = tick * ticks
    vol_tol = bar_range * float(p["volatility_tolerance_multiplier"])
    return max(tick_tol, vol_tol)

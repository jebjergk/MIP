"""BROOKS_PATTERN_RULESET_V0_2 — tuned after Phase 5B validation (not canonical Brooks)."""

from __future__ import annotations

from .pattern_ruleset_v01 import (
    ALLOWED_ACTIONS,
    LIFECYCLE_CONFIRMED,
    LIFECYCLE_DEVELOPING,
    LIFECYCLE_EXPIRED,
    LIFECYCLE_FAILED,
    LIFECYCLE_POSSIBLE,
    MIN_TICK_BY_SYMBOL,
    OBJECTIVE_RULESET_VERSION,
    tick_size,
)

RULESET_VERSION = "BROOKS_PATTERN_RULESET_V0_2"

# V0.2 changes (documented):
# - H1 must exist as POSSIBLE >= min bars before FAILED; pullback expiry -> EXPIRED not silent drop
# - H2 requires parent H1 instance id and failed/confirmed H1
# - Double bottom: min intervening bounce vs session median range; dedupe low pairs; micro bar window disjoint from normal
# - Swings: LOCAL_PIVOT vs STRUCTURAL_SWING with min excursion/separation
# - Per-family expiry replaces global 15 for most families
# - Micro channel updates one instance; structural breakout dedupes by level

DEFAULT_PARAMETERS: dict[str, object] = {
    "swing_left_bars": 1,
    "swing_right_bars": 1,
    "structural_min_excursion_ticks": 8,
    "structural_min_separation_bars": 3,
    "pullback_min_bars": 2,
    "pullback_max_bars": 30,
    "h1_min_bars_before_fail": 2,
    "h1_h2_expiry_bars": 10,
    "h2_requires_failed_h1": True,
    "double_bottom_max_bars_apart": 35,
    "double_bottom_min_bars_apart": 6,
    "micro_double_bottom_max_bars_apart": 5,
    "micro_double_bottom_min_bars_apart": 2,
    "double_bottom_tolerance_ticks": 2,
    "micro_double_bottom_tolerance_ticks": 1,
    "double_bottom_min_bounce_range_fraction": 0.35,
    "double_bottom_max_reuse_per_low": 2,
    "wedge_push_min_separation_bars": 2,
    "wedge_diminish_min_ratio": 0.82,
    "two_leg_min_leg_bars": 2,
    "two_leg_min_separation_bars": 3,
    "two_leg_min_counter_move_ticks": 5,
    "breakout_min_ticks_beyond_swing": 2,
    "breakout_level_reuse_cooldown_bars": 6,
    "micro_channel_min_bars": 4,
    "climax_min_consecutive_dir_bars": 3,
    "pattern_expiry_bars_default": 12,
    "volatility_range_median_lookback": 20,
    "volatility_tolerance_multiplier": 0.12,
    "expiry_bars_by_family": {
        "H1": 10,
        "H2": 12,
        "MICRO_DOUBLE": 8,
        "DOUBLE_BOTTOM": 18,
        "WEDGE": 20,
        "TWO_LEG": 16,
        "STRUCTURAL_BREAKOUT": 10,
        "FAILED_BREAKOUT": 8,
        "MICRO_CHANNEL": 14,
        "CLIMAX": 6,
        "LOCAL_PIVOT": 8,
        "PULLBACK": 14,
    },
}


def resolve_params(overrides: dict | None = None) -> dict[str, object]:
    base = dict(DEFAULT_PARAMETERS)
    if overrides:
        base.update(overrides)
    return base


def expiry_bars_for_family(pattern_family: str, params: dict) -> int:
    fam = pattern_family.upper()
    by_fam = params.get("expiry_bars_by_family") or {}
    if isinstance(by_fam, dict):
        for key, bars in by_fam.items():
            if key.upper() in fam:
                return int(bars)
    return int(params.get("pattern_expiry_bars_default", 12))


def price_tolerance(symbol: str, bar_range: float, params: dict | None, *, micro: bool = False) -> float:
    p = resolve_params(params)
    tick = tick_size(symbol)
    ticks = int(p["micro_double_bottom_tolerance_ticks" if micro else "double_bottom_tolerance_ticks"])
    tick_tol = tick * ticks
    vol_tol = bar_range * float(p["volatility_tolerance_multiplier"])
    return max(tick_tol, vol_tol)


def median_bar_range(bars: list, lookback: int) -> float:
    if not bars:
        return 0.0
    window = bars[-lookback:]
    ranges = [max(b.high - b.low, tick_size(b.symbol)) for b in window]
    ranges.sort()
    mid = len(ranges) // 2
    return ranges[mid] if ranges else 0.0

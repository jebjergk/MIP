"""BROOKS_CONTEXT_RULESET_V0_4 — intraday-led context (daily pack = bias/map only)."""

from __future__ import annotations

from .context_ruleset_v03 import DEFAULT_PARAMETERS as V03_PARAMS
from .context_ruleset_v03 import RULESET_VERSION as V03_RS

RULESET_VERSION = "BROOKS_CONTEXT_RULESET_V0_4"

# Daily bias (starting bias only — not session vetoes)
BIAS_POSITIVE = "POSITIVE_HTF_BIAS"
BIAS_MILD_POSITIVE = "MILD_POSITIVE_BIAS"
BIAS_NEUTRAL_POSITIVE = "NEUTRAL_POSITIVE_PULLBACK"
BIAS_NEUTRAL_RECLAIM = "NEUTRAL_RECLAIM"
BIAS_NEUTRAL_INDEPENDENT = "NEUTRAL_REQUIRE_INTRADAY_THESIS"
BIAS_NEUTRAL_CAUTIOUS = "NEUTRAL_CAUTIOUS_STRONGER_CONFIRMATION"

# Intraday thesis paths
PATH_NONE = "NONE"
PATH_A_BREAKOUT_PULLBACK = "PATH_A_BREAKOUT_PULLBACK"
PATH_B_H2_TWO_LEG = "PATH_B_H2_TWO_LEG_PULLBACK"
PATH_C_FAILED_BEAR_SUPPORT = "PATH_C_FAILED_BEAR_SUPPORT"
PATH_D_DOUBLE_BOTTOM = "PATH_D_DOUBLE_BOTTOM"
PATH_E_TREND_RESUMPTION = "PATH_E_TREND_RESUMPTION"

THESIS_NONE = "NONE"
THESIS_DEVELOPING = "DEVELOPING"
THESIS_QUALIFIED = "QUALIFIED"
THESIS_ENTRY_ARMED = "ENTRY_ARMED"
THESIS_FAILED = "FAILED"
THESIS_CONSUMED = "CONSUMED"

ALIGN_ALIGNED = "ALIGNED"
ALIGN_DAILY_NEUTRAL = "DAILY_NEUTRAL"
ALIGN_INTRADAY_OVERRIDE = "INTRADAY_OVERRIDE"
ALIGN_CONFLICT_STRONG_CONFIRM = "CONFLICT_REQUIRES_STRONG_CONFIRMATION"

# Volatility regimes
VOL_LOW_COMPRESSION = "LOW_VOLATILITY_COMPRESSION"
VOL_NORMAL = "NORMAL_VOLATILITY"
VOL_EXPANSION = "VOLATILITY_EXPANSION"
VOL_HIGH_DIRECTIONAL = "HIGH_VOLATILITY_DIRECTIONAL"
VOL_HIGH_CHAOTIC = "HIGH_VOLATILITY_CHAOTIC"

BLOCKER_CHAOTIC_VOLATILITY = "CHAOTIC_VOLATILITY_WITHOUT_CONFIRMATION"
BLOCKER_COMPRESSION_NO_BREAKOUT = "LOW_VOL_COMPRESSION_AWAIT_BREAKOUT_CONFIRMATION"
BLOCKER_DAILY_CONFLICT_CONFIRMATION = "DAILY_BEARISH_REQUIRES_STRONGER_CONFIRMATION"
BLOCKER_INTRADAY_THESIS_FAILED = "INTRADAY_THESIS_FAILED"
BLOCKER_POSSIBLE_PATTERN_ONLY = "POSSIBLE_PATTERN_INSUFFICIENT_FOR_ENTRY"

DEFAULT_PARAMETERS: dict[str, object] = {
    **V03_PARAMS,
    "ruleset_version": RULESET_VERSION,
    "underlying_context_ruleset": V03_RS,
    "vol_tr_median_window": 12,
    "vol_tr_avg_window": 6,
    "vol_opening_range_bars": 6,
    "vol_low_ratio": 0.55,
    "vol_high_ratio": 1.45,
    "vol_overlap_high": 0.62,
    "vol_directional_efficiency_high": 0.55,
    "vol_directional_efficiency_low": 0.28,
    "daily_bearish_trends": ["BEARISH", "DOWN", "DOWNTREND", "BEAR"],
    "verdict_initial_state": {
        **(V03_PARAMS.get("verdict_initial_state") or {}),  # type: ignore[arg-type]
        "DEFER": "WAITING_FOR_RTH_CONFIRMATION",
        "NO_CLEAR_LONG": "WAITING_FOR_RTH_CONFIRMATION",
    },
    "verdict_initial_action": {
        **(V03_PARAMS.get("verdict_initial_action") or {}),  # type: ignore[arg-type]
        "DEFER": "OBSERVE",
        "NO_CLEAR_LONG": "OBSERVE",
    },
}


def resolve_params(overrides: dict | None = None) -> dict[str, object]:
    base = dict(DEFAULT_PARAMETERS)
    if overrides:
        base.update(overrides)
    return base


def daily_bias_class(raw_verdict: str | None) -> str:
    v = str(raw_verdict or "").upper()
    if v in ("LONG_BIAS",):
        v = "LONG_APPROVE"
    if v == "LONG_APPROVE":
        return BIAS_POSITIVE
    if v == "LONG_APPROVE_REDUCED":
        return BIAS_MILD_POSITIVE
    if v in ("SUPPORT_HOLD", "WAIT_PULLBACK"):
        return BIAS_NEUTRAL_POSITIVE
    if v in ("RECLAIM_REQUIRED", "WAIT_RECLAIM"):
        return BIAS_NEUTRAL_RECLAIM
    if v == "NO_CLEAR_LONG":
        return BIAS_NEUTRAL_INDEPENDENT
    if v == "DEFER":
        return BIAS_NEUTRAL_CAUTIOUS
    return BIAS_NEUTRAL_INDEPENDENT


def effective_verdict_for_v03_engine(raw_verdict: str | None) -> str:
    """Map daily verdicts that V0.3 treats as hard vetoes to neutral intraday-upgrade labels."""
    v = str(raw_verdict or "").upper()
    if v in ("LONG_BIAS",):
        v = "LONG_APPROVE"
    if v == "DEFER":
        return "NO_CLEAR_LONG"
    return v


__all__ = [
    "RULESET_VERSION",
    "DEFAULT_PARAMETERS",
    "resolve_params",
    "daily_bias_class",
    "effective_verdict_for_v03_engine",
    "PATH_A_BREAKOUT_PULLBACK",
    "PATH_B_H2_TWO_LEG",
    "PATH_C_FAILED_BEAR_SUPPORT",
    "PATH_D_DOUBLE_BOTTOM",
    "PATH_E_TREND_RESUMPTION",
    "THESIS_DEVELOPING",
    "THESIS_QUALIFIED",
    "THESIS_ENTRY_ARMED",
    "THESIS_FAILED",
    "THESIS_CONSUMED",
    "VOL_LOW_COMPRESSION",
    "VOL_NORMAL",
    "VOL_EXPANSION",
    "VOL_HIGH_DIRECTIONAL",
    "VOL_HIGH_CHAOTIC",
    "BLOCKER_CHAOTIC_VOLATILITY",
    "BLOCKER_COMPRESSION_NO_BREAKOUT",
    "BLOCKER_DAILY_CONFLICT_CONFIRMATION",
    "BLOCKER_INTRADAY_THESIS_FAILED",
    "BLOCKER_POSSIBLE_PATTERN_ONLY",
    "ALIGN_ALIGNED",
    "ALIGN_DAILY_NEUTRAL",
    "ALIGN_INTRADAY_OVERRIDE",
    "ALIGN_CONFLICT_STRONG_CONFIRM",
]

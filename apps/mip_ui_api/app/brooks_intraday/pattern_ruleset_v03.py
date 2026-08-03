"""BROOKS_PATTERN_RULESET_V0_3 — Phase 5C double-bottom hierarchy + symmetric micro channels."""

from __future__ import annotations

from .pattern_ruleset_v02 import (
    ALLOWED_ACTIONS,
    DEFAULT_PARAMETERS as V02_PARAMS,
    expiry_bars_for_family,
    median_bar_range,
    resolve_params as resolve_v02,
    tick_size,
)

RULESET_VERSION = "BROOKS_PATTERN_RULESET_V0_3"

# V0.3 (Phase 5C) — documented deltas from V0.2:
# - Double-bottom hierarchy: MICRO / LOCAL / STRUCTURAL / LOW_RETEST (weak bounce only)
# - One active instance per canonical low-pair key; update in place
# - Stronger bounce fractions per tier; structural requires structural swing proximity
# - Symmetric BEAR_MICRO_CHANNEL with tick-aware tolerance; separate active instance ids
# - Channel CONFIRMED after min_bars; FAILED on opposing break; no per-bar duplicate instances

DEFAULT_PARAMETERS: dict[str, object] = {
    **V02_PARAMS,
    "double_bottom_min_bars_apart": 8,
    "micro_double_bottom_max_bars_apart": 4,
    "micro_double_bottom_min_bars_apart": 2,
    "local_double_bottom_max_bars_apart": 14,
    "structural_double_bottom_min_bars_apart": 15,
    "micro_db_min_bounce_fraction": 0.28,
    "local_db_min_bounce_fraction": 0.45,
    "structural_db_min_bounce_fraction": 0.55,
    "low_retest_min_bounce_fraction": 0.12,
    "low_retest_max_bounce_fraction": 0.28,
    "double_bottom_max_reuse_per_pair": 1,
    "structural_db_near_swing_ticks": 6,
    "micro_channel_min_bars": 4,
    "micro_channel_confirm_bars": 6,
    "micro_channel_tick_tolerance": 1,
    "congestion_range_fraction": 0.35,
    "expiry_bars_by_family": {
        **(V02_PARAMS.get("expiry_bars_by_family") or {}),  # type: ignore[arg-type]
        "MICRO_DOUBLE": 8,
        "LOCAL_DOUBLE": 16,
        "STRUCTURAL_DOUBLE": 22,
        "LOW_RETEST": 6,
        "MICRO_CHANNEL": 16,
    },
}


def resolve_params(overrides: dict | None = None) -> dict[str, object]:
    base = dict(DEFAULT_PARAMETERS)
    if overrides:
        base.update(overrides)
    return base

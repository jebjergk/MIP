"""BROOKS_CONTEXT_RULESET_V0_2 — Phase 6B calibration from audit."""

from __future__ import annotations

from .context_ruleset_v01 import (
    ACTION_CONSIDER_ENTRY,
    ACTION_DO_NOT_CHASE,
    ACTION_DO_NOT_ENTER,
    ACTION_ENTRY_ARMED,
    ACTION_OBSERVE,
    ACTION_THESIS_INVALIDATED,
    ACTION_THESIS_WEAKENED,
    ACTION_WAIT,
    ACTION_WAIT_FOR_FOLLOW_THROUGH,
    ACTION_WAIT_FOR_NEW_SETUP,
    ACTION_WAIT_FOR_PULLBACK,
    ACTION_WAIT_FOR_RECLAIM,
    ACTION_WAIT_FOR_SUPPORT,
    DEFAULT_PARAMETERS as V01_PARAMS,
    STATE_WAITING_FOR_NEW_SETUP,
    resolve_params as resolve_v01,
)

RULESET_VERSION = "BROOKS_CONTEXT_RULESET_V0_2"

# V0.2 deltas (Phase 6B):
# - Trust frozen trade_simulation_ready; recompute only upgrades never downgrades
# - LIMITED_ROOM -> WAIT + ENTRY_BLOCKED, not DO_NOT_ENTER
# - Missing confirmation -> WAIT, not DO_NOT_ENTER
# - Intraday setup break -> THESIS_WEAKENED / WAIT_FOR_NEW_SETUP, not daily invalidation
# - Persisted do_not_enter_reason + entry_condition_matrix on each bar
# - V0.3 pattern family aliases in significance map
# - Room vs resistance uses session range denominator
# - Already-invalidated sessions emit OBSERVE not repeated THESIS_INVALIDATED action

DEFAULT_PARAMETERS: dict[str, object] = {
    **V01_PARAMS,
    "ruleset_version": RULESET_VERSION,
    "pattern_significance_entry_capable": [
        "STRUCTURAL_DOUBLE_BOTTOM",
        "LOCAL_DOUBLE_BOTTOM",
        "POSSIBLE_H2_LONG",
        "H2_LONG",
        "CONFIRMED_H2_LONG",
        "TWO_LEGGED_PULLBACK",
        "CONFIRMED_WEDGE_BOTTOM",
        "WEDGE_BOTTOM",
        "STRUCTURAL_BREAKOUT",
        "FAILED_BEAR_BREAKOUT",
        "BULL_MICRO_CHANNEL",
    ],
    "pattern_family_aliases": {
        "CONFIRMED_H2_LONG": "POSSIBLE_H2_LONG",
        "H2_LONG": "POSSIBLE_H2_LONG",
        "WEDGE_BOTTOM": "CONFIRMED_WEDGE_BOTTOM",
    },
    "verdict_initial_state": {
        **(V01_PARAMS.get("verdict_initial_state") or {}),  # type: ignore[arg-type]
        "DEFER": "OBSERVATION_ONLY",
        "WAIT_RECLAIM": "WAITING_FOR_RECLAIM",
        "NO_CLEAR_LONG": "OBSERVATION_ONLY",
    },
    "verdict_initial_action": {
        **(V01_PARAMS.get("verdict_initial_action") or {}),  # type: ignore[arg-type]
        "DEFER": ACTION_OBSERVE,
        "WAIT_RECLAIM": ACTION_WAIT_FOR_RECLAIM,
        "NO_CLEAR_LONG": ACTION_OBSERVE,
    },
    "positive_do_not_enter_verdicts": ["NO_CLEAR_LONG"],
    "action_precedence": [
        "DATA_ERROR",
        "THESIS_INVALIDATED",
        "DO_NOT_ENTER",
        "DO_NOT_CHASE",
        "ENTRY_BLOCKED",
        "THESIS_WEAKENED",
        "WAIT_FOR_NEW_SETUP",
        "CONSIDER_ENTRY",
        "ENTRY_ARMED",
        "WAIT_FOR_FOLLOW_THROUGH",
        "WAIT_FOR_RECLAIM",
        "WAIT_FOR_SUPPORT",
        "WAIT_FOR_PULLBACK",
        "WAIT",
        "OBSERVE",
    ],
}


def resolve_params(overrides: dict | None = None) -> dict[str, object]:
    base = dict(DEFAULT_PARAMETERS)
    if overrides:
        base.update(overrides)
    return base

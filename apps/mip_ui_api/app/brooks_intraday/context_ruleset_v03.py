"""BROOKS_CONTEXT_RULESET_V0_3 — Phase D intraday upgrade path (isolated from V0.2).

LAST_ENTRY_BAR_INDEX_IN_SESSION = 72 matches simulation cutoff.
Bar index 72 opens at 15:30 ET (~30 minutes before forced EOD flatten on bar 77 close).
"""

from __future__ import annotations

from .context_ruleset_v01 import (
    ACTION_CONSIDER_ENTRY,
    ACTION_HOLD_POSITION,
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
    ROOM_ACCEPTABLE,
    ROOM_AMPLE,
    ROOM_AT_RESISTANCE,
    ROOM_LIMITED,
    STATE_DO_NOT_CHASE,
    STATE_ENTRY_ARMED,
    STATE_ENTRY_BLOCKED,
    STATE_OBSERVATION_ONLY,
    STATE_THESIS_INVALIDATED,
    STATE_WAITING_FOR_PULLBACK,
    STATE_WAITING_FOR_RECLAIM,
)
from .context_ruleset_v02 import DEFAULT_PARAMETERS as V02_PARAMS

RULESET_VERSION = "BROOKS_CONTEXT_RULESET_V0_3"

# --- New advisory states ---
STATE_INTRADAY_UPGRADE_CANDIDATE = "INTRADAY_UPGRADE_CANDIDATE"
STATE_WAITING_FOR_BREAKOUT_CONFIRMATION = "WAITING_FOR_BREAKOUT_CONFIRMATION"
STATE_WAITING_FOR_BREAKOUT_PULLBACK = "WAITING_FOR_BREAKOUT_PULLBACK"
STATE_FAILED_BREAKOUT = "FAILED_BREAKOUT"

# --- Resistance lifecycle ---
RL_UNBROKEN = "UNBROKEN"
RL_BROKEN_PENDING = "BROKEN_PENDING_CONFIRMATION"
RL_BROKEN_CONFIRMED = "BROKEN_CONFIRMED"
RL_FAILED_BREAKOUT = "FAILED_BREAKOUT"

# --- Room (V0.3 additive) ---
ROOM_UNKNOWN_NO_NEXT = "UNKNOWN_NO_NEXT_RESISTANCE"

# --- Verdict classes ---
VERDICT_CLASS_AUTHORIZED_DAILY = "AUTHORIZED_DAILY"
VERDICT_CLASS_NO_CLEAR_LONG = "NO_CLEAR_LONG"
VERDICT_CLASS_DEFER = "DEFER"
VERDICT_CLASS_OTHER = "OTHER"

VERDICT_ALIASES = {
    "LONG_BIAS": "LONG_APPROVE",
    "SUPPORT_HOLD": "WAIT_PULLBACK",
    "RECLAIM_REQUIRED": "WAIT_RECLAIM",
}

AUTHORIZED_DAILY_VERDICTS = frozenset(
    {"WAIT_PULLBACK", "LONG_APPROVE", "LONG_APPROVE_REDUCED", "WAIT_RECLAIM"}
)

# --- Blockers ---
BLOCKER_NO_DAILY_LONG_AUTHORIZATION = "NO_DAILY_LONG_AUTHORIZATION"
BLOCKER_WAITING_FOR_INTRADAY_UPGRADE = "WAITING_FOR_INTRADAY_UPGRADE"
BLOCKER_BREAKOUT_CANDIDATE_NOT_CONFIRMED = "BREAKOUT_CANDIDATE_NOT_CONFIRMED"
BLOCKER_WAITING_FOR_BREAKOUT_FOLLOW_THROUGH = "WAITING_FOR_BREAKOUT_FOLLOW_THROUGH"
BLOCKER_WAITING_FOR_BREAKOUT_PULLBACK = "WAITING_FOR_BREAKOUT_PULLBACK"
BLOCKER_AT_UNBROKEN_RESISTANCE = "AT_UNBROKEN_RESISTANCE"
BLOCKER_LIMITED_ROOM_TO_UNBROKEN = "LIMITED_ROOM_TO_UNBROKEN_RESISTANCE"
BLOCKER_LIMITED_ROOM_TO_NEXT = "LIMITED_ROOM_TO_NEXT_RESISTANCE"
BLOCKER_NO_VALID_NEXT_RESISTANCE = "NO_VALID_NEXT_RESISTANCE"
BLOCKER_FAILED_BREAKOUT = "FAILED_BREAKOUT"
BLOCKER_DO_NOT_CHASE_EXTENSION = "DO_NOT_CHASE_EXTENSION"
BLOCKER_BEARISH_CANCELLATION = "BEARISH_CANCELLATION"
BLOCKER_DAILY_THESIS_INVALIDATED = "DAILY_THESIS_INVALIDATED"
BLOCKER_INSUFFICIENT_TIME = "INSUFFICIENT_TIME_REMAINING"
BLOCKER_ENTRY_RISK_TOO_LARGE = "ENTRY_RISK_TOO_LARGE"
BLOCKER_DEFERRED_BY_DAILY_THESIS = "DEFERRED_BY_DAILY_THESIS"
BLOCKER_CANDIDATE_EXPIRED = "CANDIDATE_EXPIRED"
BLOCKER_GAP_ACCEPTANCE_REQUIRED = "GAP_ACCEPTANCE_REQUIRED"
BLOCKER_ONE_POSITION_TIEBREAK_SKIP = "ONE_POSITION_TIEBREAK_SKIP"

# Timing: index 72 = 15:30 ET open; ~30 minutes before forced flatten (bar 77 close).
LAST_ENTRY_BAR_INDEX_IN_SESSION = 72
LAST_ENTRY_BAR_OPENS_ET = "15:30"
FORCED_FLATTEN_NOTE = (
    "Bar index 72 opens 15:30 ET (~30 minutes before forced EOD flatten on final bar close)."
)

DEFAULT_PARAMETERS: dict[str, object] = {
    **V02_PARAMS,
    "ruleset_version": RULESET_VERSION,
    # Room (unchanged vs V0.2)
    "min_room_acceptable_fraction": 0.35,
    "min_room_ample_fraction": 0.55,
    "resistance_proximity_fraction": 0.12,
    # Breakout confirmation
    "breakout_confirm_window_bars": 3,  # N
    "breakout_acceptance_closes": 2,  # M
    "breakout_fail_tolerance_fraction": 0.05,
    # Pullback / consolidation
    "pullback_retest_tolerance_fraction": 0.08,
    "pullback_hold_tolerance_fraction": 0.05,
    "consolidation_min_bars": 4,
    "consolidation_max_range_fraction": 0.35,
    "consolidation_dip_tolerance_fraction": 0.05,
    "consolidation_expiry_bars": 12,
    # Do-not-chase
    "dnc_extension_from_break_fraction": 0.55,
    "dnc_consecutive_bull_bars": 3,
    "dnc_large_bar_fraction": 0.40,
    # Risk / time
    "max_entry_risk_fraction": 0.40,
    "last_entry_bar_index_in_session": LAST_ENTRY_BAR_INDEX_IN_SESSION,
    "opening_observation_bars": 3,
    # Expiry
    "expiry_upgrade_candidate_bars": 6,
    "expiry_breakout_confirmation_bars": 3,
    "expiry_breakout_pullback_bars": 12,
    # Gap / geometry
    "gap_acceptance_closes": 2,
    "bear_reversal_near_low_fraction": 0.25,
    "close_near_high_top_fraction": 0.25,
    "symbol_priority": ["AAPL", "AMZN", "JPM", "MCD"],
    "verdict_initial_state": {
        **(V02_PARAMS.get("verdict_initial_state") or {}),  # type: ignore[arg-type]
        "DEFER": STATE_OBSERVATION_ONLY,
        "NO_CLEAR_LONG": STATE_OBSERVATION_ONLY,
        "WAIT_PULLBACK": STATE_WAITING_FOR_PULLBACK,
        "WAIT_RECLAIM": STATE_WAITING_FOR_RECLAIM,
        "LONG_APPROVE": "WAITING_FOR_RTH_CONFIRMATION",
        "LONG_APPROVE_REDUCED": "WAITING_FOR_STRONGER_CONFIRMATION",
    },
    "verdict_initial_action": {
        **(V02_PARAMS.get("verdict_initial_action") or {}),  # type: ignore[arg-type]
        "DEFER": ACTION_DO_NOT_ENTER,
        "NO_CLEAR_LONG": ACTION_OBSERVE,
    },
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


def normalize_verdict(raw: str | None) -> str:
    v = str(raw or "DEFAULT").upper()
    return VERDICT_ALIASES.get(v, v)


def verdict_class(raw: str | None) -> str:
    v = normalize_verdict(raw)
    if v == "DEFER":
        return VERDICT_CLASS_DEFER
    if v == "NO_CLEAR_LONG":
        return VERDICT_CLASS_NO_CLEAR_LONG
    if v in AUTHORIZED_DAILY_VERDICTS:
        return VERDICT_CLASS_AUTHORIZED_DAILY
    return VERDICT_CLASS_OTHER


__all__ = [
    "RULESET_VERSION",
    "DEFAULT_PARAMETERS",
    "resolve_params",
    "normalize_verdict",
    "verdict_class",
    "LAST_ENTRY_BAR_INDEX_IN_SESSION",
    "LAST_ENTRY_BAR_OPENS_ET",
    "FORCED_FLATTEN_NOTE",
    "STATE_INTRADAY_UPGRADE_CANDIDATE",
    "STATE_WAITING_FOR_BREAKOUT_CONFIRMATION",
    "STATE_WAITING_FOR_BREAKOUT_PULLBACK",
    "STATE_FAILED_BREAKOUT",
    "STATE_OBSERVATION_ONLY",
    "STATE_ENTRY_ARMED",
    "STATE_ENTRY_BLOCKED",
    "STATE_DO_NOT_CHASE",
    "STATE_THESIS_INVALIDATED",
    "RL_UNBROKEN",
    "RL_BROKEN_PENDING",
    "RL_BROKEN_CONFIRMED",
    "RL_FAILED_BREAKOUT",
    "ROOM_UNKNOWN_NO_NEXT",
    "ROOM_AMPLE",
    "ROOM_ACCEPTABLE",
    "ROOM_LIMITED",
    "ROOM_AT_RESISTANCE",
    "ACTION_CONSIDER_ENTRY",
    "ACTION_ENTRY_ARMED",
    "ACTION_DO_NOT_ENTER",
    "ACTION_DO_NOT_CHASE",
    "ACTION_OBSERVE",
    "ACTION_WAIT",
    "ACTION_WAIT_FOR_FOLLOW_THROUGH",
    "ACTION_WAIT_FOR_RECLAIM",
    "ACTION_WAIT_FOR_PULLBACK",
    "ACTION_WAIT_FOR_SUPPORT",
    "ACTION_WAIT_FOR_NEW_SETUP",
    "ACTION_THESIS_INVALIDATED",
    "ACTION_THESIS_WEAKENED",
]

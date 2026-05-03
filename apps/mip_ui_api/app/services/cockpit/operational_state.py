"""
Derived operational state for structural trade proposals (UI contract).

All values are computed in the API layer only — never persisted.
"""
from __future__ import annotations

from typing import Optional

ACTIONABLE_PROPOSAL = "ACTIONABLE_PROPOSAL"
MONITOR = "MONITOR"
NOT_ACTIONABLE = "NOT_ACTIONABLE"
INVALIDATED = "INVALIDATED"
EXECUTED = "EXECUTED"
UNKNOWN = "UNKNOWN"

_ACTIONABLE_ACTIONS = frozenset({"PROPOSE_LONG", "PROPOSE_SHORT"})
_MONITOR_ACTIONS = frozenset(
    {
        "WATCH_LONG",
        "WATCH_SHORT",
        "WATCH_LONG_FAILURE",
        "WATCH_SHORT_FAILURE",
        "WAIT_FOR_CONFIRMATION",
    }
)
_NOT_ACTIONABLE_ACTIONS = frozenset({"NO_TRADE", "REJECT"})


def _norm_action(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip().upper()
    return s or None


def derive_operational_state(
    *,
    latest_board_action: Optional[str],
    phase4_health_present: bool,
    executed: bool,
    live_invalidated: bool,
) -> str:
    """Map Phase 4 chair FINAL_ACTION (+ gates) to operator-facing state.

    Priority: EXECUTED > INVALIDATED (live) > Phase 4 action mapping > UNKNOWN.
    """
    if executed:
        return EXECUTED
    if live_invalidated:
        return INVALIDATED
    if not phase4_health_present:
        return UNKNOWN
    action = _norm_action(latest_board_action)
    if not action:
        return UNKNOWN
    if action in _ACTIONABLE_ACTIONS:
        return ACTIONABLE_PROPOSAL
    if action in _MONITOR_ACTIONS:
        return MONITOR
    if action in _NOT_ACTIONABLE_ACTIONS:
        return NOT_ACTIONABLE
    return UNKNOWN

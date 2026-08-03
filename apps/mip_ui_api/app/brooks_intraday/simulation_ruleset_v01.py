"""BROOKS_SIMULATION_RULESET_V0_1 — Phase 7 portfolio simulation (gated on context V0.2)."""

from __future__ import annotations

RULESET_VERSION = "BROOKS_SIMULATION_RULESET_V0_1"

# Pilot week — Phase 6B calibrated context (do not gate on V0.1).
REQUIRED_CONTEXT_RULESET = "BROOKS_CONTEXT_RULESET_V0_2"
DEFAULT_CONTEXT_ATTEMPT_ID = "63ecc779-3dbb-4468-8bc1-a8bbd0f17342"

DEFAULT_PARAMETERS: dict[str, object] = {
    "ruleset_version": RULESET_VERSION,
    "required_context_ruleset": REQUIRED_CONTEXT_RULESET,
    "entry_signal_action": "CONSIDER_ENTRY",
    "min_bars_before_entry": 3,
    "last_entry_bar_index_in_session": 72,  # no new entries after ~15:00 open
    "starting_cash": 1000.0,
    "direction": "LONG",
    "whole_shares_only": True,
    "max_concurrent_positions": 1,
    "entry_fill": "BAR_CLOSE",
    "exit_fill": "NEXT_BAR_OPEN",
    "eod_exit_fill": "FINAL_BAR_CLOSE",
    "tie_break_version": "BROOKS_TIEBREAK_V0_1",
    "paa_rank": ["LONG_APPROVE", "LONG_APPROVE_REDUCED", "WAIT_RECLAIM", "WAIT_PULLBACK", "DEFER", "NO_CLEAR_LONG"],
}


def resolve_params(overrides: dict | None = None) -> dict[str, object]:
    base = dict(DEFAULT_PARAMETERS)
    if overrides:
        base.update(overrides)
    return base

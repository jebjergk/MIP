"""Presentation-only plain-language mappings (stored codes unchanged)."""

from __future__ import annotations

ACTION_LABELS: dict[str, str] = {
    "CONSIDER_ENTRY": "Entry conditions became valid",
    "HOLD_POSITION": "Hold the open trade",
    "DO_NOT_ENTER": "Do not enter",
    "DO_NOT_CHASE": "Price is too extended to enter safely",
    "WAIT_FOR_FOLLOW_THROUGH": "Wait for another bar to confirm the breakout",
    "ENTRY_ARMED": "Setup is ready, but entry still needs confirmation",
    "THESIS_INVALIDATED": "The original trade idea is no longer valid",
    "CONTEXT_FAILED_BREAKOUT": "The breakout failed and the market context deteriorated",
    "WAIT": "Wait",
}

EXIT_REASON_LABELS: dict[str, str] = {
    "PROTECTIVE_STOP_HIT": "Protective stop hit",
    "STOP_GAP_THROUGH": "Stop triggered at the bar open",
    "FORCED_END_OF_DAY_EXIT": "End-of-day exit",
    "THESIS_INVALIDATED": "Trade idea invalidated",
    "CONTEXT_FAILED_BREAKOUT": "Failed breakout exit",
}

PATTERN_LABELS: dict[str, str] = {
    "DOJI": "Indecision bar",
    "INSIDE_BAR": "Price compressed inside the previous bar",
    "OUTSIDE_BAR": "Price traded above and below the previous bar",
    "POSSIBLE_BREAKOUT_BAR": "The bar may be starting a breakout",
    "POSSIBLE_FOLLOW_THROUGH_BAR": "The bar may be confirming the prior move",
    "TWO_LEGGED_PULLBACK": "The pullback developed in two separate pushes",
    "STRUCTURAL_SWING_LOW": "A meaningful higher low was confirmed",
    "STRUCTURAL_SWING_HIGH": "A meaningful swing high was confirmed",
    "FAILED_H1_LONG": "The first long attempt failed",
    "POSSIBLE_H2_LONG": "A second long attempt may be forming",
    "FAILED_BREAKOUT": "Breakout failed",
}

LEDGER_EVENT_LABELS: dict[str, str] = {
    "ENTRY": "Trade opened",
    "INITIAL_STOP_CALCULATED": "Initial stop calculated",
    "STOP_ACTIVATED": "Initial stop activated",
    "STOP_TRAIL_SCHEDULED": "A confirmed structure has justified a future stop adjustment",
    "STOP_TRAIL_UPDATE": "The previously scheduled stop adjustment is now active",
    "STOP_EVAL": "The active protective stop was checked against this bar",
    "EXIT": "Trade closed",
}

REENTRY_BLOCK_LABELS: dict[str, str] = {
    "REENTRY_BLOCKED_AFTER_EOD_EXIT": "No re-entry after end-of-day exit today",
    "REENTRY_COOLDOWN_ACTIVE": "Re-entry cooldown after the previous exit",
    "REENTRY_SETUP_ALREADY_USED": "Previous setup already used",
    "REENTRY_FRESH_ARM_REQUIRED": "Waiting for a new setup after the previous trade",
    "REENTRY_WAIT_FOR_NEW_SETUP": "Waiting for a new setup after the previous trade",
}

TERM_LABELS: dict[str, str] = {
    "PULLBACK_DEVELOPING": "Pullback developing",
    "BEAR_MICRO_CHANNEL": "Bear micro channel developing",
    "BULL_MICRO_CHANNEL": "Bull micro channel developing",
    "POSSIBLE_H1_LONG": "Possible first long attempt",
    "POSSIBLE_H2_LONG": "Possible H1/H2-style continuation structure",
    "TWO_LEGGED_PULLBACK": "Two-legged pullback possible",
}


def translate_action(code: str | None) -> str:
    if not code:
        return "—"
    return ACTION_LABELS.get(str(code).upper(), str(code).replace("_", " ").title())


def translate_exit_reason(code: str | None) -> str:
    if not code:
        return "—"
    return EXIT_REASON_LABELS.get(str(code).upper(), str(code).replace("_", " ").title())


def translate_pattern(family: str | None) -> str:
    if not family:
        return "—"
    key = str(family).upper()
    return PATTERN_LABELS.get(key, key.replace("_", " ").title())


def translate_ledger_event(code: str | None) -> str:
    if not code:
        return "—"
    return LEDGER_EVENT_LABELS.get(str(code).upper(), str(code).replace("_", " ").title())


def translate_sim_block_reason(code: str | None) -> str:
    if not code:
        return "—"
    key = str(code).upper()
    return REENTRY_BLOCK_LABELS.get(key, key.replace("_", " ").title())


def translate_term(term: str | None) -> str:
    if not term:
        return "—"
    key = str(term).upper()
    if key in TERM_LABELS:
        return TERM_LABELS[key]
    return key.replace("_", " ").title()

"""BROOKS_POSITION_MANAGEMENT_RULESET_V0_1 — review-only long hold management (inactive by default)."""

from __future__ import annotations

RULESET_VERSION = "BROOKS_POSITION_MANAGEMENT_RULESET_V0_1"

# Parent sim still uses V0_1 entries; this ruleset version is stored on disposable attempts only.
PARENT_SIMULATION_RULESET = "BROOKS_SIMULATION_RULESET_V0_1"

DEFAULT_PARAMETERS: dict[str, object] = {
    "ruleset_version": RULESET_VERSION,
    "price_tick": 0.01,
    "inactive_review_only": True,
    "exit_reasons": {
        "stop_gap": "STOP_GAP_THROUGH",
        "stop_hit": "PROTECTIVE_STOP_HIT",
        "failed_breakout": "CONTEXT_FAILED_BREAKOUT",
        "thesis": "THESIS_INVALIDATED",
        "eod": "FORCED_END_OF_DAY_EXIT",
    },
}


def resolve_params(overrides: dict | None = None) -> dict[str, object]:
    base = dict(DEFAULT_PARAMETERS)
    if overrides:
        base.update(overrides)
    return base

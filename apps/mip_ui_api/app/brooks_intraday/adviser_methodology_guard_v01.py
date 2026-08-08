"""Guardrails for Brooks methodology interpretation (not entry/exit thresholds)."""

from __future__ import annotations

PREMATURE_FAILURE_MARKET_STATES = frozenset(
    {
        "FAILED_BULL_BREAKOUT",
        "FAILED_BULL_BREAKOUT_IN_PROGRESS",
        "FAILED_BULL_BREAKOUT_CONFIRMED",
        "FAILED_BREAKOUT",
        "BULL_TRAP",
    }
)


def is_level_break_only_invalidation_detail(detail: str) -> bool:
    d = (detail or "").lower()
    return "closed below" in d or "breakout reference" in d


def guard_premature_failed_breakout_market_state(
    *,
    wake_reason: str,
    wake_detail: str,
    market_state: str,
) -> tuple[str, str | None]:
    if wake_reason != "THESIS_INVALIDATED":
        return market_state, None
    upper = (market_state or "").upper().replace(" ", "_")
    if upper not in PREMATURE_FAILURE_MARKET_STATES and "FAILED_BULL" not in upper:
        return market_state, None
    if not is_level_break_only_invalidation_detail(wake_detail):
        return market_state, None
    return (
        "BREAKOUT_REFERENCE_TEST_REASSESSMENT",
        "Level-break wake only; replaced premature failed-breakout market_state label.",
    )

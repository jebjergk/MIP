"""
Phase 1 move_quality — allowed inputs only:
tape_pressure, book_pressure, spread, relative_volume, side_confidence,
warmup_state, baseline_confidence, feed_health, opening_price_discovery_window
"""

from __future__ import annotations

from typing import Literal

from app.threshold_profile import (
    DIRECTIONAL_MID_RET,
    DIRECTIONAL_TAPE_PRESSURE,
    OPENING_TAPE_PRESSURE_MULT,
)

MoveQuality = Literal[
    "insufficient_evidence",
    "neutral_chop",
    "directional_push_up",
    "directional_push_down",
]


def classify_phase1_raw(
    *,
    warmup_state: str,
    baseline_confidence: str,
    feed_health: str,
    side_confidence_aggregate: str,
    tape_pressure_signed: float,
    book_pressure_signed: float | None,
    mid_ret_60s: float | None,
    spread: float | None,
    relative_volume_score: float | None,
    opening_price_discovery_window: bool,
) -> MoveQuality:
    del spread  # allowed input for copy/templates; no hidden logic in Phase 1
    del relative_volume_score

    if warmup_state != "ready":
        return "insufficient_evidence"
    if baseline_confidence == "low":
        return "insufficient_evidence"
    if feed_health in ("stale", "disconnected"):
        return "insufficient_evidence"
    if side_confidence_aggregate == "low":
        return "insufficient_evidence"

    thresh = DIRECTIONAL_TAPE_PRESSURE
    if opening_price_discovery_window:
        thresh *= OPENING_TAPE_PRESSURE_MULT

    tp = tape_pressure_signed

    if feed_health == "delayed":
        return "neutral_chop"

    if abs(tp) < thresh:
        return "neutral_chop"

    if mid_ret_60s is None:
        return "neutral_chop"

    if book_pressure_signed is not None:
        if tp > 0 and book_pressure_signed < -0.55:
            return "neutral_chop"
        if tp < 0 and book_pressure_signed > 0.55:
            return "neutral_chop"

    if tp >= thresh and mid_ret_60s >= DIRECTIONAL_MID_RET:
        return "directional_push_up"
    if tp <= -thresh and mid_ret_60s <= -DIRECTIONAL_MID_RET:
        return "directional_push_down"

    return "neutral_chop"

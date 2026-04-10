"""
Phase 2 move_quality: Phase 1 labels plus vacuum, absorption, exhaustion.
Uses only explicit metrics from advanced_metrics + Phase 1 inputs.
"""

from __future__ import annotations

from typing import Literal

from app.engine.classifier_phase1 import classify_phase1_raw
from app.threshold_profile import (
    ABSORPTION_SCORE_MIN,
    BURST_STRONG,
    EXHAUSTION_SCORE_MIN,
    REGIME_AFTERHOURS_VACUUM_MULT,
    REGIME_PREMARKET_VACUUM_MULT,
    VACUUM_SCORE_MIN,
)

MoveQualityPhase2 = Literal[
    "insufficient_evidence",
    "neutral_chop",
    "directional_push_up",
    "directional_push_down",
    "vacuum_jump_up",
    "vacuum_jump_down",
    "absorption_against_buyers",
    "absorption_against_sellers",
    "exhaustion_after_up_push",
    "exhaustion_after_down_push",
]


def _vacuum_ret_threshold(session_regime: str) -> float:
    base = 0.00016
    mult = 1.0
    if session_regime == "after_hours":
        mult *= REGIME_AFTERHOURS_VACUUM_MULT
    elif session_regime == "pre_market":
        mult *= REGIME_PREMARKET_VACUUM_MULT
    elif session_regime in ("open_window", "closing_window"):
        mult *= 1.08
    return base * mult


def classify_phase2_raw(
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
    session_regime: str,
    burst_score: float,
    mid_ret_5s: float | None,
    vacuum_up_score: float,
    vacuum_down_score: float,
    absorption_against_buyers_score: float,
    absorption_against_sellers_score: float,
    exhaustion_up_score: float,
    exhaustion_down_score: float,
    burst_peak_recent: float,
) -> MoveQualityPhase2:
    p1 = classify_phase1_raw(
        warmup_state=warmup_state,
        baseline_confidence=baseline_confidence,
        feed_health=feed_health,
        side_confidence_aggregate=side_confidence_aggregate,
        tape_pressure_signed=tape_pressure_signed,
        book_pressure_signed=book_pressure_signed,
        mid_ret_60s=mid_ret_60s,
        spread=spread,
        relative_volume_score=relative_volume_score,
        opening_price_discovery_window=opening_price_discovery_window,
    )

    if warmup_state != "ready":
        return p1
    if baseline_confidence == "low":
        return p1
    if feed_health in ("stale", "disconnected"):
        return p1
    if side_confidence_aggregate == "low":
        return p1

    if feed_health == "delayed":
        return "neutral_chop"

    vac_th = _vacuum_ret_threshold(session_regime)
    if opening_price_discovery_window:
        vac_th *= 1.12

    mr5 = mid_ret_5s or 0.0
    if (
        feed_health == "live"
        and max(vacuum_up_score, vacuum_down_score) >= VACUUM_SCORE_MIN
        and abs(mr5) >= vac_th
    ):
        if mr5 > 0 and vacuum_up_score >= vacuum_down_score:
            return "vacuum_jump_up"
        if mr5 < 0 and vacuum_down_score >= vacuum_up_score:
            return "vacuum_jump_down"

    if absorption_against_buyers_score >= ABSORPTION_SCORE_MIN:
        return "absorption_against_buyers"
    if absorption_against_sellers_score >= ABSORPTION_SCORE_MIN:
        return "absorption_against_sellers"

    if (
        burst_peak_recent >= BURST_STRONG
        and burst_score < BURST_STRONG * 0.52
        and max(exhaustion_up_score, exhaustion_down_score) >= EXHAUSTION_SCORE_MIN
    ):
        if exhaustion_up_score >= exhaustion_down_score and tape_pressure_signed > 0.05:
            return "exhaustion_after_up_push"
        if exhaustion_down_score > exhaustion_up_score and tape_pressure_signed < -0.05:
            return "exhaustion_after_down_push"

    return p1

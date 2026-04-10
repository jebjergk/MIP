"""Unit tests — Phase 1 classifier (allowed inputs only)."""

from app.engine.classifier_phase1 import classify_phase1_raw


def test_insufficient_when_warmup_not_ready():
    r = classify_phase1_raw(
        warmup_state="warming",
        baseline_confidence="high",
        feed_health="live",
        side_confidence_aggregate="high",
        tape_pressure_signed=0.9,
        book_pressure_signed=0.1,
        mid_ret_60s=0.001,
        spread=0.01,
        relative_volume_score=0.8,
        opening_price_discovery_window=False,
    )
    assert r == "insufficient_evidence"


def test_directional_push_up_when_gates_pass():
    r = classify_phase1_raw(
        warmup_state="ready",
        baseline_confidence="high",
        feed_health="live",
        side_confidence_aggregate="high",
        tape_pressure_signed=0.55,
        book_pressure_signed=0.1,
        mid_ret_60s=0.0002,
        spread=0.01,
        relative_volume_score=0.5,
        opening_price_discovery_window=False,
    )
    assert r == "directional_push_up"


def test_delayed_feed_neutral_chop():
    r = classify_phase1_raw(
        warmup_state="ready",
        baseline_confidence="high",
        feed_health="delayed",
        side_confidence_aggregate="high",
        tape_pressure_signed=0.99,
        book_pressure_signed=None,
        mid_ret_60s=0.01,
        spread=0.02,
        relative_volume_score=0.9,
        opening_price_discovery_window=False,
    )
    assert r == "neutral_chop"

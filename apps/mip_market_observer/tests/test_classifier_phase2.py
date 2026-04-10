from app.engine.classifier_phase2 import classify_phase2_raw


def test_phase2_delayed_still_neutral():
    r = classify_phase2_raw(
        warmup_state="ready",
        baseline_confidence="high",
        feed_health="delayed",
        side_confidence_aggregate="high",
        tape_pressure_signed=0.9,
        book_pressure_signed=None,
        mid_ret_60s=0.01,
        spread=0.02,
        relative_volume_score=0.8,
        opening_price_discovery_window=False,
        session_regime="regular_session",
        burst_score=0.9,
        mid_ret_5s=0.002,
        vacuum_up_score=0.99,
        vacuum_down_score=0.0,
        absorption_against_buyers_score=0.0,
        absorption_against_sellers_score=0.0,
        exhaustion_up_score=0.0,
        exhaustion_down_score=0.0,
        burst_peak_recent=0.0,
    )
    assert r == "neutral_chop"


def test_vacuum_jump_when_live():
    r = classify_phase2_raw(
        warmup_state="ready",
        baseline_confidence="high",
        feed_health="live",
        side_confidence_aggregate="high",
        tape_pressure_signed=0.2,
        book_pressure_signed=0.0,
        mid_ret_60s=0.0001,
        spread=0.05,
        relative_volume_score=0.7,
        opening_price_discovery_window=False,
        session_regime="regular_session",
        burst_score=0.4,
        mid_ret_5s=0.00025,
        vacuum_up_score=0.58,
        vacuum_down_score=0.05,
        absorption_against_buyers_score=0.0,
        absorption_against_sellers_score=0.0,
        exhaustion_up_score=0.0,
        exhaustion_down_score=0.0,
        burst_peak_recent=0.0,
    )
    assert r == "vacuum_jump_up"


def test_exhaustion_after_burst_peak():
    r = classify_phase2_raw(
        warmup_state="ready",
        baseline_confidence="high",
        feed_health="live",
        side_confidence_aggregate="high",
        tape_pressure_signed=0.15,
        book_pressure_signed=0.1,
        mid_ret_60s=0.00015,
        spread=0.02,
        relative_volume_score=0.5,
        opening_price_discovery_window=False,
        session_regime="regular_session",
        burst_score=0.25,
        mid_ret_5s=0.00005,
        vacuum_up_score=0.1,
        vacuum_down_score=0.1,
        absorption_against_buyers_score=0.0,
        absorption_against_sellers_score=0.0,
        exhaustion_up_score=0.55,
        exhaustion_down_score=0.1,
        burst_peak_recent=0.68,
    )
    assert r == "exhaustion_after_up_push"

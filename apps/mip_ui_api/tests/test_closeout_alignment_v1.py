"""Deterministic ALIGN_RULE_V1 unit tests (Phase 4)."""

from app.closeout_alignment_v1 import (
    COMPARISON_RULE_VERSION,
    EPSILON_FLAT_PCT,
    classify_realized_outcome_class,
    compute_alignment_rule_v1,
    compute_position_return_pct,
    exit_type_reason_code,
    recommended_action_from_alpha_spec,
)


def test_position_return_long_short():
    assert abs(compute_position_return_pct(100.0, 101.0, "BUY") - 0.01) < 1e-9
    assert abs(compute_position_return_pct(100.0, 99.0, "BUY") - (-0.01)) < 1e-9
    assert abs(compute_position_return_pct(100.0, 99.0, "SELL") - 0.01) < 1e-9
    assert abs(compute_position_return_pct(100.0, 101.0, "SELL") - (-0.01)) < 1e-9


def test_realized_outcome_flat_epsilon():
    roc, codes = classify_realized_outcome_class(EPSILON_FLAT_PCT / 2)
    assert roc == "FLAT"
    assert "REALIZED_FLAT" in codes


def test_alignment_enter_favorable_tp():
    a = compute_alignment_rule_v1(
        recommended_action="ENTER",
        alpha_override_class="ACCEPT_ALPHA",
        realized_outcome_class="FAVORABLE",
        exit_type="TP",
        missing_entry_fill=False,
        missing_exit_fill=False,
    )
    assert a["alignment_class"] == "ALIGNED"
    assert a["realized_outcome_class"] == "FAVORABLE"
    assert a["comparison_rule_version"] == COMPARISON_RULE_VERSION
    assert "EXIT_TAKE_PROFIT" in a["alignment_reason_codes"]
    assert COMPARISON_RULE_VERSION in a["alignment_reason_codes"]


def test_alignment_skip_overridden_unfavorable():
    a = compute_alignment_rule_v1(
        recommended_action="SKIP",
        alpha_override_class="INCREASE_VS_ALPHA",
        realized_outcome_class="UNFAVORABLE",
        exit_type="SL",
        missing_entry_fill=False,
        missing_exit_fill=False,
    )
    assert a["alignment_class"] == "ADVERSE"
    assert "ENTRY_ALPHA_SKIP_OVERRIDDEN" in a["alignment_reason_codes"]
    assert "SKIP_BASELINE_TRADE_CLOSED" in a["alignment_reason_codes"]


def test_alignment_reduce_overridden_favorable():
    a = compute_alignment_rule_v1(
        recommended_action="REDUCE",
        alpha_override_class="INCREASE_VS_ALPHA",
        realized_outcome_class="FAVORABLE",
        exit_type="MANUAL",
        missing_entry_fill=False,
        missing_exit_fill=False,
    )
    assert a["alignment_class"] == "ALIGNED"
    assert "ENTRY_ALPHA_REDUCE_OVERRIDDEN" in a["alignment_reason_codes"]


def test_determinism_same_inputs():
    kwargs = dict(
        recommended_action="ENTER",
        alpha_override_class=None,
        realized_outcome_class="FLAT",
        exit_type="OTHER",
        missing_entry_fill=False,
        missing_exit_fill=False,
    )
    assert compute_alignment_rule_v1(**kwargs) == compute_alignment_rule_v1(**kwargs)


def test_missing_broker_data_neutral():
    a = compute_alignment_rule_v1(
        recommended_action="ENTER",
        alpha_override_class="ACCEPT_ALPHA",
        realized_outcome_class="FAVORABLE",
        exit_type="TP",
        missing_entry_fill=True,
        missing_exit_fill=False,
    )
    assert a["alignment_class"] == "NEUTRAL"
    assert "MISSING_ENTRY_FILL" in a["alignment_reason_codes"]


def test_recommended_action_from_alpha_stub_none():
    assert recommended_action_from_alpha_spec({"stub": True, "phase": 1}) is None


def test_recommended_action_from_alpha_v1():
    assert (
        recommended_action_from_alpha_spec(
            {"alpha_schema_version": "ALPHA_SPEC_V1", "recommended_action": "SKIP"}
        )
        == "SKIP"
    )


def test_exit_type_revalidation_code():
    assert exit_type_reason_code("REVALIDATION") == "EXIT_REVALIDATION"


def test_no_alpha_baseline_neutral():
    a = compute_alignment_rule_v1(
        recommended_action=None,
        alpha_override_class="NO_ALPHA_BASELINE",
        realized_outcome_class="FAVORABLE",
        exit_type="TP",
        missing_entry_fill=False,
        missing_exit_fill=False,
    )
    assert a["alignment_class"] == "NEUTRAL"
    assert "ALIGN_NO_ACTIONABLE_ALPHA" in a["alignment_reason_codes"]

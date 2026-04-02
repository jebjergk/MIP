"""Operator lifecycle payload builder (LIC bootstrap)."""

from app.services.live_intelligence.entry_lifecycle_ui import build_operator_entry_lifecycle


def test_unlinked_symbol():
    out = build_operator_entry_lifecycle("TEST", {})
    assert out["has_entry_intel_link"] is False


def test_minimal_linked_row():
    row = {
        "ENTRY_ACTION_ID": "act-1",
        "LINK_SNAPSHOT_ID": "snap-1",
        "COMMITTEE_RUN_ID": None,
        "PROPOSAL_ID": 1,
        "WORLDS_SPEC": '{"historical_distribution": {"sample_size": 20, "upside_probability": 0.3, "base_probability": 0.5, "downside_probability": 0.2}}',
        "ALPHA_SPEC": '{"alpha_schema_version": "ALPHA_SPEC_V1", "recommended_action": "ENTER", "confidence_band": "HIGH", "downside_risk_band": "LOW", "expected_value_net": 0.01, "recommended_size_band": "S", "alpha_summary_text": "ok"}',
        "SOURCE_VERSION": "EIS_SCHEMA_V2",
        "EIS_VERSION": 1,
        "VERDICT_JSON": None,
        "COMMITTEE_RECOMMENDATION": None,
    }
    out = build_operator_entry_lifecycle("ABC", row)
    assert out["has_entry_intel_link"] is True
    assert out["entry_action_id"] == "act-1"
    assert out["entry_analysis"]["recommended_action"] == "ENTER"
    assert out["committee_vs_baseline"]["headline"] == "No committee record"
    assert out["outcome"] is None

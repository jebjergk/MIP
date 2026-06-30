"""Execution-readiness pre-screen ranking — shadow-aligned dossier scoring."""
from __future__ import annotations

from datetime import date

from MIP.scripts.proposal_board_phase4.ranking import (
    rank_eligible_rows,
    score_candidate,
    score_execution_readiness,
)


def _structural_payload(fresh_setup_date: str) -> dict:
    return {
        "setup_events_evidence_only": [
            {
                "setup_event_id": 1001,
                "setup_date": fresh_setup_date,
                "setup_status": "ELIGIBLE",
                "setup_family": "AGENTIC_LONG",
                "event_direction": "LONG",
            }
        ],
        "primary_evidence_setup_event_id": 1001,
        "levels": {"nearest_level_distance_pct": 1.0},
        "structure": {},
        "regime": {"tags": {"trend_regime": "STRONG_TREND_DOWN"}},
        "candle_sequence": [],
        "price": {},
        "history": {
            "long_history": [
                {
                    "setup_family": "AGENTIC_LONG",
                    "meaningful_hit_rate": 0.80,
                    "path_survival_hit_rate": 0.40,
                }
            ],
        },
    }


def test_execution_readiness_favors_clean_path_over_at_resistance():
    as_of = date(2026, 6, 30)
    hostile = {
        **_structural_payload("2026-06-29"),
        "actionability_context": {
            "continuation_quality": "REJECTED",
            "confirmation_needed": True,
            "target_path_clear": False,
            "entry_location_quality": "AT_RESISTANCE",
            "resistance_overhead_risk": "HIGH",
            "active_candle_cluster": "UPPER_ZONE_REJECTION_CLUSTER",
        },
    }
    cleaner = {
        **_structural_payload("2026-06-29"),
        "actionability_context": {
            "continuation_quality": "UNCONFIRMED",
            "confirmation_needed": False,
            "target_path_clear": True,
            "entry_location_quality": "AT_SUPPORT",
            "resistance_overhead_risk": "CLEAR",
            "active_candle_cluster": "ORDERLY_PULLBACK",
        },
    }
    hostile_exec, _ = score_execution_readiness(hostile, as_of)
    cleaner_exec, _ = score_execution_readiness(cleaner, as_of)
    assert cleaner_exec > hostile_exec

    rows = [
        (1, "PFE", "STOCK", hostile),
        (2, "APA", "STOCK", cleaner),
    ]
    ranked = rank_eligible_rows(rows, as_of)
    assert ranked[0][1] == "APA"


def test_combined_score_keeps_strong_structural_when_execution_similar():
    as_of = date(2026, 6, 30)
    act = {
        "continuation_quality": "UNCONFIRMED",
        "confirmation_needed": False,
        "target_path_clear": True,
        "entry_location_quality": "AT_SUPPORT",
        "resistance_overhead_risk": "CLEAR",
    }
    strong = _structural_payload("2026-06-30")
    strong["actionability_context"] = act
    weak = {
        "setup_events_evidence_only": [],
        "actionability_context": act,
        "levels": {},
        "structure": {},
        "regime": {},
        "candle_sequence": [],
        "price": {},
        "history": {"long_history": []},
    }
    strong_score, strong_bd = score_candidate(strong, as_of, symbol="STRONG")
    weak_score, _ = score_candidate(weak, as_of, symbol="WEAK")
    assert strong_score > weak_score
    assert strong_bd.get("execution_readiness_score") is not None
    assert strong_bd.get("structural_appeal_score") is not None

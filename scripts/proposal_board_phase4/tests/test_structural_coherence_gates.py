"""Unit tests for Phase 8 structural coherence gates (no Snowflake required).

Covers:
  * detect_conflicts OPPOSING_SETUP_UNRESOLVED rule
  * ranking demotion for incoherent primary evidence and opposing setups
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from MIP.scripts.proposal_board_phase4.conflict_detection import (  # noqa: E402
    detect_conflicts,
)
from MIP.scripts.proposal_board_phase4.ranking import score_structural_appeal  # noqa: E402


def _positions(thesis_verdict: str = "LONG_THESIS") -> dict:
    return {
        "THESIS": {"verdict": thesis_verdict, "primary_reason_code": "THESIS_LONG"},
        "MARKET_STRUCTURE": {"verdict": "TREND_UP"},
        "LEVEL_PRICE_ACTION": {"verdict": "LONG_LOCATION"},
        "HISTORICAL_EVIDENCE": {"verdict": "LONG_SUPPORTIVE"},
        "RISK_EXECUTION": {"verdict": "ACTIONABLE"},
    }


def _long_thesis_with_opposing_short_evidence():
    return {
        "setup_events_evidence_only": [
            {
                "setup_event_id": 51543,
                "setup_family": "THREE_BAR_REVERSAL_SHORT",
                "event_direction": "SHORT",
                "setup_status": "ELIGIBLE",
                "structure_confidence": 0.84,
            }
        ]
    }


def test_detect_conflicts_flags_opposing_short_when_thesis_long():
    conflicts = detect_conflicts(
        _positions("LONG_THESIS"),
        _long_thesis_with_opposing_short_evidence(),
    )
    topics = [c.topic for c in conflicts]
    assert "OPPOSING_SETUP_UNRESOLVED" in topics


def test_detect_conflicts_no_opposing_flag_without_evidence():
    conflicts = detect_conflicts(_positions("LONG_THESIS"), None)
    topics = [c.topic for c in conflicts]
    assert "OPPOSING_SETUP_UNRESOLVED" not in topics


def test_detect_conflicts_ignores_low_confidence_opposing():
    evidence = _long_thesis_with_opposing_short_evidence()
    evidence["setup_events_evidence_only"][0]["structure_confidence"] = 0.30
    conflicts = detect_conflicts(_positions("LONG_THESIS"), evidence)
    topics = [c.topic for c in conflicts]
    assert "OPPOSING_SETUP_UNRESOLVED" not in topics


def _incoherent_primary_payload():
    return {
        "setup_events_evidence_only": [
            {
                "setup_event_id": 51630,
                "setup_family": "TREND_PULLBACK_LONG",
                "event_direction": "LONG",
                "setup_status": "ELIGIBLE",
                "setup_date": "2026-07-21",
                "structure_confidence": 0.80,
                "level_entry_coherent": False,
            }
        ],
        "primary_evidence_setup_event_id": 51630,
        "history": {"long_history": []},
        "levels": {},
        "structure": {},
        "regime": {},
        "candle_sequence": [],
        "price": {},
    }


def _coherent_primary_payload():
    p = _incoherent_primary_payload()
    p["setup_events_evidence_only"][0]["level_entry_coherent"] = True
    return p


def test_ranking_penalises_incoherent_primary_evidence():
    as_of = date(2026, 7, 22)
    incoherent, incoherent_bd = score_structural_appeal(
        _incoherent_primary_payload(), as_of
    )
    coherent, coherent_bd = score_structural_appeal(
        _coherent_primary_payload(), as_of
    )
    assert coherent > incoherent
    assert incoherent_bd.get("primary_setup_incoherent_penalty") == -40.0
    assert "primary_setup_incoherent_penalty" not in coherent_bd


def test_ranking_penalises_eligible_opposing_setup():
    """Two payloads with same LONG primary evidence; the one that ALSO has
    a high-confidence SHORT eligible setup pays the -15 opposing penalty."""
    as_of = date(2026, 7, 22)

    # Baseline: adds a second LONG eligible setup (same direction — no penalty).
    baseline = _coherent_primary_payload()
    baseline["setup_events_evidence_only"].append({
        "setup_event_id": 51625,
        "setup_family": "BREAKOUT_RETEST_LONG",
        "event_direction": "LONG",
        "setup_status": "ELIGIBLE",
        "setup_date": "2026-07-21",
        "structure_confidence": 0.80,
        "level_entry_coherent": True,
    })

    # Same count of fresh eligible setups, but the extra one is opposite.
    with_opposing = _coherent_primary_payload()
    with_opposing["setup_events_evidence_only"].append({
        "setup_event_id": 51543,
        "setup_family": "THREE_BAR_REVERSAL_SHORT",
        "event_direction": "SHORT",
        "setup_status": "ELIGIBLE",
        "setup_date": "2026-07-21",
        "structure_confidence": 0.84,
        "level_entry_coherent": True,
    })

    baseline_score, baseline_bd = score_structural_appeal(baseline, as_of)
    opposing_score, opposing_bd = score_structural_appeal(with_opposing, as_of)

    assert opposing_score < baseline_score
    assert opposing_bd.get("opposing_eligible_setup_penalty") == -15.0
    assert "opposing_eligible_setup_penalty" not in baseline_bd

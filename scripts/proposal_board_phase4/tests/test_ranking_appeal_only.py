"""Appeal-only pre-screen ranking — no rotation penalty for recently reviewed symbols."""
from __future__ import annotations

from datetime import date

from MIP.scripts.proposal_board_phase4.ranking import rank_eligible_rows, score_candidate


def _payload(fresh_setup_date: str, setup_status: str = "ELIGIBLE") -> dict:
    return {
        "setup_events_evidence_only": [
            {
                "setup_date": fresh_setup_date,
                "setup_status": setup_status,
                "setup_family": "BREAKOUT",
            }
        ],
        "levels": {"nearest_level_distance_pct": 1.5},
        "structure": {},
        "regime": {},
        "candle_sequence": [],
        "price": {},
        "history": {"has_any": True},
    }


def test_same_appeal_score_ranks_alphabetically_by_symbol():
    as_of = date(2026, 6, 29)
    rows = [
        (2, "ZZZZ", "STOCK", _payload("2026-06-28")),
        (1, "AAAA", "STOCK", _payload("2026-06-28")),
    ]
    ranked = rank_eligible_rows(rows, as_of)
    assert [sym for _, sym, _, _, _, _ in ranked] == ["AAAA", "ZZZZ"]


def test_higher_appeal_wins_regardless_of_review_order():
    as_of = date(2026, 6, 29)
    strong = _payload("2026-06-29")
    weak = {
        "setup_events_evidence_only": [],
        "levels": {},
        "structure": {},
        "regime": {},
        "candle_sequence": [],
        "price": {},
        "history": {"has_any": True},
    }
    strong_score, _ = score_candidate(strong, as_of, symbol="STRONG")
    weak_score, _ = score_candidate(weak, as_of, symbol="WEAK")
    assert strong_score > weak_score

    rows = [
        (10, "WEAK", "STOCK", weak),
        (11, "STRONG", "STOCK", strong),
    ]
    ranked = rank_eligible_rows(rows, as_of)
    assert ranked[0][1] == "STRONG"


def test_board_eligible_setup_ranks_above_no_active_setup():
    as_of = date(2026, 6, 29)
    with_setup = {
        "setup_events_evidence_only": [
            {
                "setup_date": "2026-06-28",
                "setup_status": "ELIGIBLE",
                "setup_family": "TREND_PULLBACK_LONG",
            }
        ],
        "primary_evidence_setup_event_id": 12345,
        "history": {
            "long_history": [
                {"setup_family": "TREND_PULLBACK_LONG", "trust_label": "PROVISIONAL"}
            ],
        },
        "levels": {},
        "structure": {},
        "regime": {},
        "candle_sequence": [],
        "price": {},
    }
    no_setup = {
        "setup_events_evidence_only": [],
        "history": {"long_history": []},
        "levels": {},
        "structure": {},
        "regime": {},
        "candle_sequence": [],
        "price": {},
    }
    rows = [
        (1, "NVDA", "STOCK", no_setup),
        (2, "NEE", "STOCK", with_setup),
    ]
    ranked = rank_eligible_rows(rows, as_of)
    assert ranked[0][1] == "NEE"

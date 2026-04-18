"""Unit tests for LPA Committee 2.0 orchestration helpers (no Snowflake)."""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.routers.live import (  # noqa: E402
    _build_inline_hearing_payload,
    _dedupe_structural_entry_pending_rows,
    _is_structural_entry_pending_row,
    _structural_proposal_status_is_actionable,
)
from app.routers.committee import (  # noqa: E402
    committee_final_decision_commit_for_action,
    HearingCommitRequest,
)
from fastapi import HTTPException  # noqa: E402


class TestStructuralProposalActionable(unittest.TestCase):
    def test_terminal_statuses_not_actionable(self):
        self.assertFalse(_structural_proposal_status_is_actionable("EXECUTED"))
        self.assertFalse(_structural_proposal_status_is_actionable("REJECTED"))
        self.assertFalse(_structural_proposal_status_is_actionable("CANCELLED"))
        self.assertFalse(_structural_proposal_status_is_actionable("EXPIRED"))

    def test_proposed_is_actionable(self):
        self.assertTrue(_structural_proposal_status_is_actionable("PROPOSED"))
        self.assertTrue(_structural_proposal_status_is_actionable(None))


class TestStructuralEntryPendingRow(unittest.TestCase):
    def test_entry_vs_exit(self):
        self.assertTrue(
            _is_structural_entry_pending_row(
                {"live_intent_kind": "STRUCTURAL", "action_intent": "ENTRY"},
            ),
        )
        self.assertFalse(
            _is_structural_entry_pending_row(
                {"live_intent_kind": "STRUCTURAL", "action_intent": "EXIT"},
            ),
        )
        self.assertFalse(
            _is_structural_entry_pending_row(
                {"live_intent_kind": "ALPHA", "action_intent": "ENTRY"},
            ),
        )


class TestDedupeStructuralEntryPending(unittest.TestCase):
    def test_picks_max_actionable_proposal_per_symbol(self):
        cur = MagicMock()
        st_rows = [
            {"PROPOSAL_ID": 10, "STATUS": "PROPOSED"},
            {"PROPOSAL_ID": 20, "STATUS": "PROPOSED"},
        ]

        def fake_fetch_all(c):
            return st_rows

        pending = [
            {
                "action_id": "a10",
                "symbol": "XYZ",
                "live_intent_kind": "STRUCTURAL",
                "action_intent": "ENTRY",
                "proposal_id": 10,
                "status": "OPEN_ELIGIBLE",
            },
            {
                "action_id": "a20",
                "symbol": "XYZ",
                "live_intent_kind": "STRUCTURAL",
                "action_intent": "ENTRY",
                "proposal_id": 20,
                "status": "OPEN_ELIGIBLE",
            },
            {
                "action_id": "leg",
                "symbol": "QQQ",
                "live_intent_kind": "STRUCTURAL",
                "action_intent": "EXIT",
                "proposal_id": 5,
                "status": "OPEN_ELIGIBLE",
            },
        ]
        with patch("app.routers.live.fetch_all", side_effect=fake_fetch_all):
            out = _dedupe_structural_entry_pending_rows(cur, pending)
        by_action = {str(r.get("action_id")): r for r in out}
        self.assertIn("a20", by_action)
        self.assertNotIn("a10", by_action)
        self.assertIn("leg", by_action)
        canon = by_action["a20"]
        self.assertEqual(len(canon.get("superseded_pending") or []), 1)
        self.assertEqual(canon["superseded_pending"][0]["action_id"], "a10")

    def test_falls_back_when_only_terminal_proposals(self):
        cur = MagicMock()
        st_rows = [
            {"PROPOSAL_ID": 1, "STATUS": "EXECUTED"},
            {"PROPOSAL_ID": 2, "STATUS": "EXECUTED"},
        ]

        pending = [
            {
                "action_id": "x1",
                "symbol": "ABC",
                "live_intent_kind": "STRUCTURAL",
                "action_intent": "ENTRY",
                "proposal_id": 1,
                "status": "X",
            },
            {
                "action_id": "x2",
                "symbol": "ABC",
                "live_intent_kind": "STRUCTURAL",
                "action_intent": "ENTRY",
                "proposal_id": 2,
                "status": "X",
            },
        ]
        with patch("app.routers.live.fetch_all", return_value=st_rows):
            out = _dedupe_structural_entry_pending_rows(cur, pending)
        pids = {r.get("proposal_id") for r in out if r.get("action_id", "").startswith("x")}
        self.assertEqual(pids, {2})


class TestInlineHearingPayload(unittest.TestCase):
    def test_build_inline_hearing_includes_exhibits(self):
        refresh = {
            "stance": "APPROVE",
            "confidence": 0.71,
            "hearing_ts": "2026-04-10T12:00:00",
            "updated_at": "2026-04-10T12:01:00",
            "proposal": {"symbol": "ABC", "direction": "LONG", "setup_family": "SF1"},
            "snapshot_panel": {
                "TRUST_LABEL": "TRUSTED",
                "REGIME_STATE": "GOOD",
                "STRUCTURAL_STATE": "TREND_UP",
                "ENTRY_ZONE_JSON": {"low": 10, "high": 11},
                "INVALIDATION_JSON": {"level": 9, "rule": "BELOW"},
            },
            "hearing_evidence": {
                "latest_price": 10.5,
                "zone_distance_pct": 4.76,
                "trend_regime_now": "UPTREND",
                "vol_regime_now": "NORMAL",
                "structural_state_now": "TREND_UP",
                "invalidation_level": 9,
                "invalidation_breached": False,
                "invalidation_cushion_pct": 14.29,
                "regime_continuity": "ALIGNED",
                "regime_continuity_detail": "ok",
                "path_quality_interpretation": "Test path line.",
                "recent_bar_dates": ["2026-04-10"],
                "recent_bar_trace": [{"bar_date": "2026-04-09", "close": 10.2}],
            },
            "operational": {"posture": {"trail_posture": "STANDARD", "size_posture": "FULL", "path_quality": "OK"}},
            "chair": {
                "stance": "APPROVE",
                "confidence": 0.71,
                "top_supports": ["s1"],
                "top_tensions": ["t1"],
                "execution_shaping": {"stance": "APPROVE"},
                "what_changed_since_proposal": ["line a", "line b"],
            },
            "roles": [{"role_name": "REGIME", "output": {"stance_badge": "OK", "one_liner": "x"}}],
            "artifacts": [
                {
                    "artifact_kind": "PATH_STRIP",
                    "payload": {
                        "pct_adverse": 0.3,
                        "mhr": 0.5,
                        "label": "OK",
                        "interpretation": "interp",
                    },
                },
                {
                    "artifact_kind": "SYMBOL_FINGERPRINT",
                    "payload": {
                        "one_liner": "fp",
                        "bullets": ["b1"],
                        "badge": "MIXED",
                        "trust_label": "TRUSTED",
                    },
                },
            ],
            "exhibit_public_disclosure_context": {
                "schema_version": "1",
                "symbol": "ABC",
                "market_type": "STOCK",
                "mapping_quality": "MAPPED",
                "tone_vs_trade": "NEUTRAL",
                "summary_lines": ["1 mapped public disclosure row(s) for ABC."],
                "recent_transactions": [],
                "disclaimer": "Public disclosure only; not a trade signal.",
            },
        }
        proposal = {"SYMBOL": "ABC", "DIRECTION": "LONG", "SETUP_FAMILY": "SF1", "TRUST_LABEL": "TRUSTED"}
        out = _build_inline_hearing_payload(
            action_id="act-1",
            proposal_id=99,
            hearing_id="hid",
            proposal=proposal,
            refresh_payload=refresh,
        )
        self.assertEqual(out["exhibit_geometry_hero"]["symbol"], "ABC")
        self.assertEqual(out["exhibit_path_quality"]["pct_adverse_before_favorable"], 0.3)
        self.assertEqual(out["exhibit_regime_continuity"]["continuity_verdict"], "ALIGNED")
        self.assertEqual(out["exhibit_protection"]["cushion_pct"], 14.29)
        self.assertEqual(out["what_changed_strip"][0], "line a")
        self.assertTrue(len(out["chair_board"]["top_supports"]) >= 1)
        self.assertEqual(out["exhibit_public_disclosure_context"]["symbol"], "ABC")

    def test_build_inline_hearing_passes_live_disclosure_exhibit(self):
        refresh = {
            "stance": "APPROVE",
            "confidence": 0.5,
            "hearing_ts": "2026-04-10T12:00:00",
            "updated_at": "2026-04-10T12:01:00",
            "proposal": {"symbol": "ABC", "direction": "LONG"},
            "snapshot_panel": {"ENTRY_ZONE_JSON": {"low": 1, "high": 2}, "INVALIDATION_JSON": {"rule": "R"}},
            "hearing_evidence": {"recent_bar_dates": []},
            "operational": {"posture": {}},
            "chair": {
                "stance": "APPROVE",
                "confidence": 0.5,
                "top_supports": [],
                "top_tensions": [],
                "execution_shaping": {},
                "what_changed_since_proposal": [],
            },
            "roles": [],
            "artifacts": [],
            "exhibit_public_disclosure_context": {"schema_version": "1", "symbol": "ABC"},
            "exhibit_live_politician_disclosure_context": {
                "schema_version": "1",
                "symbol": "ABC",
                "summary_lines": ["Live line"],
                "source_label": "S",
                "fetched_at_utc": "2026-04-18T12:00:00Z",
            },
        }
        out = _build_inline_hearing_payload(
            action_id="a",
            proposal_id=1,
            hearing_id="h",
            proposal={"SYMBOL": "ABC", "DIRECTION": "LONG"},
            refresh_payload=refresh,
        )
        self.assertEqual(out["exhibit_live_politician_disclosure_context"]["summary_lines"], ["Live line"])

    def test_build_inline_hearing_omits_disclosure_exhibit_when_not_in_refresh(self):
        refresh = {
            "stance": "APPROVE",
            "confidence": 0.5,
            "hearing_ts": "2026-04-10T12:00:00",
            "updated_at": "2026-04-10T12:01:00",
            "proposal": {"symbol": "ZZZ", "direction": "LONG"},
            "snapshot_panel": {
                "ENTRY_ZONE_JSON": {"low": 1, "high": 2},
                "INVALIDATION_JSON": {"rule": "R"},
            },
            "hearing_evidence": {"recent_bar_dates": []},
            "operational": {"posture": {}},
            "chair": {
                "stance": "APPROVE",
                "confidence": 0.5,
                "top_supports": [],
                "top_tensions": [],
                "execution_shaping": {},
                "what_changed_since_proposal": [],
            },
            "roles": [],
            "artifacts": [],
        }
        out = _build_inline_hearing_payload(
            action_id="a",
            proposal_id=1,
            hearing_id="h",
            proposal={"SYMBOL": "ZZZ", "DIRECTION": "LONG"},
            refresh_payload=refresh,
        )
        self.assertNotIn("exhibit_public_disclosure_context", out)


class TestCommitteeFinalDecisionCommitForAction(unittest.TestCase):
    def test_conflict_when_bound_to_other_action(self):
        cur = MagicMock()
        existing = {
            "HEARING_ID": "h1",
            "ACTION_ID": "other-action",
            "PROPOSAL_ID": 1,
        }
        with patch("app.routers.committee.fetch_all", return_value=[existing]):
            with self.assertRaises(HTTPException) as ctx:
                committee_final_decision_commit_for_action(
                    cur,
                    "h1",
                    HearingCommitRequest(action_id="my-action"),
                )
        self.assertEqual(ctx.exception.status_code, 409)

    def test_idempotent_same_action(self):
        cur = MagicMock()
        existing = {
            "HEARING_ID": "h1",
            "ACTION_ID": "same-id",
            "PROPOSAL_ID": 1,
        }
        with patch("app.routers.committee.fetch_all", return_value=[existing]):
            with patch("app.routers.committee.serialize_row", return_value={"ACTION_ID": "same-id"}):
                out = committee_final_decision_commit_for_action(
                    cur,
                    "h1",
                    HearingCommitRequest(action_id="same-id"),
                )
        self.assertTrue(out.get("already_committed"))
        self.assertTrue(out.get("ok"))


if __name__ == "__main__":
    unittest.main()

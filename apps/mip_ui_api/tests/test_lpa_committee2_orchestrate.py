"""Unit tests for LPA Committee 2.0 orchestration helpers (no Snowflake)."""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.routers.live import (  # noqa: E402
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

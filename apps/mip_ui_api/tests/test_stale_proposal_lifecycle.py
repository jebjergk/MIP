"""LPA stale-proposal lifecycle — overview + submit/execute gate tests.

Covers:
  * `_dedupe_structural_entry_pending_rows` keeps only proposal_freshness
    == 'CURRENT' structural ENTRY rows and never promotes a stale row
    to canonical.
  * `_compute_action_proposal_freshness` resolves the four labels from
    the lineage join (CURRENT, SUPERSEDED_BY_NEWER_RUN, EXPIRED,
    NO_PROPOSAL_LINK).
  * `_assert_proposal_freshness_for_structural_entry` raises HTTP 409
    with `PROPOSAL_EXPIRED_OR_SUPERSEDED` for non-CURRENT structural
    ENTRY actions and is a no-op for EXIT / non-structural rows.
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import HTTPException

from app.routers.live import (
    _assert_proposal_freshness_for_structural_entry,
    _compute_action_proposal_freshness,
    _dedupe_structural_entry_pending_rows,
)


def _se_entry(symbol: str, proposal_id: int, freshness: str, status: str = "REVALIDATED_PASS") -> dict:
    return {
        "live_intent_kind": "STRUCTURAL",
        "action_intent": "ENTRY",
        "symbol": symbol,
        "proposal_id": proposal_id,
        "proposal_freshness": freshness,
        "status": status,
        "action_id": f"action-{symbol}-{proposal_id}",
    }


class TestDedupeStructuralEntryPendingRows(unittest.TestCase):
    def test_drops_non_current_structural_entry_rows(self):
        """A SUPERSEDED_BY_NEWER_RUN / EXPIRED row must never appear in
        the deduped output. The overview build-loop hides them upstream,
        and this function provides defense in depth."""
        rows = [
            _se_entry("DOW", 101, "EXPIRED"),
            _se_entry("PG",  102, "SUPERSEDED_BY_NEWER_RUN"),
        ]
        cur = MagicMock()
        out = _dedupe_structural_entry_pending_rows(cur, rows)
        self.assertEqual(out, [])

    def test_keeps_current_row_and_demotes_same_symbol_siblings(self):
        """When multiple CURRENT siblings exist for the same symbol (Phase
        5D union semantics), highest PROPOSAL_ID wins and the rest are
        demoted into superseded_pending for operator visibility."""
        rows = [
            _se_entry("AAPL", 200, "CURRENT"),
            _se_entry("AAPL", 205, "CURRENT"),
        ]
        cur = MagicMock()
        out = _dedupe_structural_entry_pending_rows(cur, rows)
        self.assertEqual(len(out), 1)
        canonical = out[0]
        self.assertEqual(canonical["proposal_id"], 205)
        siblings = canonical.get("superseded_pending") or []
        self.assertEqual(len(siblings), 1)
        self.assertEqual(siblings[0]["proposal_id"], 200)

    def test_mixed_input_keeps_current_only(self):
        """Mixed input: a CURRENT row for one symbol and a stale row for
        another. The stale row must be dropped entirely; the CURRENT row
        survives untouched."""
        rows = [
            _se_entry("NVDA", 300, "CURRENT"),
            _se_entry("DOW",  301, "EXPIRED"),
        ]
        cur = MagicMock()
        out = _dedupe_structural_entry_pending_rows(cur, rows)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["symbol"], "NVDA")
        self.assertEqual(out[0]["proposal_id"], 300)

    def test_non_structural_rows_passed_through(self):
        non_structural = {
            "live_intent_kind": "AGENTIC",
            "action_intent": "ENTRY",
            "symbol": "MSFT",
            "proposal_id": 400,
            "proposal_freshness": "EXPIRED",
            "status": "PROPOSED",
            "action_id": "agentic-1",
        }
        cur = MagicMock()
        out = _dedupe_structural_entry_pending_rows(cur, [non_structural])
        self.assertEqual(out, [non_structural])

    def test_exit_rows_passed_through(self):
        exit_row = {
            "live_intent_kind": "STRUCTURAL",
            "action_intent": "EXIT",
            "symbol": "JD",
            "proposal_id": 500,
            "proposal_freshness": "EXPIRED",
            "status": "PROPOSED",
            "action_id": "exit-1",
        }
        cur = MagicMock()
        out = _dedupe_structural_entry_pending_rows(cur, [exit_row])
        self.assertEqual(out, [exit_row])


class TestComputeActionProposalFreshness(unittest.TestCase):
    def test_no_proposal_link(self):
        cur = MagicMock()
        self.assertEqual(_compute_action_proposal_freshness(cur, None), "NO_PROPOSAL_LINK")
        cur.execute.assert_not_called()

    def test_expired_when_row_missing(self):
        cur = MagicMock()
        cur.fetchone.return_value = None
        self.assertEqual(_compute_action_proposal_freshness(cur, 1234), "EXPIRED")

    def test_expired_when_status_not_proposed(self):
        cur = MagicMock()
        cur.fetchone.return_value = ("EXPIRED", "run-A")
        self.assertEqual(_compute_action_proposal_freshness(cur, 1234), "EXPIRED")

    def test_superseded_when_no_matching_auth_run(self):
        cur = MagicMock()
        cur.fetchone.return_value = ("PROPOSED", None)
        self.assertEqual(
            _compute_action_proposal_freshness(cur, 1234),
            "SUPERSEDED_BY_NEWER_RUN",
        )

    def test_current_when_proposed_and_auth_run_matches(self):
        cur = MagicMock()
        cur.fetchone.return_value = ("PROPOSED", "run-auth-1")
        self.assertEqual(_compute_action_proposal_freshness(cur, 1234), "CURRENT")


class TestAssertProposalFreshnessForStructuralEntry(unittest.TestCase):
    def _action(self, **overrides):
        base = {
            "ACTION_ID": "a-1",
            "LIVE_INTENT_KIND": "STRUCTURAL",
            "SIDE": "BUY",
            "ACTION_INTENT": "ENTRY",
            "PROPOSAL_ID": 999,
            "SETUP_EVENT_ID": "setup-1",
        }
        base.update(overrides)
        return base

    @patch("app.routers.live._compute_action_proposal_freshness", return_value="EXPIRED")
    @patch("app.routers.live.get_connection")
    def test_raises_409_for_stale_structural_entry(self, mock_conn, _mock_fresh):
        mock_conn.return_value = MagicMock()
        with self.assertRaises(HTTPException) as exc_ctx:
            _assert_proposal_freshness_for_structural_entry(self._action())
        exc = exc_ctx.exception
        self.assertEqual(exc.status_code, 409)
        self.assertIn("PROPOSAL_EXPIRED_OR_SUPERSEDED", exc.detail["reason_codes"])
        self.assertEqual(exc.detail["proposal_freshness"], "EXPIRED")

    @patch("app.routers.live._compute_action_proposal_freshness", return_value="CURRENT")
    @patch("app.routers.live.get_connection")
    def test_noop_for_current_structural_entry(self, mock_conn, _mock_fresh):
        mock_conn.return_value = MagicMock()
        _assert_proposal_freshness_for_structural_entry(self._action())

    @patch("app.routers.live._compute_action_proposal_freshness", return_value="EXPIRED")
    @patch("app.routers.live.get_connection")
    def test_noop_for_exit_action_even_if_stale(self, mock_conn, mock_fresh):
        """EXIT actions are never gated by proposal freshness — closing a
        live position must remain available regardless of what happened
        to the originating proposal."""
        mock_conn.return_value = MagicMock()
        _assert_proposal_freshness_for_structural_entry(
            self._action(ACTION_INTENT="EXIT")
        )
        mock_fresh.assert_not_called()

    @patch("app.routers.live._compute_action_proposal_freshness", return_value="EXPIRED")
    @patch("app.routers.live.get_connection")
    def test_noop_for_non_structural_action(self, mock_conn, mock_fresh):
        mock_conn.return_value = MagicMock()
        _assert_proposal_freshness_for_structural_entry(
            self._action(LIVE_INTENT_KIND="AGENTIC", SETUP_EVENT_ID=None)
        )
        mock_fresh.assert_not_called()

    def test_noop_on_none_action(self):
        _assert_proposal_freshness_for_structural_entry(None)


if __name__ == "__main__":
    unittest.main()

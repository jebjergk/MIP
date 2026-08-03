"""Phase 9A — UI orchestration tests (no terminal, no IB orders)."""

from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from app.brooks_intraday.experiment_phase9_constants import (
    ACTION_RUN_NEXT,
    COMPLETED_WEEK_RUN_ID,
    COMPLETED_WEEK_START,
    COHORT_WEEKS,
    LEASE_SEC,
    OVERALL_WAITING_TWS,
)
from app.brooks_intraday.experiment_phase9_engine import _next_pending_week
from app.brooks_intraday.experiment_phase9_tws import classify_tws_preflight


class Phase9AWeekSelectionTests(unittest.TestCase):
    def test_run_next_selects_2026_06_22_after_completed_july_week(self):
        progress = {
            "weeks": {
                COMPLETED_WEEK_START: {"status": "WEEK_COMPLETE", "run_id": COMPLETED_WEEK_RUN_ID},
            }
        }
        nxt = _next_pending_week(progress)
        self.assertEqual(nxt, date(2026, 6, 22))

    def test_cohort_has_three_weeks(self):
        self.assertEqual(len(COHORT_WEEKS), 3)


class Phase9ATwsTests(unittest.TestCase):
    def test_tws_unavailable(self):
        self.assertEqual(
            classify_tws_preflight({"connected": False, "ib_messages": ["Couldn't connect"]}),
            "TWS_NOT_RUNNING",
        )

    def test_tws_ready(self):
        self.assertEqual(
            classify_tws_preflight(
                {"connected": True, "historical_data_capability": "contract_qualified"}
            ),
            "CONNECTED_AND_READY",
        )


class Phase9AWorkerLeaseTests(unittest.TestCase):
    @patch("app.brooks_intraday.experiment_execution_repository.get_connection")
    def test_duplicate_worker_claim_fails(self, mock_conn):
        from app.brooks_intraday.experiment_execution_repository import try_claim_lease

        cur = MagicMock()
        cur.rowcount = 0
        conn = mock_conn.return_value
        conn.cursor.return_value = cur
        conn.close = MagicMock()
        self.assertFalse(try_claim_lease("exec-1", "worker-a", LEASE_SEC))


class Phase9AQueueTests(unittest.TestCase):
    @patch("app.brooks_intraday.experiment_phase9_service.update_execution")
    @patch("app.brooks_intraday.experiment_phase9_service.append_event")
    @patch("app.brooks_intraday.experiment_phase9_service.ensure_execution_record")
    @patch("app.brooks_intraday.experiment_phase9_service._next_pending_week")
    def test_run_next_queues_running(self, mock_pending, mock_ensure, _ae, _ue):
        from app.brooks_intraday.experiment_phase9_service import request_run_next

        mock_ensure.return_value = {"execution_id": "e1", "overall_status": "READY", "progress_json": {}}
        mock_pending.return_value = date(2026, 6, 22)
        out = request_run_next()
        self.assertTrue(out.get("ok"))


class Phase9AWorkerIsolationTests(unittest.TestCase):
    def test_engine_has_no_order_paths(self):
        import app.brooks_intraday.experiment_phase9_engine as e

        src = open(e.__file__, encoding="utf-8").read()
        self.assertNotIn("placeOrder", src)
        self.assertNotIn("app.routers.live", src)


class Phase9AStatusTests(unittest.TestCase):
    def test_waiting_tws_constant(self):
        self.assertEqual(OVERALL_WAITING_TWS, "WAITING_FOR_TWS")


if __name__ == "__main__":
    unittest.main()

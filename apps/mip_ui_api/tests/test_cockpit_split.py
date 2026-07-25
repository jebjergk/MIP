"""
Cockpit split tests — 7 scenarios validating the separation of
DAILY_MARKET_UPDATE from AGENTIC_OPPORTUNITY_SEARCH.

Tests cover:
  1. Phase 4 not invoked when run_proposal_board=False (default).
  2. Staleness gate blocks agentic search when pipeline is stale.
  3. Pre-board safety gates enforced by run_board.py are untouched.
  4. FX/ETF excluded from agentic search (STOCK-only enforcement).
  5. COCKPIT_RUN_LOG idempotency — two runs get different run IDs.
  6. Daily market update success writes DAILY_MARKET_UPDATE run_type.
  7. No auto-broker order when agentic search succeeds.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.routers import management


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_proc(returncode=0, stdout="", stderr=""):
    proc = MagicMock()
    proc.returncode = returncode
    proc.stdout = stdout
    proc.stderr = stderr
    return proc


def _ib_job_success_payload():
    return json.dumps({"status": "SUCCESS", "processed_days": 1})


def _pipeline_success_payload():
    return json.dumps([{"status": "SUCCESS", "effective_to_ts": "2026-06-18T23:59:59"}])


def _staleness_row(pipeline_lag=0, bar_lag=0):
    """Mimic a healthy staleness check result row."""
    return {
        "EXPECTED_DATE": "2026-06-18",
        "PIPELINE_DATE": "2026-06-18",
        "BAR_DATE": "2026-06-18",
        "PIPELINE_LAG_DAYS": pipeline_lag,
        "BAR_LAG_DAYS": bar_lag,
    }


def _board_success_payload():
    return json.dumps({
        "status": "COMPLETE",
        "proposals_published": 2,
        "candidates_evaluated": 5,
    })


# ── Test 1: Phase 4 NOT invoked with run_proposal_board=False ─────────────────

class TestPhase4NotInvokedByDefault(unittest.TestCase):
    """
    POST /manage/ib/daily-job/run with run_proposal_board=False (the new default)
    must not call run_board.py at all.
    """

    @patch("app.routers.management._cockpit_run_complete")
    @patch("app.routers.management._cockpit_run_start")
    @patch("app.routers.management.resolve_subprocess_python")
    @patch("app.routers.management.mip_workspace_root")
    @patch("subprocess.run")
    def test_no_board_call_when_flag_is_false(
        self,
        mock_subproc,
        mock_root,
        mock_py,
        mock_start,
        mock_complete,
    ):
        root = Path("/fake/root")
        mock_root.return_value = root
        py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
        mock_py.return_value = py

        runner = root / "cursorfiles" / "run_ib_manual_daily_job.py"
        runner.parent.mkdir(parents=True, exist_ok=True)
        runner.touch()
        py.parent.mkdir(parents=True, exist_ok=True)
        py.touch()

        ib_proc = _make_proc(stdout=_ib_job_success_payload())
        pipeline_proc = _make_proc(stdout=_pipeline_success_payload())
        mock_subproc.side_effect = [ib_proc, pipeline_proc]

        management.run_ib_manual_daily_job(
            target_date="'2026-06-18'",
            dry_run=False,
            skip_ingest=False,
            run_pipeline=True,
            run_proposal_board=False,  # explicit False — the new default
            proposal_board_portfolio=1,
            proposal_board_max_proposals=8,
            proposal_board_max_rounds=1,
            proposal_board_max_candidates=5,
            proposal_board_inter_concurrency=2,
            proposal_board_market_types="STOCK",
            import_proposals_to_lpa=False,
            synth_intraday_daily=False,
        )

        # Exactly 2 subprocess.run calls: IB job + pipeline. No board call.
        self.assertEqual(mock_subproc.call_count, 2)
        for c in mock_subproc.call_args_list:
            cmd = c[0][0]
            self.assertNotIn("run_board", str(cmd))

        # COCKPIT_RUN_LOG must record DAILY_MARKET_UPDATE
        mock_start.assert_called_once()
        start_args = mock_start.call_args[0]
        self.assertEqual(start_args[1], "DAILY_MARKET_UPDATE")

        mock_complete.assert_called_once()
        complete_args = mock_complete.call_args[0]
        self.assertEqual(complete_args[1], "SUCCESS")


# ── Test 2: Staleness gate blocks agentic search when stale ──────────────────

class TestStalenessGateBlocksAgenticSearch(unittest.TestCase):
    """
    POST /manage/proposal-board/run must raise 409 if pipeline/bar lag exceeds
    staleness_max_trading_days_lag.
    """

    @patch("app.routers.management._cockpit_run_complete")
    @patch("app.routers.management._cockpit_run_start")
    @patch("app.routers.management.fetch_all")
    @patch("app.routers.management.get_connection")
    def test_stale_pipeline_raises_409(self, mock_conn, mock_fetch, mock_start, mock_complete):
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value = MagicMock()
        mock_conn.return_value.close = MagicMock()

        # Pipeline is 3 days stale (lag > default max of 1)
        mock_fetch.return_value = [_staleness_row(pipeline_lag=3, bar_lag=3)]

        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as ctx:
            management.run_proposal_board(
                portfolio=1,
                max_proposals=8,
                max_rounds=1,
                max_candidates=5,
                inter_concurrency=2,
                market_types="STOCK",
                import_proposals_to_lpa=False,
                staleness_max_trading_days_lag=1,
            )

        self.assertEqual(ctx.exception.status_code, 409)
        detail = ctx.exception.detail
        self.assertIn("stale", json.dumps(detail).lower())

        # COCKPIT_RUN_LOG must record FAILED
        mock_complete.assert_called_once()
        self.assertEqual(mock_complete.call_args[0][1], "FAILED")

    @patch("app.routers.management._import_proposals_to_active_lpa_portfolios")
    @patch("app.routers.management._cockpit_run_complete")
    @patch("app.routers.management._cockpit_run_start")
    @patch("app.routers.management.fetch_all")
    @patch("app.routers.management.get_connection")
    @patch("app.routers.management.mip_workspace_root")
    @patch("app.routers.management.resolve_subprocess_python")
    @patch("subprocess.run")
    def test_board_subprocess_failure_recovers_when_proposals_published(
        self, mock_subproc, mock_py, mock_root, mock_conn, mock_fetch, mock_start, mock_complete, mock_import
    ):
        """Subprocess exit != 0 must still import when CHAIR_DONE heal finds published rows."""
        mock_root.return_value = Path("/fake/root")
        mock_py.return_value = Path("/fake/python")
        mock_conn.return_value.cursor.return_value = MagicMock()
        mock_conn.return_value.close = MagicMock()
        mock_conn.return_value.commit = MagicMock()
        mock_fetch.return_value = [_staleness_row(pipeline_lag=0, bar_lag=0)]
        mock_subproc.return_value = _make_proc(returncode=1, stdout='{"status":"FAILED"}')
        mock_import.return_value = {
            "healed_chair_done_runs": 1,
            "active_portfolios": [1, 2],
            "per_portfolio_results": {"1": {"imported_count": 1}, "2": {"imported_count": 1}},
            "total_imported": 2,
            "skip_summary": {"imported_count": 2},
        }

        result = management.run_proposal_board(
            portfolio=1,
            max_proposals=8,
            max_rounds=1,
            max_candidates=5,
            inter_concurrency=2,
            market_types="STOCK",
            import_proposals_to_lpa=True,
            staleness_max_trading_days_lag=1,
        )

        self.assertEqual(result.get("status"), "PARTIAL_SUCCESS")
        self.assertTrue(result.get("board_recovery"))
        self.assertEqual(result.get("lpa_import_total_imported"), 2)
        mock_import.assert_called_once()

    @patch("app.routers.management._cockpit_run_complete")
    @patch("app.routers.management._cockpit_run_start")
    @patch("app.routers.management.fetch_all")
    @patch("app.routers.management.get_connection")
    @patch("app.routers.management.mip_workspace_root")
    @patch("app.routers.management.resolve_subprocess_python")
    @patch("subprocess.run")
    def test_weekend_friday_data_passes_gate(
        self, mock_subproc, mock_py, mock_root, mock_conn, mock_fetch, mock_start, mock_complete
    ):
        """Friday pipeline/bar with lag 0 vs last session must pass on Sat/Sun/Mon pre-open.

        Staleness is measured against EXPECTED_DATE (last trading day), not calendar
        today — otherwise Fri data reads as 2 calendar days stale on Sunday.
        """
        mock_root.return_value = Path("/fake/root")
        mock_py.return_value = Path("/fake/python")
        mock_conn.return_value.cursor.return_value = MagicMock()
        mock_conn.return_value.close = MagicMock()
        mock_fetch.return_value = [
            {
                "EXPECTED_DATE": "2026-07-17",
                "PIPELINE_DATE": "2026-07-17",
                "BAR_DATE": "2026-07-17",
                "PIPELINE_LAG_DAYS": 0,
                "BAR_LAG_DAYS": 0,
            }
        ]
        mock_subproc.return_value = _make_proc(returncode=0, stdout=_board_success_payload())

        result = management.run_proposal_board(
            portfolio=1,
            max_proposals=8,
            max_rounds=1,
            max_candidates=5,
            inter_concurrency=2,
            market_types="STOCK",
            import_proposals_to_lpa=False,
            staleness_max_trading_days_lag=1,
        )

        self.assertEqual(result.get("status"), "SUCCESS")

    @patch("app.routers.management._cockpit_run_complete")
    @patch("app.routers.management._cockpit_run_start")
    @patch("app.routers.management.fetch_all")
    @patch("app.routers.management.get_connection")
    @patch("app.routers.management.mip_workspace_root")
    @patch("app.routers.management.resolve_subprocess_python")
    @patch("subprocess.run")
    def test_fresh_pipeline_passes_gate(
        self, mock_subproc, mock_py, mock_root, mock_conn, mock_fetch, mock_start, mock_complete
    ):
        """Lag of 0 must NOT raise 409."""
        mock_root.return_value = Path("/fake/root")
        mock_py.return_value = Path("/fake/python")
        mock_conn.return_value.cursor.return_value = MagicMock()
        mock_conn.return_value.close = MagicMock()
        mock_fetch.return_value = [_staleness_row(pipeline_lag=0, bar_lag=0)]
        mock_subproc.return_value = _make_proc(returncode=0, stdout=_board_success_payload())

        result = management.run_proposal_board(
            portfolio=1,
            max_proposals=8,
            max_rounds=1,
            max_candidates=5,
            inter_concurrency=2,
            market_types="STOCK",
            import_proposals_to_lpa=False,
            staleness_max_trading_days_lag=1,
        )

        self.assertEqual(result.get("status"), "SUCCESS")
        self.assertEqual(result.get("run_type"), "AGENTIC_OPPORTUNITY_SEARCH")


# ── Test 3: STOCK-only enforcement in agentic market_types parameter ──────────

class TestStockOnlyEnforcement(unittest.TestCase):
    """
    /manage/proposal-board/run normalises market_types and passes only
    requested types to run_board.py. ETF/FX can be added explicitly by
    operator, but STOCK is the default and must always appear.
    """

    @patch("app.routers.management._cockpit_run_complete")
    @patch("app.routers.management._cockpit_run_start")
    @patch("app.routers.management.fetch_all")
    @patch("app.routers.management.get_connection")
    @patch("app.routers.management.mip_workspace_root")
    @patch("app.routers.management.resolve_subprocess_python")
    @patch("subprocess.run")
    def test_default_market_types_is_stock_only(
        self, mock_subproc, mock_py, mock_root, mock_conn, mock_fetch, mock_start, mock_complete
    ):
        mock_root.return_value = Path("/fake/root")
        mock_py.return_value = Path("/fake/python")
        mock_conn.return_value.cursor.return_value = MagicMock()
        mock_conn.return_value.close = MagicMock()
        mock_fetch.return_value = [_staleness_row()]
        mock_subproc.return_value = _make_proc(stdout=_board_success_payload())

        management.run_proposal_board(
            portfolio=1,
            max_proposals=8,
            max_rounds=1,
            max_candidates=5,
            inter_concurrency=2,
            market_types="STOCK",  # default
            import_proposals_to_lpa=False,
            staleness_max_trading_days_lag=1,
        )

        board_call = mock_subproc.call_args
        cmd = board_call[0][0]
        mt_idx = cmd.index("--market-types")
        self.assertEqual(cmd[mt_idx + 1], "STOCK")

    @patch("app.routers.management._cockpit_run_complete")
    @patch("app.routers.management._cockpit_run_start")
    @patch("app.routers.management.fetch_all")
    @patch("app.routers.management.get_connection")
    @patch("app.routers.management.mip_workspace_root")
    @patch("app.routers.management.resolve_subprocess_python")
    @patch("subprocess.run")
    def test_fx_etf_excluded_from_default(
        self, mock_subproc, mock_py, mock_root, mock_conn, mock_fetch, mock_start, mock_complete
    ):
        """Sending only STOCK should never pass FX or ETF to run_board."""
        mock_root.return_value = Path("/fake/root")
        mock_py.return_value = Path("/fake/python")
        mock_conn.return_value.cursor.return_value = MagicMock()
        mock_conn.return_value.close = MagicMock()
        mock_fetch.return_value = [_staleness_row()]
        mock_subproc.return_value = _make_proc(stdout=_board_success_payload())

        management.run_proposal_board(
            portfolio=1, max_proposals=8, max_rounds=1, max_candidates=5,
            inter_concurrency=2, market_types="STOCK", import_proposals_to_lpa=False,
            staleness_max_trading_days_lag=1,
        )

        board_call = mock_subproc.call_args
        mt_idx = board_call[0][0].index("--market-types")
        passed_types = board_call[0][0][mt_idx + 1].upper()
        self.assertNotIn("FX", passed_types)
        self.assertNotIn("ETF", passed_types)
        self.assertIn("STOCK", passed_types)


# ── Test 4: COCKPIT_RUN_LOG idempotency — two runs get different run IDs ──────

class TestCockpitRunLogIdempotency(unittest.TestCase):
    """
    Each call to run_ib_manual_daily_job or run_proposal_board generates a
    fresh UUID run_id. Two sequential calls must not share the same run_id.
    """

    def test_daily_market_update_generates_unique_run_ids(self):
        run_ids = []

        def capture_start(run_id, run_type, **kwargs):
            run_ids.append(run_id)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runner = root / "cursorfiles" / "run_ib_manual_daily_job.py"
            runner.parent.mkdir(parents=True, exist_ok=True)
            runner.touch()
            py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
            py.parent.mkdir(parents=True, exist_ok=True)
            py.touch()

            with patch("app.routers.management._cockpit_run_start", side_effect=capture_start), \
                 patch("app.routers.management._cockpit_run_complete"), \
                 patch("app.routers.management.mip_workspace_root", return_value=root), \
                 patch("app.routers.management.resolve_subprocess_python", return_value=py), \
                 patch("subprocess.run") as mock_subproc:

                mock_subproc.side_effect = [
                    _make_proc(stdout=_ib_job_success_payload()),
                    _make_proc(stdout=_pipeline_success_payload()),
                    _make_proc(stdout=_ib_job_success_payload()),
                    _make_proc(stdout=_pipeline_success_payload()),
                ]

                for _ in range(2):
                    management.run_ib_manual_daily_job(
                        target_date="'2026-06-18'",
                        dry_run=False,
                        skip_ingest=False,
                        run_pipeline=True,
                        run_proposal_board=False,
                        proposal_board_portfolio=1,
                        proposal_board_max_proposals=8,
                        proposal_board_max_rounds=1,
                        proposal_board_max_candidates=5,
                        proposal_board_inter_concurrency=2,
                        proposal_board_market_types="STOCK",
                        import_proposals_to_lpa=False,
                        synth_intraday_daily=False,
                    )

        self.assertEqual(len(run_ids), 2)
        self.assertNotEqual(run_ids[0], run_ids[1])

    def test_agentic_search_generates_unique_run_ids(self):
        run_ids = []

        def capture_start(run_id, run_type, **kwargs):
            run_ids.append(run_id)

        with patch("app.routers.management._cockpit_run_start", side_effect=capture_start), \
             patch("app.routers.management._cockpit_run_complete"), \
             patch("app.routers.management.fetch_all") as mock_fetch, \
             patch("app.routers.management.get_connection") as mock_conn, \
             patch("app.routers.management.mip_workspace_root") as mock_root, \
             patch("app.routers.management.resolve_subprocess_python") as mock_py, \
             patch("subprocess.run") as mock_subproc:

            mock_root.return_value = Path("/fake/root")
            mock_py.return_value = Path("/fake/python")
            mock_conn.return_value.cursor.return_value = MagicMock()
            mock_conn.return_value.close = MagicMock()
            mock_fetch.return_value = [_staleness_row()]
            mock_subproc.return_value = _make_proc(stdout=_board_success_payload())

            for _ in range(2):
                management.run_proposal_board(
                    portfolio=1,
                    max_proposals=8,
                    max_rounds=1,
                    max_candidates=5,
                    inter_concurrency=2,
                    market_types="STOCK",
                    import_proposals_to_lpa=False,
                    staleness_max_trading_days_lag=1,
                )

        self.assertEqual(len(run_ids), 2)
        self.assertNotEqual(run_ids[0], run_ids[1])


# ── Test 5: Daily market update response contains run_type ────────────────────

class TestDailyMarketUpdateRunType(unittest.TestCase):
    """
    /manage/ib/daily-job/run must return run_type='DAILY_MARKET_UPDATE'
    and agentic_opportunity_search_not_run=True in its response.
    """

    @patch("app.routers.management._cockpit_run_complete")
    @patch("app.routers.management._cockpit_run_start")
    @patch("subprocess.run")
    def test_response_declares_daily_market_update_type(
        self, mock_subproc, mock_start, mock_complete
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runner = root / "cursorfiles" / "run_ib_manual_daily_job.py"
            runner.parent.mkdir(parents=True, exist_ok=True)
            runner.touch()
            py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
            py.parent.mkdir(parents=True, exist_ok=True)
            py.touch()

            mock_subproc.side_effect = [
                _make_proc(stdout=_ib_job_success_payload()),
                _make_proc(stdout=_pipeline_success_payload()),
            ]

            with patch("app.routers.management.mip_workspace_root", return_value=root), \
                 patch("app.routers.management.resolve_subprocess_python", return_value=py):
                result = management.run_ib_manual_daily_job(
                    target_date="'2026-06-18'",
                    dry_run=False,
                    skip_ingest=False,
                    run_pipeline=True,
                    run_proposal_board=False,
                    proposal_board_portfolio=1,
                    proposal_board_max_proposals=8,
                    proposal_board_max_rounds=1,
                    proposal_board_max_candidates=5,
                    proposal_board_inter_concurrency=2,
                    proposal_board_market_types="STOCK",
                    import_proposals_to_lpa=False,
                    synth_intraday_daily=False,
                )

        self.assertEqual(result.get("run_type"), "DAILY_MARKET_UPDATE")
        self.assertTrue(result.get("agentic_opportunity_search_not_run"))
        self.assertFalse(result.get("proposal_board_triggered"))


# ── Test 6: Agentic search response confirms no auto trade ────────────────────

class TestAgenticSearchNoAutoTrade(unittest.TestCase):
    """
    POST /manage/proposal-board/run must always return trade_auto_executed=False
    regardless of the board result.
    """

    @patch("app.routers.management._cockpit_run_complete")
    @patch("app.routers.management._cockpit_run_start")
    @patch("app.routers.management.fetch_all")
    @patch("app.routers.management.get_connection")
    @patch("app.routers.management.mip_workspace_root")
    @patch("app.routers.management.resolve_subprocess_python")
    @patch("subprocess.run")
    def test_trade_auto_executed_is_always_false(
        self, mock_subproc, mock_py, mock_root, mock_conn, mock_fetch, mock_start, mock_complete
    ):
        mock_root.return_value = Path("/fake/root")
        mock_py.return_value = Path("/fake/python")
        mock_conn.return_value.cursor.return_value = MagicMock()
        mock_conn.return_value.close = MagicMock()
        mock_fetch.return_value = [_staleness_row()]
        mock_subproc.return_value = _make_proc(stdout=_board_success_payload())

        result = management.run_proposal_board(
            portfolio=1,
            max_proposals=8,
            max_rounds=1,
            max_candidates=5,
            inter_concurrency=2,
            market_types="STOCK",
            import_proposals_to_lpa=False,
            staleness_max_trading_days_lag=1,
        )

        self.assertFalse(result.get("trade_auto_executed"))
        self.assertEqual(result.get("run_type"), "AGENTIC_OPPORTUNITY_SEARCH")


# ── Test 7: run_proposal_board default is False ───────────────────────────────

class TestRunProposalBoardDefaultIsFalse(unittest.TestCase):
    """
    The run_proposal_board Query parameter on /manage/ib/daily-job/run must
    default to False (not True). Callers that do not pass the flag explicitly
    must never trigger Phase 4.
    """

    def test_default_value_is_false(self):
        import inspect
        sig = inspect.signature(management.run_ib_manual_daily_job)
        param = sig.parameters.get("run_proposal_board")
        self.assertIsNotNone(param, "run_proposal_board parameter must exist")
        # FastAPI Query default lives in param.default.default
        default_val = getattr(param.default, "default", param.default)
        self.assertFalse(
            default_val,
            f"run_proposal_board must default to False but got {default_val!r}",
        )


if __name__ == "__main__":
    unittest.main()

import unittest
from datetime import date, datetime, time
from unittest.mock import MagicMock, patch

from app.brooks_intraday.paa_reconstruction import (
    DOSSIER_ORIGIN_RECONSTRUCTION,
    assert_point_in_time_daily_bars,
    latest_daily_input_date_before,
    reconstruct_single_dossier,
)
from app.brooks_intraday.readiness import compute_readiness_labels
from app.price_action.geometry import Bar


class BrooksPhase2CReconstructionTests(unittest.TestCase):
    def test_monday_uses_prior_friday_cutoff(self):
        monday = date(2026, 7, 20)
        cutoff = latest_daily_input_date_before(monday)
        self.assertEqual(cutoff, date(2026, 7, 17))

    def test_tuesday_may_use_monday(self):
        tuesday = date(2026, 7, 21)
        cutoff = latest_daily_input_date_before(tuesday)
        self.assertEqual(cutoff, date(2026, 7, 20))

    def test_future_daily_bar_rejected(self):
        target = date(2026, 7, 21)
        bars = [
            Bar(day=date(2026, 7, 20), open=1, high=2, low=0.5, close=1.5, volume=100),
            Bar(day=date(2026, 7, 21), open=1, high=2, low=0.5, close=1.5, volume=100),
        ]
        with self.assertRaises(Exception):
            assert_point_in_time_daily_bars(bars, target)

    def test_leakage_loader_rejects_future_cutoff(self):
        from app.price_action.service import load_daily_bars_historical_reconstruction, PriceActionError

        with patch("app.price_action.service._load_daily_bars_from_mart") as load:
            load.return_value = [
                Bar(day=date(2026, 7, 21), open=1, high=2, low=0.5, close=1.5, volume=100),
            ]
            with self.assertRaises(PriceActionError):
                load_daily_bars_historical_reconstruction(
                    "AAPL",
                    target_trading_date=date(2026, 7, 21),
                    daily_bar_cutoff=date(2026, 7, 20),
                    lookback_bars=120,
                )

    @patch("app.brooks_intraday.paa_reconstruction.analyse_for_brooks_historical_reconstruction")
    @patch("app.brooks_intraday.paa_reconstruction.load_daily_bars_historical_reconstruction")
    def test_reconstructed_provenance(self, load_bars, analyse):
        load_bars.return_value = [
            Bar(day=date(2026, 7, 17), open=100, high=101, low=99, close=100.5, volume=1e6),
        ] * 40
        mock_resp = MagicMock()
        mock_resp.detected_geometry = {
            "structure": {"trend": "UPTREND", "highs": "HIGHER_HIGH", "lows": "HIGHER_LOW"},
            "support_zones": [{"low": 98, "high": 99}],
            "resistance_zones": [{"low": 105, "high": 106}],
            "latest_close": 100.5,
            "swings": [{"kind": "high", "price": 104}],
            "long_location": "CONSTRUCTIVE_PULLBACK",
            "market_cycle_context": {"cycle": "TREND_PULLBACK"},
        }
        mock_resp.situation_model = {"long_location": "CONSTRUCTIVE_PULLBACK"}
        mock_resp.methodologist.model_dump.return_value = {
            "plain_explanation": {"summary": "Reconstructed context."},
            "expert": {"preferred_scenario": "Pullback long", "main_risk": "Break support"},
            "verdict": {"decision": "WAIT_PULLBACK", "confidence": 0.6},
        }
        mock_resp.methodologist.verdict.decision = "WAIT_PULLBACK"
        mock_resp.methodologist.verdict.confidence = 0.6
        analyse.return_value = mock_resp

        dossier = reconstruct_single_dossier(
            run_id="run-1",
            symbol="AAPL",
            trading_date=date(2026, 7, 20),
        )
        self.assertEqual(dossier["dossier_origin"], DOSSIER_ORIGIN_RECONSTRUCTION)
        self.assertIsNone(dossier["source_paa_analysis_id"])
        self.assertTrue(dossier["reconstruction_id"].startswith("brooks-recon-"))
        self.assertEqual(dossier["latest_daily_bar_used"], "2026-07-17")

    def test_replay_ready_after_20_reconstructed(self):
        state = {
            "bars_frozen": True,
            "preparation": {
                "counts": {
                    "dossiers_expected": 20,
                    "dossiers_compiled": 20,
                    "reconstructed_dossiers": 20,
                    "genuine_paa_dossiers": 0,
                    "bar_sessions_expected": 20,
                    "bar_sessions_complete": 20,
                },
                "fatal_errors": [],
            },
        }
        labels = compute_readiness_labels(state)
        self.assertEqual(labels["dossier_readiness"], "READY")
        self.assertEqual(labels["replay_readiness"], "READY")

    @patch("app.brooks_intraday.repository.get_connection")
    def test_no_paa_audit_insert(self, conn_mock):
        conn = MagicMock()
        conn_mock.return_value = conn
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.return_value = None
        from app.brooks_intraday.repository import insert_dossier_if_absent

        insert_dossier_if_absent(
            run_id="r1",
            symbol="AAPL",
            trading_date=date(2026, 7, 20),
            dossier={"dossier_origin": "HISTORICAL_RECONSTRUCTION", "source_hash": "abc"},
            paa_analysis_id=None,
            board_run_id=None,
            normalized_status="HISTORICAL_RECONSTRUCTION",
            source_hash="abc",
            dossier_version="v",
            compiler_version="c",
            run_frozen=False,
        )
        sql = cur.execute.call_args_list[-1][0][0]
        self.assertIn("BROOKS_INTRADAY_DOSSIER", sql)
        self.assertNotIn("PRICE_ACTION_ANALYSER_AUDIT", sql)


if __name__ == "__main__":
    unittest.main()

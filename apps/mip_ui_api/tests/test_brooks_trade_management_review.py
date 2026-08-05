"""Review-only trade management summary API."""

from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.trade_management_review import (
    _excursion_stats,
    build_trade_management_review,
    simulation_exit_rules_for_attempt,
)

RUN_ID = "6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e"
CTX = "3defa3de-d699-424a-8ceb-78020b453284"
SIM = "8de2e63f-99f8-46ed-a2ec-f5b8c321c651"
TRADE_ID = "1fa6d8ee-bf27-453e-a587-51a2fad5fd56"


class TradeManagementReviewTests(unittest.TestCase):
    def test_simulation_exit_rules_v01_lists_no_stop_logic(self):
        rules = simulation_exit_rules_for_attempt(SIM)
        self.assertEqual(rules["simulation_ruleset_version"], "BROOKS_SIMULATION_RULESET_V0_1")
        joined = " ".join(rules["exit_rules"])
        self.assertIn("No price stop", joined)
        self.assertIn("stop_reference", joined)

    def test_excursion_stats_from_bars(self):
        bars = [
            HistoricalBar(
                symbol="AMZN",
                ts_utc=datetime(2026, 7, 13, 14, 25),
                ts_ny=datetime(2026, 7, 13, 10, 25),
                trading_date=datetime(2026, 7, 13).date(),
                open=246.0,
                high=246.62,
                low=246.0,
                close=246.53,
                volume=1,
                source="test",
                bar_size_minutes=5,
                rth=True,
            ),
            HistoricalBar(
                symbol="AMZN",
                ts_utc=datetime(2026, 7, 13, 18, 45),
                ts_ny=datetime(2026, 7, 13, 14, 45),
                trading_date=datetime(2026, 7, 13).date(),
                open=249.0,
                high=249.65,
                low=249.0,
                close=249.27,
                volume=1,
                source="test",
                bar_size_minutes=5,
                rth=True,
            ),
        ]
        with patch(
            "app.brooks_intraday.trade_management_review.load_bars_from_store",
            return_value=bars,
        ):
            exc = _excursion_stats(
                {
                    "symbol": "AMZN",
                    "entry_ts": "2026-07-13T14:25:00",
                    "exit_ts": "2026-07-13T19:55:00",
                    "entry_price": 246.53,
                    "quantity": 4,
                }
            )
        self.assertEqual(exc["mfe"], 3.12)
        self.assertEqual(exc["max_unrealized_pnl"], 12.48)
        self.assertEqual(exc["max_unrealized_pnl_ts"], "2026-07-13T18:45:00")
        self.assertEqual(exc["max_unrealized_pnl_price"], 249.65)

    @patch("app.brooks_intraday.trade_management_review.load_context_observations")
    @patch("app.brooks_intraday.trade_management_review.load_bars_from_store")
    @patch("app.brooks_intraday.trade_management_review.load_simulation_attempt")
    @patch("app.brooks_intraday.trade_management_review.load_sim_trades")
    def test_build_review_daily_invalidation_stored_not_enforced(
        self, mock_trades, mock_attempt, mock_bars, mock_ctx
    ):
        mock_trades.return_value = [
            {
                "trade_id": TRADE_ID,
                "symbol": "AMZN",
                "entry_ts": "2026-07-13T14:25:00",
                "exit_ts": "2026-07-13T19:55:00",
                "entry_price": 246.53,
                "exit_price": 247.33,
                "exit_reason": "FORCED_END_OF_DAY_EXIT",
                "realized_pnl": 3.2,
                "quantity": 4,
            }
        ]
        mock_attempt.return_value = {"simulation_ruleset_version": "BROOKS_SIMULATION_RULESET_V0_1"}
        mock_bars.return_value = []
        mock_ctx.return_value = [
            {
                "symbol": "AMZN",
                "bar_ts": "2026-07-13T14:25:00",
                "payload_json": {"daily_thesis_invalidation": 199.24},
            }
        ]
        review = build_trade_management_review(
            RUN_ID,
            TRADE_ID,
            context_attempt_id=CTX,
            simulation_attempt_id=SIM,
        )
        self.assertEqual(review["initial_stop"]["price"], 199.24)
        self.assertEqual(review["initial_stop"]["simulator_enforcement"], "stored_only_not_checked")
        self.assertEqual(review["exit_reason"], "FORCED_END_OF_DAY_EXIT")


if __name__ == "__main__":
    unittest.main()

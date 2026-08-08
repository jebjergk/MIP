"""Adviser V1.0 Learning View payload (presentation-only)."""

from __future__ import annotations

import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from app.brooks_intraday.adviser_v1_learning_view import (
    V1_LEARNING_DEFAULTS,
    _action_markers,
    build_adviser_v1_learning_payload,
    dedupe_v1_validation_sessions,
    list_v1_validation_sessions,
)


class AdviserV1LearningViewTests(unittest.TestCase):
    def test_normalize_completed_trades_two_round_trips(self):
        from app.brooks_intraday.adviser_v1_learning_view import _normalize_completed_trades

        rows = [
            {"trade_id": "a", "entry_ts_ny": "2026-07-14T09:40:00", "entry_price": 1, "quantity": 2},
            {
                "trade_id": "a",
                "entry_ts_ny": "2026-07-14T09:50:00",
                "exit_ts_ny": "2026-07-14T09:50:00",
                "exit_price": 2,
                "realized_pnl": -4.72,
                "quantity": 2,
            },
            {"trade_id": "b", "entry_ts_ny": "2026-07-14T10:10:00", "entry_price": 3, "quantity": 2},
            {
                "trade_id": "b",
                "exit_ts_ny": "2026-07-14T13:30:00",
                "realized_pnl": -11.34,
                "exit_price": 4,
                "quantity": 2,
            },
        ]
        out = _normalize_completed_trades(rows)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["trade_number"], 1)
        self.assertAlmostEqual(out[0]["realized_pnl"], -4.72)

    def test_find_v1_session_ignores_archived_db_rows(self):
        from app.brooks_intraday.adviser_v1_learning_view import find_v1_session_for_symbol_date

        with patch(
            "app.brooks_intraday.adviser_v1_learning_view._load_reviewable_v1_sessions_from_db",
            return_value=[],
        ):
            self.assertIsNone(find_v1_session_for_symbol_date("MCD", "2026-07-14"))

    def test_v1_sessions_exclude_amzn(self):
        sessions = list_v1_validation_sessions(hydrate_from_db=False)
        symbols = {s["symbol"] for s in sessions}
        self.assertIn("JPM", symbols)
        self.assertIn("MCD", symbols)
        self.assertNotIn("AMZN", symbols)
        mcd = next(s for s in sessions if s["symbol"] == "MCD")
        self.assertEqual(mcd["adviser_attempt_id"], V1_LEARNING_DEFAULTS["adviser_attempt_id"])

    def test_dedupe_v1_sessions_keeps_best_per_symbol_day(self):
        rows = [
            {
                "symbol": "AAPL",
                "trading_date": "2026-07-15",
                "adviser_attempt_id": "a",
                "adviser_status": "COMPLETED",
                "trade_count": 0,
                "adviser_call_count": 11,
                "created_at": "2026-08-08T11:06:00",
            },
            {
                "symbol": "AAPL",
                "trading_date": "2026-07-15",
                "adviser_attempt_id": "b",
                "adviser_status": "COMPLETED",
                "trade_count": 1,
                "adviser_call_count": 20,
                "created_at": "2026-08-08T11:01:00",
            },
        ]
        out = dedupe_v1_validation_sessions(rows)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["adviser_attempt_id"], "b")

    def test_action_markers_canonical(self):
        self.assertEqual(_action_markers("ARM_LONG")[0]["label"], "ARM_LONG")
        self.assertEqual(_action_markers("HOLD")[0]["kind"], "hold")
        self.assertEqual(_action_markers(None), [])

    @patch("app.brooks_intraday.adviser_v1_learning_view._card_meta", return_value={})
    @patch("app.brooks_intraday.adviser_v1_learning_view._load_sim_attempt")
    @patch("app.brooks_intraday.adviser_v1_learning_view._load_sim_trades", return_value=[])
    @patch("app.brooks_intraday.adviser_v1_learning_view.load_adviser_bars", return_value=[])
    @patch("app.brooks_intraday.adviser_v1_learning_view.load_bars_from_store")
    @patch("app.brooks_intraday.adviser_v1_learning_view._load_calls_full")
    @patch("app.brooks_intraday.adviser_v1_learning_view.load_adviser_attempt_meta")
    def test_payload_shape(
        self,
        mock_meta,
        mock_calls,
        mock_bars,
        mock_adv_bars,
        mock_trades,
        mock_sim,
        _cards,
    ):
        mock_meta.return_value = {
            "run_id": "run-1",
            "symbol": "MCD",
            "trading_date": "2026-07-14",
            "corpus_version": "v1.1",
            "cost_summary_json": {"rag_calls": 3},
            "config_json": {"adviser_version": "BROOKS_INTRADAY_ADVISER_V1_0"},
        }
        ts = datetime(2026, 7, 14, 9, 35)
        mock_bars.return_value = [
            SimpleNamespace(
                ts_ny=ts,
                open=100.0,
                high=101.0,
                low=99.5,
                close=100.5,
                volume=1000,
                rth=True,
            )
        ]
        mock_calls.return_value = [
            {
                "call_number": 1,
                "bar_ts_ny": ts,
                "bar_ts_et": "09:35",
                "wake_reason": "SCHEDULED",
                "action": "ARM_LONG",
                "position_state": "FLAT",
                "rag_card_ids": [],
                "daily_intraday_context": {
                    "setup_contract": {"setup_id": "abc", "setup_family": "TR", "armed": True},
                    "intraday_regime": {"regime": "TRADING_RANGE", "always_in_bias": None},
                },
                "observation_packet": {"intraday_regime": {"regime": "TRADING_RANGE"}},
                "invalidation_predicates": [{"type": "LOW", "level": 99.0}],
                "brooks_reasoning_summary": "Verbatim reasoning.",
            }
        ]
        mock_sim.return_value = {
            "starting_cash": 100000,
            "ending_cash": 100000,
            "realized_pnl": 0,
        }

        payload = build_adviser_v1_learning_payload(
            adviser_attempt_id="81482bb9-0171-481b-9368-a3b0a4b4e50b",
            simulation_attempt_id="758f00ab-2d62-46fb-8746-bd46fb00a178",
        )

        self.assertTrue(payload["adviser_v1_learning"])
        self.assertEqual(payload["phase"], "ADVISER_V1_LEARNING")
        self.assertEqual(len(payload["observation_grid"]), 1)
        self.assertEqual(payload["observation_grid"][0]["adviser_action"], "ARM_LONG")
        self.assertEqual(payload["chart"]["bars"][0]["markers"][0]["label"], "ARM_LONG")
        self.assertEqual(len(payload["adviser_evidence_by_call"]), 1)
        self.assertEqual(payload["session_summary"]["trades"], 0)


if __name__ == "__main__":
    unittest.main()

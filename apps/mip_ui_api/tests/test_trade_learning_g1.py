"""Phase G1 trade learning payload (presentation-only)."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from app.brooks_intraday.learning_constants import (
    PHASE_E1_CONTEXT_ATTEMPT_ID,
    PHASE_E1_VALIDATION_RUN_ID,
    PM_V01_CERT_SIMULATION_ATTEMPT_ID,
)
from app.brooks_intraday.trade_learning_g1 import (
    CANONICAL_G1_DEFAULTS,
    build_trade_learning_g1_payload,
    slice_g1_payload_for_replay,
)

RUN = PHASE_E1_VALIDATION_RUN_ID
CTX = PHASE_E1_CONTEXT_ATTEMPT_ID
SIM = PM_V01_CERT_SIMULATION_ATTEMPT_ID
TD = "2026-07-13"


def _grid_row(ts: str, *, sym_effect: str = "—", action: str = "WAIT") -> dict:
    return {
        "bar_ts": ts,
        "bar_ts_ny": ts.replace("T14:", "T10:").replace("T15:", "T11:"),
        "trading_date": TD,
        "selected_action": action,
        "state_after": action,
        "simulation_effect": sym_effect,
        "active_patterns": [],
        "ohlcv": {"open": 246.0, "high": 246.6, "low": 246.0, "close": 246.5},
    }


AMZN_TRADE = {
    "trade_id": "e0d236d5-c281-4c5c-8479-4c6a10f67356",
    "symbol": "AMZN",
    "entry_ts": "2026-07-13T14:25:00",
    "exit_ts": "2026-07-13T15:45:00",
    "entry_price": 246.53,
    "exit_price": 248.25,
    "quantity": 4,
    "exit_reason": "PROTECTIVE_STOP_HIT",
    "realized_pnl": 6.88,
}

LEDGER = {
    "events": [
        {"event": "INITIAL_STOP_CALCULATED", "symbol": "AMZN", "ts": "2026-07-13 14:25:00", "stop_price": 246.065},
        {"event": "STOP_ACTIVATED", "symbol": "AMZN", "ts": "2026-07-13 14:30:00", "active_stop": 246.065},
        {"event": "STOP_TRAIL_UPDATE", "symbol": "AMZN", "ts": "2026-07-13 15:20:00", "new_stop": 248.17},
        {"event": "STOP_TRAIL_UPDATE", "symbol": "AMZN", "ts": "2026-07-13 15:30:00", "new_stop": 248.25},
        {"event": "ENTRY", "symbol": "AMZN", "ts": "2026-07-13 14:25:00"},
        {"event": "EXIT", "symbol": "AMZN", "ts": "2026-07-13 15:45:00"},
    ]
}


class TradeLearningG1Tests(unittest.TestCase):
    @patch("app.brooks_intraday.trade_learning_g1.load_dossiers_for_run", return_value=[{"symbol": "AMZN"}])
    @patch("app.brooks_intraday.trade_learning_g1.load_management_ledger", return_value=LEDGER)
    @patch("app.brooks_intraday.trade_learning_g1.load_sim_trades")
    @patch("app.brooks_intraday.trade_learning_g1.build_symbol_learning_payload")
    @patch("app.brooks_intraday.trade_learning_g1.validate_review_attempt_override")
    @patch("app.brooks_intraday.trade_learning_g1.resolve_attempt_chain")
    def test_amzn_trade_summary_canonical(
        self, mock_chain, _validate, mock_sym_payload, mock_trades, _ledger, _dossiers
    ):
        mock_chain.return_value = {"simulation_ruleset": "BROOKS_POSITION_MANAGEMENT_RULESET_V0_1"}
        mock_trades.return_value = [AMZN_TRADE]
        rows = [
            _grid_row("2026-07-13T14:25:00", sym_effect="ENTRY AMZN x4", action="CONSIDER_ENTRY"),
            _grid_row("2026-07-13T15:20:00", action="HOLD_POSITION"),
            _grid_row("2026-07-13T15:45:00", sym_effect="EXIT AMZN", action="HOLD_POSITION"),
        ]
        mock_sym_payload.return_value = {"grid_rows": rows}

        with patch("app.brooks_intraday.trade_learning_g1._excursion_stats") as mock_exc:
            mock_exc.return_value = {
                "mfe": 2.82,
                "mae": 0.46,
                "max_unrealized_pnl": 11.28,
                "max_unrealized_pnl_ts": "2026-07-13T15:05:00",
            }
            payload = build_trade_learning_g1_payload(
                run_id=RUN,
                state={},
                cfg={},
                context_attempt_id=CTX,
                simulation_attempt_id=SIM,
                symbol="AMZN",
                trading_date=TD,
            )

        ts = payload["trade_summary"]
        self.assertEqual(ts["realized_pnl"], 6.88)
        self.assertEqual(ts["entry_price"], 246.53)
        self.assertEqual(ts["exit_price"], 248.25)
        self.assertEqual(ts["initial_stop"], 246.065)
        self.assertEqual(ts["exit_reason_plain"], "Protective stop hit")
        self.assertEqual(payload["symbol"], "AMZN")
        self.assertEqual(len(payload["educational_grid"]), 3)

    @patch("app.brooks_intraday.trade_learning_g1.load_dossiers_for_run", return_value=[{"symbol": "AAPL"}])
    @patch("app.brooks_intraday.trade_learning_g1.load_management_ledger", return_value=LEDGER)
    @patch("app.brooks_intraday.trade_learning_g1.load_sim_trades")
    @patch("app.brooks_intraday.trade_learning_g1.build_symbol_learning_payload")
    @patch("app.brooks_intraday.trade_learning_g1.validate_review_attempt_override")
    @patch("app.brooks_intraday.trade_learning_g1.resolve_attempt_chain")
    def test_aapl_has_no_amzn_entry_effect(self, mock_chain, _v, mock_sym, mock_trades, _l, _d):
        mock_chain.return_value = {"simulation_ruleset": "BROOKS_POSITION_MANAGEMENT_RULESET_V0_1"}
        mock_trades.return_value = [AMZN_TRADE]
        mock_sym.return_value = {
            "grid_rows": [
                _grid_row("2026-07-13T14:25:00", sym_effect="—", action="WAIT"),
            ]
        }
        payload = build_trade_learning_g1_payload(
            run_id=RUN,
            state={},
            cfg={},
            context_attempt_id=CTX,
            simulation_attempt_id=SIM,
            symbol="AAPL",
            trading_date=TD,
        )
        self.assertIsNone(payload["trade_summary"])
        self.assertEqual(payload["educational_grid"][0]["simulation_effect"], "—")

    @patch("app.brooks_intraday.trade_learning_g1.load_dossiers_for_run", return_value=[{"symbol": "AMZN"}])
    @patch("app.brooks_intraday.trade_learning_g1.load_management_ledger", return_value=LEDGER)
    @patch("app.brooks_intraday.trade_learning_g1.load_sim_trades", return_value=[AMZN_TRADE])
    @patch("app.brooks_intraday.trade_learning_g1.build_symbol_learning_payload")
    @patch("app.brooks_intraday.trade_learning_g1.validate_review_attempt_override")
    @patch("app.brooks_intraday.trade_learning_g1.resolve_attempt_chain")
    def test_stop_steps_at_trail_bars(self, mock_chain, _v, mock_sym, *_):
        mock_chain.return_value = {}
        bars = [
            "2026-07-13T14:25:00",
            "2026-07-13T14:30:00",
            "2026-07-13T15:15:00",
            "2026-07-13T15:20:00",
            "2026-07-13T15:30:00",
            "2026-07-13T15:45:00",
        ]
        mock_sym.return_value = {"grid_rows": [_grid_row(ts) for ts in bars]}
        with patch("app.brooks_intraday.trade_learning_g1._excursion_stats", return_value={}):
            payload = build_trade_learning_g1_payload(
                run_id=RUN,
                state={},
                cfg={},
                context_attempt_id=CTX,
                simulation_attempt_id=SIM,
                symbol="AMZN",
                trading_date=TD,
            )
        by_ts = {r["bar_ts"]: r["stop"] for r in payload["educational_grid"]}
        self.assertEqual(by_ts["2026-07-13T14:25:00"], "—")
        self.assertEqual(by_ts["2026-07-13T14:30:00"], "246.065")
        self.assertEqual(by_ts["2026-07-13T15:20:00"], "248.170")
        self.assertEqual(by_ts["2026-07-13T15:30:00"], "248.250")

    def test_replay_slice_hides_future_bars(self):
        payload = {
            "chart": {
                "bars": [
                    {"ts_utc": "2026-07-13T14:25:00"},
                    {"ts_utc": "2026-07-13T14:30:00"},
                    {"ts_utc": "2026-07-13T15:45:00"},
                ]
            },
            "educational_grid": [
                {"bar_ts": "2026-07-13T14:25:00"},
                {"bar_ts": "2026-07-13T14:30:00"},
                {"bar_ts": "2026-07-13T15:45:00"},
            ],
        }
        sliced = slice_g1_payload_for_replay(payload, "2026-07-13T14:30:00")
        self.assertEqual(len(sliced["chart"]["bars"]), 2)
        self.assertEqual(len(sliced["educational_grid"]), 2)

    def test_canonical_defaults_ids(self):
        self.assertEqual(CANONICAL_G1_DEFAULTS["run_id"], RUN)
        self.assertEqual(CANONICAL_G1_DEFAULTS["simulation_attempt_id"], SIM)


if __name__ == "__main__":
    unittest.main()

"""Phase 7 — simulation gating and portfolio engine tests."""

from __future__ import annotations

import unittest
from datetime import datetime

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.context_ruleset_v01 import ACTION_CONSIDER_ENTRY, ACTION_OBSERVE
from app.brooks_intraday.simulation_engine import (
    BLOCK_REASON_NOT_ENTRY_SIGNAL,
    BLOCK_REASON_POSITION_OPEN,
    PortfolioSimState,
    entry_permitted,
    execute_entry,
    process_symbol_bar,
    record_blocked_entry,
    tie_break_pick,
)
from app.brooks_intraday.simulation_ruleset_v01 import REQUIRED_CONTEXT_RULESET


def _bar(sym: str = "AAPL", close: float = 100.0) -> HistoricalBar:
    dt = datetime(2026, 7, 21, 14, 0, 0)
    return HistoricalBar(
        symbol=sym,
        trading_date=dt.date(),
        ts_utc=dt,
        ts_ny=dt,
        open=close - 0.1,
        high=close + 0.2,
        low=close - 0.2,
        close=close,
        volume=1000,
        source="TEST",
        bar_size_minutes=5,
        rth=True,
    )


def _ctx(action: str) -> dict:
    return {
        "selected_action": action,
        "payload_json": {
            "layers_json": {
                "contextual_interpretation": {"room_class": "AMPLE_ROOM"},
                "diagnostics": {"entry_score": 7},
            }
        },
    }


class Phase7GatingTests(unittest.TestCase):
    def test_only_consider_entry_permits(self):
        dossier = {"trade_simulation_ready": True, "paa_verdict": "LONG_APPROVE"}
        ok, reason = entry_permitted(ctx=_ctx(ACTION_OBSERVE), dossier=dossier, bar_index_in_session=5, params={})
        self.assertFalse(ok)
        self.assertEqual(reason, BLOCK_REASON_NOT_ENTRY_SIGNAL)
        ok2, _ = entry_permitted(
            ctx=_ctx(ACTION_CONSIDER_ENTRY), dossier=dossier, bar_index_in_session=5, params={}
        )
        self.assertTrue(ok2)

    def test_v02_context_ruleset_constant(self):
        self.assertEqual(REQUIRED_CONTEXT_RULESET, "BROOKS_CONTEXT_RULESET_V0_2")


class Phase7OnePositionTests(unittest.TestCase):
    def test_second_entry_blocked_when_position_open(self):
        pf = PortfolioSimState(cash=1000.0)
        execute_entry(
            pf,
            candidate={"context": _ctx(ACTION_CONSIDER_ENTRY), "dossier": {"trade_simulation_ready": True}},
            bar=_bar("AAPL", 100),
        )
        self.assertIsNotNone(pf.open_position)
        record_blocked_entry(
            pf,
            symbol="AMZN",
            signal_ts=_bar("AMZN").ts_utc,
            reason=BLOCK_REASON_POSITION_OPEN,
            candidate_action=ACTION_CONSIDER_ENTRY,
        )
        self.assertEqual(pf.blocked_signals[-1]["block_reason"], BLOCK_REASON_POSITION_OPEN)


class Phase7TieBreakTests(unittest.TestCase):
    def test_deterministic_winner(self):
        cands = [
            {"symbol": "AMZN", "context": _ctx(ACTION_CONSIDER_ENTRY), "dossier": {"paa_verdict": "WAIT_PULLBACK"}},
            {"symbol": "AAPL", "context": _ctx(ACTION_CONSIDER_ENTRY), "dossier": {"paa_verdict": "LONG_APPROVE"}},
        ]
        w1, _ = tie_break_pick(cands, params={}, symbol_order=["AAPL", "AMZN", "JPM", "MCD"])
        w2, _ = tie_break_pick(cands, params={}, symbol_order=["AAPL", "AMZN", "JPM", "MCD"])
        self.assertEqual(w1["symbol"], w2["symbol"])


class Phase7ExitScheduleTests(unittest.TestCase):
    def test_thesis_invalidated_schedules_exit(self):
        pf = PortfolioSimState(cash=0.0)
        pf.open_position = type("P", (), {})()
        pf.open_position.symbol = "AAPL"
        pf.open_position.trade_id = "t1"
        pf.open_position.entry_price = 100
        pf.open_position.quantity = 5
        pf.open_position.signal_ts = _bar().ts_utc
        pf.open_position.entry_ts = _bar().ts_utc
        pf.open_position.initial_cash = 1000
        # use real OpenSimPosition
        from app.brooks_intraday.simulation_engine import OpenSimPosition

        pf.open_position = OpenSimPosition(
            trade_id="t1",
            symbol="AAPL",
            quantity=5,
            entry_price=100,
            entry_ts=_bar().ts_utc,
            signal_ts=_bar().ts_utc,
            initial_cash=1000,
        )
        res = process_symbol_bar(
            portfolio=pf,
            ctx={"selected_action": "THESIS_INVALIDATED"},
            bar=_bar(),
            dossier={"trade_simulation_ready": True},
            bar_index_in_session=10,
            is_last_bar_in_session=False,
        )
        self.assertTrue(res.exit_scheduled)
        self.assertIsNotNone(pf.pending_exit)


if __name__ == "__main__":
    unittest.main()

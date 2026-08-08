"""POC 4 regression: watch semantics, lab sizing, CONSIDER_ENTRY, trade reporting."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import datetime

from app.brooks_intraday.adviser_setup_contract_v01 import (
    entry_allowed_consider_entry,
    freeze_setup_contract_from_predicates,
)
from app.brooks_intraday.adviser_sim_lab_v01 import (
    LAB_STARTING_CASH,
    LabSimState,
    summarize_sim_trade_rows,
    try_open_long,
    whole_share_qty,
)
from app.brooks_intraday.adviser_watch_v01 import (
    MAX_WATCH_PREDICATES,
    evaluate_watch_edge,
    process_watch_predicates,
    watch_logical_key,
    watch_predicate_is_future_actionable,
)
from app.brooks_intraday.adviser_wake_v01 import ThesisWakeState, evaluate_wake
from app.brooks_intraday.objective_ruleset_v01 import compute_geometry


@dataclass
class _Bar:
    open: float
    high: float
    low: float
    close: float
    volume: float = 1.0
    ts_ny: datetime | None = None


def _g(bar: _Bar):
    return compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)


class WatchSemanticsTests(unittest.TestCase):
    def test_watch_edge_requires_false_to_true(self):
        pred = {"type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"}
        prev_bar = _Bar(9.8, 9.95, 9.7, 9.85)
        bar = _Bar(9.9, 10.2, 9.7, 10.05)
        prev = {"direction": "BULLISH", "range": 0.3, "high": 9.95, "low": 9.7}
        self.assertFalse(
            evaluate_watch_edge(pred, bar=bar, prev_bar=None, prev_geom=prev, recent=[bar])
        )
        self.assertTrue(
            evaluate_watch_edge(
                pred, bar=bar, prev_bar=prev_bar, prev_geom=prev, recent=[prev_bar, bar]
            )
        )
        bar_still_above = _Bar(10.0, 10.3, 9.95, 10.1)
        self.assertFalse(
            evaluate_watch_edge(
                pred,
                bar=bar_still_above,
                prev_bar=bar,
                prev_geom=prev,
                recent=[prev_bar, bar, bar_still_above],
            )
        )

    def test_semantic_watch_does_not_refire_on_new_predicate_id(self):
        state = ThesisWakeState()
        pred_a = {"id": "aaa", "type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"}
        pred_b = {"id": "bbb", "type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"}
        lkey = watch_logical_key(pred_a)
        state.fired_watch_logical.add(lkey)
        state.replace_predicates_from_adviser(
            watch_predicates=[pred_b],
            invalidation_predicates=[],
            watch_text=[],
            prior_action="WATCH_LONG",
            new_action="WATCH_LONG",
        )
        self.assertIn(lkey, state.fired_watch_logical)

    def test_max_three_active_watch_predicates(self):
        bar = _Bar(10, 10.5, 9.5, 10.0)
        raw = [
            {"type": "PULLBACK_LEG_COMPLETED", "minimum_legs": 2},
            {"type": "SIGNAL_BAR_CONFIRMED"},
            {"type": "SECOND_ENTRY_CONFIRMED"},
            {"type": "FAILED_BREAKOUT_CONFIRMED"},
        ]
        kept, stats = process_watch_predicates(raw, bar)
        self.assertLessEqual(len(kept), MAX_WATCH_PREDICATES)
        self.assertEqual(stats.stored, MAX_WATCH_PREDICATES)
        self.assertEqual(stats.rejected_cap, 1)

    def test_watch_must_be_future_actionable(self):
        bar = _Bar(10.1, 10.5, 10.0, 10.2)
        pred = {"type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"}
        self.assertFalse(watch_predicate_is_future_actionable(bar, pred))
        kept, stats = process_watch_predicates([pred], bar)
        self.assertEqual(kept, [])
        self.assertEqual(stats.rejected_already_true, 1)


class LabSimTests(unittest.TestCase):
    def test_whole_share_qty_from_cash(self):
        qty = whole_share_qty(available_cash=1000.0, entry_price=248.10)
        self.assertEqual(qty, 4)
        self.assertLessEqual(qty * 248.10, 1000.0)

    def test_cannot_buy_100_amzn_on_1k(self):
        state = LabSimState(cash=LAB_STARTING_CASH)
        ok = try_open_long(state, entry_price=248.10, stop_price=240.0, bar_ts="t")
        self.assertTrue(ok)
        self.assertLessEqual(state.position_qty, 4)
        self.assertNotEqual(state.position_qty, 100)

    def test_one_position_constraint(self):
        state = LabSimState(cash=LAB_STARTING_CASH)
        self.assertTrue(try_open_long(state, entry_price=50.0, stop_price=48.0, bar_ts="t1"))
        self.assertFalse(try_open_long(state, entry_price=50.0, stop_price=48.0, bar_ts="t2"))


class ConsiderEntryTests(unittest.TestCase):
    def test_consider_entry_with_pending_frozen_confirmation_does_not_allow(self):
        contract = freeze_setup_contract_from_predicates(
            mandatory_preds=[
                {
                    "type": "CLOSE_ABOVE",
                    "level": 248.50,
                    "valid_from_bar_offset": 0,
                    "valid_until_bar_offset": 5,
                }
            ],
            optional_text=[],
            signal_bar_index=10,
            signal_bar_et="09:55",
        )
        bar = _Bar(248, 249, 247, 248.5)
        allowed, reason = entry_allowed_consider_entry(
            "CONSIDER_ENTRY",
            contract,
            bar=bar,
            bar_index=11,
            recent=[bar],
            prev_bar=None,
            prev_geom=None,
            g=_g(bar),
        )
        self.assertFalse(allowed)
        self.assertIn("pending", reason.lower())

    def test_consider_entry_after_frozen_boolean_may_execute(self):
        contract = freeze_setup_contract_from_predicates(
            mandatory_preds=[
                {
                    "type": "CLOSE_ABOVE",
                    "level": 248.50,
                    "valid_from_bar_offset": 0,
                    "valid_until_bar_offset": 5,
                }
            ],
            optional_text=[],
            signal_bar_index=10,
            signal_bar_et="09:55",
        )
        bar = _Bar(248.4, 249.0, 248.2, 248.55)
        allowed, reason = entry_allowed_consider_entry(
            "CONSIDER_ENTRY",
            contract,
            bar=bar,
            bar_index=11,
            recent=[bar],
            prev_bar=None,
            prev_geom=None,
            g=_g(bar),
        )
        self.assertTrue(allowed)
        self.assertEqual(reason, "frozen_confirmations_satisfied")


class TradeReportingTests(unittest.TestCase):
    def test_summarize_round_trip_not_raw_row_count(self):
        rows = [
            {
                "trade_id": "t1",
                "entry_ts_ny": "2026-07-13 09:55:00",
                "entry_price": 248.1,
                "quantity": 4,
            },
            {
                "trade_id": "t1",
                "entry_ts_ny": "2026-07-13 09:55:00",
                "exit_ts_ny": "2026-07-13 10:20:00",
                "exit_price": 247.5,
                "quantity": 4,
                "realized_pnl": -2.4,
            },
        ]
        s = summarize_sim_trade_rows(rows)
        self.assertEqual(s["entries"], 1)
        self.assertEqual(s["completed_round_trips"], 1)
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(s["completed_round_trips"], len(rows))


if __name__ == "__main__":
    unittest.main()

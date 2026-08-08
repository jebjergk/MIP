"""Certification tests — Re-entry Policy V0.1."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from app.brooks_intraday.context_ruleset_v01 import (
    ACTION_CONSIDER_ENTRY,
    ACTION_ENTRY_ARMED,
    ACTION_OBSERVE,
    STATE_ENTRY_ARMED,
)
from app.brooks_intraday.reentry_policy_v01 import (
    BLOCK_REENTRY_COOLDOWN,
    BLOCK_REENTRY_EOD,
    BLOCK_REENTRY_SETUP_CONSUMED,
)
from app.brooks_intraday.simulation_engine import BLOCK_REASON_POSITION_OPEN, BLOCK_REASON_TIE_BREAK
from app.brooks_intraday.simulation_runner import FixtureStep, make_bar, make_ctx, run_simulation_fixture


def _ctx(
    action: str,
    *,
    cycle: str,
    state_before: str = STATE_ENTRY_ARMED,
    armed_bar: int = 10,
) -> dict:
    base = make_ctx(action)
    base["state_before"] = state_before
    pj = base.setdefault("payload_json", {})
    layers = pj.setdefault("layers_json", {})
    diag = layers.setdefault("diagnostics", {})
    diag["setup_cycle_id"] = cycle
    diag["entry_armed_bar"] = armed_bar
    return base


class ReentryPolicyCertificationTests(unittest.TestCase):
    TD = date(2026, 7, 13)
    DOSSIER = {"trade_simulation_ready": True, "paa_verdict": "LONG_APPROVE"}

    def test_stale_duplicate_consider_entry_blocked(self):
        sym = "AAPL"
        cycle = "AAPL|2026-07-13|1"
        steps = [
            FixtureStep(
                20,
                False,
                bars={sym: make_bar(sym, datetime(2026, 7, 13, 14, 0), o=100, h=100.2, l=99.8, c=100, trading_date=self.TD)},
                context_by_symbol={sym: _ctx(ACTION_CONSIDER_ENTRY, cycle=cycle, state_before=STATE_ENTRY_ARMED, armed_bar=19)},
                dossiers_by_symbol={sym: self.DOSSIER},
            ),
            FixtureStep(
                25,
                False,
                bars={sym: make_bar(sym, datetime(2026, 7, 13, 14, 25), o=100, h=100.2, l=99.8, c=100, trading_date=self.TD)},
                context_by_symbol={sym: _ctx("THESIS_INVALIDATED", cycle=cycle)},
                dossiers_by_symbol={sym: self.DOSSIER},
            ),
            FixtureStep(
                26,
                False,
                bars={sym: make_bar(sym, datetime(2026, 7, 13, 14, 30), o=99.5, h=100, l=99.5, c=99.8, trading_date=self.TD)},
                context_by_symbol={sym: _ctx(ACTION_OBSERVE, cycle=cycle, state_before="OBSERVATION_ONLY")},
                dossiers_by_symbol={sym: self.DOSSIER},
            ),
            FixtureStep(
                28,
                False,
                bars={sym: make_bar(sym, datetime(2026, 7, 13, 14, 40), o=99.8, h=100, l=99.7, c=99.9, trading_date=self.TD)},
                context_by_symbol={sym: _ctx(ACTION_OBSERVE, cycle=cycle, state_before="OBSERVATION_ONLY")},
                dossiers_by_symbol={sym: self.DOSSIER},
            ),
            FixtureStep(
                30,
                False,
                bars={sym: make_bar(sym, datetime(2026, 7, 13, 14, 50), o=100, h=100.2, l=99.9, c=100, trading_date=self.TD)},
                context_by_symbol={sym: _ctx(ACTION_CONSIDER_ENTRY, cycle=cycle, state_before=STATE_ENTRY_ARMED, armed_bar=19)},
                dossiers_by_symbol={sym: self.DOSSIER},
            ),
        ]
        r = run_simulation_fixture(steps, symbol_order=[sym])
        self.assertEqual(len(r.portfolio.closed_trades), 1)
        reasons = [b["block_reason"] for b in r.portfolio.blocked_signals]
        self.assertIn(BLOCK_REENTRY_SETUP_CONSUMED, reasons)

    def test_fresh_cycle_second_trade_allowed(self):
        sym = "AAPL"
        c1 = "AAPL|2026-07-13|1"
        c2 = "AAPL|2026-07-13|2"
        mk = lambda i, h, m, ctx: FixtureStep(
            i,
            False,
            bars={sym: make_bar(sym, datetime(2026, 7, 13, h, m), o=100, h=100.2, l=99.8, c=100, trading_date=self.TD)},
            context_by_symbol={sym: ctx},
            dossiers_by_symbol={sym: self.DOSSIER},
        )
        steps = [
            mk(20, 14, 0, _ctx(ACTION_CONSIDER_ENTRY, cycle=c1, armed_bar=19)),
            mk(22, 14, 10, _ctx("THESIS_INVALIDATED", cycle=c1)),
            mk(23, 14, 15, _ctx(ACTION_OBSERVE, cycle=c1, state_before="OBSERVATION_ONLY")),
            mk(24, 14, 20, _ctx(ACTION_OBSERVE, cycle=c1, state_before="OBSERVATION_ONLY")),
            mk(25, 14, 25, _ctx(ACTION_OBSERVE, cycle=c1, state_before="OBSERVATION_ONLY")),
            mk(26, 14, 30, _ctx(ACTION_ENTRY_ARMED, cycle=c2, state_before="WAITING_FOR_BREAKOUT_PULLBACK", armed_bar=26)),
            mk(28, 14, 40, _ctx(ACTION_CONSIDER_ENTRY, cycle=c2, state_before=STATE_ENTRY_ARMED, armed_bar=26)),
        ]
        r = run_simulation_fixture(steps, symbol_order=[sym])
        self.assertEqual(len(r.portfolio.closed_trades), 1, r.portfolio.blocked_signals)
        self.assertIsNotNone(r.portfolio.open_position)
        day_key = f"{sym}|2026-07-13"
        self.assertEqual(r.portfolio.reentry_tracker.by_day[day_key].trades_today, 2)

    def test_cooldown_blocks_early_rearm(self):
        sym = "AAPL"
        c1 = "AAPL|2026-07-13|1"
        c2 = "AAPL|2026-07-13|2"
        steps = [
            FixtureStep(10, False, bars={sym: make_bar(sym, datetime(2026, 7, 13, 10, 0), o=100, h=100, l=100, c=100, trading_date=self.TD)}, context_by_symbol={sym: _ctx(ACTION_CONSIDER_ENTRY, cycle=c1, armed_bar=9)}, dossiers_by_symbol={sym: self.DOSSIER}),
            FixtureStep(12, False, bars={sym: make_bar(sym, datetime(2026, 7, 13, 10, 10), o=100, h=100, l=100, c=100, trading_date=self.TD)}, context_by_symbol={sym: _ctx("THESIS_INVALIDATED", cycle=c1)}, dossiers_by_symbol={sym: self.DOSSIER}),
            FixtureStep(13, False, bars={sym: make_bar(sym, datetime(2026, 7, 13, 10, 15), o=100, h=100, l=100, c=100, trading_date=self.TD)}, context_by_symbol={sym: _ctx(ACTION_ENTRY_ARMED, cycle=c2, state_before="OBSERVATION_ONLY", armed_bar=13)}, dossiers_by_symbol={sym: self.DOSSIER}),
            FixtureStep(14, False, bars={sym: make_bar(sym, datetime(2026, 7, 13, 10, 20), o=100, h=100, l=100, c=100, trading_date=self.TD)}, context_by_symbol={sym: _ctx(ACTION_CONSIDER_ENTRY, cycle=c2, armed_bar=13)}, dossiers_by_symbol={sym: self.DOSSIER}),
        ]
        r = run_simulation_fixture(steps, symbol_order=[sym])
        self.assertEqual(len(r.portfolio.closed_trades), 1)
        self.assertTrue(any(b["block_reason"] == BLOCK_REENTRY_COOLDOWN for b in r.portfolio.blocked_signals))

    def test_one_position_blocks_other_symbol(self):
        steps = [
            FixtureStep(
                10,
                False,
                bars={
                    "AAPL": make_bar("AAPL", datetime(2026, 7, 13, 10, 0), o=100, h=100, l=100, c=100, trading_date=self.TD),
                    "AMZN": make_bar("AMZN", datetime(2026, 7, 13, 10, 0), o=200, h=200, l=200, c=200, trading_date=self.TD),
                },
                context_by_symbol={
                    "AAPL": _ctx(ACTION_CONSIDER_ENTRY, cycle="AAPL|2026-07-13|1", armed_bar=9),
                    "AMZN": _ctx(ACTION_CONSIDER_ENTRY, cycle="AMZN|2026-07-13|1", armed_bar=9),
                },
                dossiers_by_symbol={"AAPL": self.DOSSIER, "AMZN": self.DOSSIER},
            ),
        ]
        r = run_simulation_fixture(steps, symbol_order=["AAPL", "AMZN"])
        self.assertEqual(len(r.portfolio.closed_trades), 0)
        self.assertIsNotNone(r.portfolio.open_position)
        self.assertTrue(
            any(
                b["block_reason"] in (BLOCK_REASON_POSITION_OPEN, BLOCK_REASON_TIE_BREAK)
                for b in r.portfolio.blocked_signals
            )
        )

    def test_eod_blocks_same_day_reentry(self):
        sym = "AAPL"
        c2 = "AAPL|2026-07-13|2"
        steps = [
            FixtureStep(10, False, bars={sym: make_bar(sym, datetime(2026, 7, 13, 10, 0), o=100, h=100, l=100, c=100, trading_date=self.TD)}, context_by_symbol={sym: _ctx(ACTION_CONSIDER_ENTRY, cycle="AAPL|2026-07-13|1", armed_bar=9)}, dossiers_by_symbol={sym: self.DOSSIER}),
            FixtureStep(12, True, bars={sym: make_bar(sym, datetime(2026, 7, 13, 15, 55), o=101, h=101, l=101, c=101, trading_date=self.TD)}, context_by_symbol={sym: _ctx(ACTION_OBSERVE, cycle="AAPL|2026-07-13|1")}, dossiers_by_symbol={sym: self.DOSSIER}),
            FixtureStep(20, False, bars={sym: make_bar(sym, datetime(2026, 7, 13, 16, 0), o=101, h=101, l=101, c=101, trading_date=self.TD)}, context_by_symbol={sym: _ctx(ACTION_CONSIDER_ENTRY, cycle=c2, armed_bar=19)}, dossiers_by_symbol={sym: self.DOSSIER}),
        ]
        r = run_simulation_fixture(steps, symbol_order=[sym])
        self.assertEqual(len(r.portfolio.closed_trades), 1)
        self.assertEqual(r.portfolio.closed_trades[0]["exit_reason"], "FORCED_END_OF_DAY_EXIT")
        self.assertTrue(any(b["block_reason"] == BLOCK_REENTRY_EOD for b in r.portfolio.blocked_signals))

    def test_canonical_amzn_single_trade_fixture(self):
        """Regression: one arm/entry/PM-style stop exit — economics unchanged."""
        sym = "AMZN"
        cycle = "AMZN|2026-07-13|1"
        steps = [
            FixtureStep(
                25,
                False,
                bars={sym: make_bar(sym, datetime(2026, 7, 13, 14, 25), o=246.39, h=246.62, l=246.07, c=246.53, trading_date=self.TD)},
                context_by_symbol={sym: _ctx(ACTION_CONSIDER_ENTRY, cycle=cycle, armed_bar=24)},
                dossiers_by_symbol={sym: self.DOSSIER},
            ),
            FixtureStep(
                45,
                False,
                bars={sym: make_bar(sym, datetime(2026, 7, 13, 15, 45), o=248.3, h=248.5, l=248.17, c=248.25, trading_date=self.TD)},
                context_by_symbol={sym: _ctx(ACTION_OBSERVE, cycle=cycle)},
                dossiers_by_symbol={sym: self.DOSSIER},
            ),
        ]
        r = run_simulation_fixture(steps, symbol_order=[sym], starting_cash=1000.0)
        self.assertEqual(len(r.portfolio.closed_trades), 0)
        self.assertIsNotNone(r.portfolio.open_position)
        from app.brooks_intraday.position_management_engine import PMPortfolio, on_entry_filled, process_pm_bar_open

        pm = PMPortfolio(cash=r.portfolio.cash)
        pm.open_position = r.portfolio.open_position
        pm.reentry_tracker = r.portfolio.reentry_tracker
        ctx = _ctx(ACTION_CONSIDER_ENTRY, cycle=cycle, armed_bar=24)
        ctx["payload_json"]["layers_json"]["contextual_interpretation"] = {"broken_resistance": 246.065}
        entry_bar = steps[0].bars[sym]
        on_entry_filled(pm, ctx=ctx, entry_bar=entry_bar, quantity=4)
        exit_bar = make_bar(sym, datetime(2026, 7, 13, 15, 45), o=248.3, h=248.5, l=248.17, c=248.25, trading_date=self.TD)
        activate = make_bar(sym, datetime(2026, 7, 13, 14, 30), o=246.55, h=247, l=246.2, c=246.8, trading_date=self.TD)
        process_pm_bar_open(pm, bar=activate, bar_index_in_session=26)
        pm.pm_position.active_stop = 248.25
        pm.pm_position.stop_active = True
        process_pm_bar_open(pm, bar=exit_bar, bar_index_in_session=45)
        self.assertEqual(len(pm.closed_trades), 1)
        t = pm.closed_trades[0]
        self.assertAlmostEqual(float(t["entry_price"]), 246.53, places=2)
        self.assertAlmostEqual(float(t["exit_price"]), 248.25, places=2)
        self.assertEqual(t["exit_reason"], "PROTECTIVE_STOP_HIT")
        self.assertAlmostEqual(float(t["realized_pnl"]), 6.88, places=2)


if __name__ == "__main__":
    unittest.main()

"""Unit tests for BROOKS_POSITION_MANAGEMENT_RULESET_V0_1."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.position_management_engine import (
    PMPortfolio,
    on_entry_filled,
    pm01_initial_stop,
    process_pm_bar_close,
    process_pm_bar_open,
    schedule_swing_confirmations_for_bar,
)


def _bar(ts: str, o: float, h: float, l: float, c: float) -> HistoricalBar:
    dt = datetime.fromisoformat(ts)
    return HistoricalBar(
        symbol="AMZN",
        ts_utc=dt,
        ts_ny=dt,
        trading_date=date(2026, 7, 13),
        open=o,
        high=h,
        low=l,
        close=c,
        volume=1,
        source="test",
        bar_size_minutes=5,
        rth=True,
    )


class PM01InitialStopTests(unittest.TestCase):
    def test_prefers_broken_resistance_over_entry_low(self):
        ctx = {
            "payload_json": {
                "layers_json": {
                    "contextual_interpretation": {"broken_resistance": 246.065},
                },
            }
        }
        bar = _bar("2026-07-13T14:25:00", 246.39, 246.62, 246.07, 246.53)
        px, src = pm01_initial_stop(ctx=ctx, entry_bar=bar, params={"price_tick": 0.01})
        self.assertEqual(px, 246.065)
        self.assertEqual(src, "broken_resistance")


class PM02StopTests(unittest.TestCase):
    def test_gap_through_at_open(self):
        pm = PMPortfolio(cash=500)
        ctx = {"payload_json": {"layers_json": {"contextual_interpretation": {"broken_resistance": 246.065}}}}
        entry = _bar("2026-07-13T14:25:00", 246, 246.6, 246, 246.53)
        on_entry_filled(pm, ctx=ctx, entry_bar=entry, quantity=4)
        nxt = _bar("2026-07-13T14:30:00", 245.9, 246.0, 245.5, 245.8)
        process_pm_bar_open(pm, bar=nxt)
        self.assertIsNone(pm.pm_position)
        self.assertEqual(pm.closed_trades[0]["exit_reason"], "STOP_GAP_THROUGH")


class PM04FailedBreakoutTests(unittest.TestCase):
    def test_schedules_exit_next_bar(self):
        pm = PMPortfolio(cash=500)
        ctx = {"payload_json": {"layers_json": {"contextual_interpretation": {"broken_resistance": 246.065}}}}
        entry = _bar("2026-07-13T14:25:00", 246, 246.6, 246, 246.53)
        on_entry_filled(pm, ctx=ctx, entry_bar=entry, quantity=4)
        act = _bar("2026-07-13T14:30:00", 246.55, 247, 246.2, 246.8)
        process_pm_bar_open(pm, bar=act)
        fail_ctx = {
            "state_after": "FAILED_BREAKOUT",
            "selected_action": "DO_NOT_ENTER",
            "payload_json": {
                "layers_json": {"contextual_interpretation": {"resistance_lifecycle": "FAILED_BREAKOUT"}},
            },
        }
        fail_bar = _bar("2026-07-13T14:35:00", 246.8, 247, 246.5, 246.6)
        process_pm_bar_close(pm, ctx=fail_ctx, bar=fail_bar, is_last_bar_in_session=False, swing_low_confirms=[])
        self.assertIsNotNone(pm.pm_position.pending_context_exit)
        fill_bar = _bar("2026-07-13T14:40:00", 246.4, 246.5, 246.2, 246.3)
        process_pm_bar_open(pm, bar=fill_bar)
        self.assertIsNone(pm.pm_position)
        self.assertEqual(pm.closed_trades[0]["exit_reason"], "CONTEXT_FAILED_BREAKOUT")


class SwingScheduleTests(unittest.TestCase):
    def test_confirms_on_latest_ts(self):
        pats = [
            {
                "symbol": "AMZN",
                "pattern_family": "STRUCTURAL_SWING_LOW",
                "lifecycle_status": "CONFIRMED",
                "latest_ts": "2026-07-13T15:10:00",
                "pattern_instance_id": "x",
            }
        ]
        out = schedule_swing_confirmations_for_bar(pats, symbol="AMZN", bar_ts="2026-07-13T15:10:00")
        self.assertEqual(len(out), 1)

    def test_trail_uses_relevant_prices_swing_low(self):
        pm = PMPortfolio(cash=500)
        ctx = {"payload_json": {"layers_json": {"contextual_interpretation": {"broken_resistance": 246.065}}}}
        entry = _bar("2026-07-13T14:25:00", 246, 246.6, 246, 246.53)
        on_entry_filled(pm, ctx=ctx, entry_bar=entry, quantity=4)
        act = _bar("2026-07-13T14:30:00", 246.55, 247, 246.2, 246.8)
        process_pm_bar_open(pm, bar=act)
        confirm_bar = _bar("2026-07-13T15:15:00", 248.3, 248.5, 248.27, 248.4)
        swings = [
            {
                "pattern_family": "STRUCTURAL_SWING_LOW",
                "lifecycle_status": "CONFIRMED",
                "pattern_instance_id": "6356365d-07c0-4c18-900d-a91ca01580d2",
                "start_ts": "2026-07-13T15:10:00",
                "relevant_prices_json": {"swing_low": 248.18},
            }
        ]
        process_pm_bar_close(
            pm,
            ctx=ctx,
            bar=confirm_bar,
            is_last_bar_in_session=False,
            swing_low_confirms=swings,
        )
        self.assertEqual(pm.pm_position.pending_stop, 248.17)
        trail = next(e for e in pm.management_ledger if e.get("event") == "STOP_TRAIL_SCHEDULED")
        self.assertEqual(trail["candidate_stop"], 248.17)


if __name__ == "__main__":
    unittest.main()

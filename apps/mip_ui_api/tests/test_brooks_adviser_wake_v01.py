"""Regression tests for thesis-driven Adviser wake + daily PAA policy."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import datetime

from app.brooks_intraday.adviser_paa_policy_v01 import (
    assert_no_short_action,
    daily_bias_note,
    is_frozen_daily_confirmation,
    sanitize_confirmation_strings,
)
from app.brooks_intraday.adviser_wake_v01 import (
    WAKE_INVALIDATED,
    WAKE_SAFETY,
    WAKE_WATCH,
    ThesisWakeState,
    evaluate_predicate,
    evaluate_wake,
    legacy_material_change,
)
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


class AdviserWakeTests(unittest.TestCase):
    def test_bar_flip_does_not_wake_without_predicate(self):
        state = ThesisWakeState(
            bars_since_meaningful_wake=0,
            watch_predicates=[{"type": "LEVEL_BREAK", "level": 300.0, "direction": "ABOVE"}],
        )
        bar = _Bar(10, 10.5, 9.8, 10.2)
        prev = {"direction": "BEARISH", "range": 0.5, "high": 10.0, "low": 9.5}
        g = _g(bar)
        self.assertIsNone(
            evaluate_wake(state, bar_index=1, bar=bar, g=g, prev_geom=prev, recent=[bar])
        )
        self.assertIsNotNone(legacy_material_change(prev, bar, g))

    def test_swing_break_does_not_wake_without_predicate(self):
        state = ThesisWakeState(bars_since_meaningful_wake=0)
        bar = _Bar(10, 11, 9.9, 10.8)
        prev = {"direction": "BULLISH", "range": 0.4, "high": 10.0, "low": 9.6}
        g = _g(bar)
        self.assertIsNotNone(legacy_material_change(prev, bar, g))
        self.assertIsNone(
            evaluate_wake(state, bar_index=2, bar=bar, g=g, prev_geom=prev, recent=[bar])
        )

    def test_explicit_swing_predicate_can_wake(self):
        pred = {"type": "SWING_BREAK", "direction": "ABOVE", "margin": 0.05}
        bar = _Bar(10, 11, 9.9, 10.8)
        prev = {"direction": "BULLISH", "range": 0.4, "high": 10.0, "low": 9.6}
        g = _g(bar)
        self.assertTrue(
            evaluate_predicate(pred, bar=bar, g=g, prev_geom=prev, recent=[bar])
        )

    def test_watch_match_wakes_once_on_false_to_true_edge(self):
        state = ThesisWakeState(
            watch_predicates=[{"type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"}]
        )
        prev_bar = _Bar(9.7, 9.95, 9.6, 9.85)
        bar = _Bar(9.9, 10.2, 9.7, 10.05)
        prev = {"direction": "BULLISH", "range": 0.3, "high": 9.95, "low": 9.6}
        g = _g(bar)
        recent = [prev_bar, bar]
        d1 = evaluate_wake(
            state, bar_index=1, bar=bar, g=g, prev_geom=prev, recent=recent, prev_bar=prev_bar
        )
        self.assertIsNotNone(d1)
        self.assertEqual(d1.reason, WAKE_WATCH)
        d2 = evaluate_wake(
            state, bar_index=2, bar=bar, g=g, prev_geom=prev, recent=recent, prev_bar=prev_bar
        )
        self.assertIsNone(d2)

    def test_invalidation_wakes_once(self):
        state = ThesisWakeState(
            invalidation_predicates=[{"type": "BREAK_BELOW_LEVEL", "level": 10.0}]
        )
        bar = _Bar(10.1, 10.2, 9.8, 9.85)
        prev_bar = _Bar(10.2, 10.3, 10.0, 10.15)
        prev = {"direction": "BEARISH", "range": 0.4, "high": 10.2, "low": 9.9}
        g = _g(bar)
        d1 = evaluate_wake(
            state, bar_index=1, bar=bar, g=g, prev_geom=prev, recent=[prev_bar, bar], prev_bar=prev_bar
        )
        self.assertEqual(d1.reason, WAKE_INVALIDATED)
        self.assertIn("closed below", d1.detail.lower())
        self.assertNotIn("invalidation predicates:", d1.detail.lower())
        d2 = evaluate_wake(
            state, bar_index=2, bar=bar, g=g, prev_geom=prev, recent=[bar], prev_bar=prev_bar
        )
        self.assertIsNone(d2)
        self.assertGreaterEqual(state.invalidation_blocked_already_fired, 1)

    def test_safety_refresh_after_quiet_period(self):
        state = ThesisWakeState(bars_since_meaningful_wake=10)
        bar = _Bar(10, 10.1, 9.9, 10.0)
        prev = {"direction": "NEUTRAL", "range": 0.2, "high": 10.0, "low": 9.9}
        g = _g(bar)
        d = evaluate_wake(state, bar_index=5, bar=bar, g=g, prev_geom=prev, recent=[bar])
        self.assertEqual(d.reason, WAKE_SAFETY)

    def test_no_lookahead_uses_only_current_bar(self):
        pred = {"type": "LEVEL_BREAK", "level": 50.0, "direction": "ABOVE"}
        bar = _Bar(10, 10.5, 9.5, 10.2)
        future = _Bar(50, 51, 49, 50.5)
        g = _g(bar)
        self.assertFalse(
            evaluate_predicate(pred, bar=bar, g=g, prev_geom=None, recent=[bar, future])
        )


class AdviserPaaPolicyTests(unittest.TestCase):
    def test_frozen_paa_confirmation_detected(self):
        self.assertTrue(is_frozen_daily_confirmation("Daily PAA shifts to CLEAR_LONG"))
        self.assertTrue(is_frozen_daily_confirmation("PAA must become CLEAR_LONG"))

    def test_sanitize_removes_frozen_confirmations(self):
        kept, removed = sanitize_confirmation_strings(
            [
                "Second entry confirms with bull signal bar",
                "Daily PAA shifts to CLEAR_LONG",
            ]
        )
        self.assertEqual(len(kept), 1)
        self.assertEqual(len(removed), 1)

    def test_daily_bias_visible_without_blocking(self):
        note = daily_bias_note("NO_CLEAR_LONG")
        self.assertTrue(note["daily_is_cautious_for_longs"])
        self.assertIn("Daily context is cautious", note["intraday_requirement_hint"])

    def test_no_short_action(self):
        self.assertEqual(assert_no_short_action("ARM_SHORT"), "OBSERVE")
        self.assertEqual(assert_no_short_action("WATCH_LONG"), "WATCH_LONG")


if __name__ == "__main__":
    unittest.main()

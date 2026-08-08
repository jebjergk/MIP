"""Tests for strict invalidation predicate processing (POC 3)."""

from __future__ import annotations

import unittest

from app.brooks_intraday.adviser_invalidation_v01 import (
    evaluate_invalidation_edge,
    predicate_is_future_actionable,
    process_invalidation_predicates,
)
from app.brooks_intraday.adviser_paa_policy_v01 import is_frozen_daily_confirmation


class _Bar:
    def __init__(self, close: float):
        self.close = close
        self.open = close
        self.high = close
        self.low = close


class InvalidationStrictTests(unittest.TestCase):
    def test_reject_already_true_below_level(self):
        bar = _Bar(247.0)
        raw = [{"type": "BREAK_BELOW_LEVEL", "level": 248.27}]
        self.assertFalse(predicate_is_future_actionable(bar, raw[0]))
        stored, stats = process_invalidation_predicates(raw, bar)
        self.assertEqual(len(stored), 0)
        self.assertEqual(stats.rejected_already_true, 1)

    def test_edge_trigger_once(self):
        pred = {"type": "BREAK_BELOW_LEVEL", "level": 246.0}
        prev = _Bar(246.5)
        cur = _Bar(245.8)
        self.assertTrue(evaluate_invalidation_edge(pred, bar=cur, prev_bar=prev))
        self.assertFalse(evaluate_invalidation_edge(pred, bar=_Bar(245.5), prev_bar=cur))

    def test_cap_two_predicates(self):
        bar = _Bar(250.0)
        raw = [
            {"type": "BREAK_BELOW_LEVEL", "level": 248.0},
            {"type": "BREAK_BELOW_LEVEL", "level": 247.0},
            {"type": "BREAK_BELOW_LEVEL", "level": 246.0},
        ]
        stored, stats = process_invalidation_predicates(raw, bar)
        self.assertLessEqual(len(stored), 2)
        self.assertGreater(stats.rejected_cap + stats.rejected_dedupe, 0)

    def test_paa_clear_long_confirmation_blocked(self):
        self.assertTrue(is_frozen_daily_confirmation("WAIT_PULLBACK becomes CLEAR_LONG"))
        self.assertTrue(is_frozen_daily_confirmation("Confirmation: PAA shifts to bullish"))


if __name__ == "__main__":
    unittest.main()

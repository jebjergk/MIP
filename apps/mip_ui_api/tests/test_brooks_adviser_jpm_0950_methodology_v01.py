"""Methodology correction tests — strong bull trend context (JPM 09:50 fixture)."""

from __future__ import annotations

import re
import unittest

from app.brooks_intraday.adviser_jpm_0950_fixture_v01 import (
    BREAKOUT_REFERENCE,
    jpm_0950_invalidation_scenario,
)
from app.brooks_intraday.adviser_methodology_guard_v01 import (
    guard_premature_failed_breakout_market_state,
)
from app.brooks_intraday.adviser_regime_v01 import REGIME_STRONG_BULL_TREND, update_intraday_regime
from app.brooks_intraday.adviser_retrieval_query_v01 import (
    build_retrieval_query,
    sanitize_thesis_for_retrieval,
)
from app.brooks_intraday.adviser_wake_v01 import WAKE_INVALIDATED
from app.brooks_intraday.objective_ruleset_v01 import compute_geometry


class _Bar:
    def __init__(self, o, h, l, c, v=1.0):
        self.open, self.high, self.low, self.close, self.volume = o, h, l, c, v


class MethodologyRetrievalTests(unittest.TestCase):
    def test_sanitize_strips_failure_echo_phrases(self):
        raw = "Pullback holds; failed bull breakout and bull trap; bear pause noted."
        clean = sanitize_thesis_for_retrieval(raw)
        self.assertNotRegex(clean, re.compile(r"failed\s+bull\s+breakout", re.I))
        self.assertNotRegex(clean, re.compile(r"bull\s+trap", re.I))

    def test_build_query_dominates_regime_not_failure_labels(self):
        q = build_retrieval_query(
            wake_reason=WAKE_INVALIDATED,
            bar_direction="BEARISH",
            bar_close=339.58,
            position_state="IN_TRADE",
            intraday_regime=REGIME_STRONG_BULL_TREND,
            wake_detail=f"Breakout reference {BREAKOUT_REFERENCE:.2f} was closed below (close 339.58).",
            thesis_context="failed bull breakout bull trap",
        )
        self.assertIn("regime=STRONG_BULL_TREND", q)
        self.assertNotRegex(q.lower(), r"thesis:\s*failed bull breakout")
        self.assertNotIn("failed bull breakout", q.lower().split("prior context")[0])


class MethodologyGuardTests(unittest.TestCase):
    def test_level_break_only_blocks_premature_failed_breakout_label(self):
        detail = f"Breakout reference {BREAKOUT_REFERENCE:.2f} was closed below (close 339.58)."
        state, note = guard_premature_failed_breakout_market_state(
            wake_reason=WAKE_INVALIDATED,
            wake_detail=detail,
            market_state="FAILED_BULL_BREAKOUT_IN_PROGRESS",
        )
        self.assertEqual(state, "BREAKOUT_REFERENCE_TEST_REASSESSMENT")
        self.assertIsNotNone(note)

    def test_guard_does_not_apply_without_invalidation_wake(self):
        state, note = guard_premature_failed_breakout_market_state(
            wake_reason="POSITION_EVENT",
            wake_detail="Breakout reference 339.99 was closed below.",
            market_state="FAILED_BULL_BREAKOUT",
        )
        self.assertEqual(state, "FAILED_BULL_BREAKOUT")
        self.assertIsNone(note)


class Jpm0950FixtureTests(unittest.TestCase):
    def test_fixture_objective_wake_not_failed_breakout_diagnosis(self):
        fx = jpm_0950_invalidation_scenario()
        self.assertEqual(fx["wake_reason"], WAKE_INVALIDATED)
        detail = fx["wake_detail"].lower()
        self.assertIn("339.99", detail)
        self.assertIn("closed below", detail)
        self.assertNotIn("failed_bull_breakout", detail.replace(" ", "_"))
        self.assertNotIn("invalidation predicates:", detail)

    def test_fixture_retrieval_allows_competing_brooks_context(self):
        fx = jpm_0950_invalidation_scenario()
        q = fx["retrieval_query"].lower()
        self.assertIn("strong_bull_trend", q.replace("-", "_"))
        self.assertNotRegex(q, r"^brooks intraday long-only context: thesis: failed")

    def test_fixture_does_not_encode_hold_or_exit(self):
        fx = jpm_0950_invalidation_scenario()
        self.assertNotIn("action", fx)
        self.assertNotIn("expected_action", fx)

    def test_fixture_guard_replaces_auto_failed_breakout_state(self):
        fx = jpm_0950_invalidation_scenario()
        self.assertEqual(fx["guarded_market_state"], "BREAKOUT_REFERENCE_TEST_REASSESSMENT")

    def test_regime_persists_structured_labels(self):
        bar = _Bar(340, 342, 339, 341.5)
        g = compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=1)
        from app.brooks_intraday.adviser_regime_v01 import IntradayRegimeState

        st = IntradayRegimeState()
        recent = [_Bar(337, 338, 336, 337.5), bar]
        update_intraday_regime(st, bar=bar, g=g, recent=recent, bar_index=5)
        self.assertIn(st.regime, {"STRONG_BULL_TREND", "TREND_FROM_OPEN", "BULL_CHANNEL", "TRANSITION", "NEUTRAL"})


if __name__ == "__main__":
    unittest.main()

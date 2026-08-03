"""Phase 9 — unseen-week validation tests."""

from __future__ import annotations

import unittest
from datetime import date

from app.brooks_intraday.experiment_freeze import FREEZE_ID, build_freeze_record
from app.brooks_intraday.experiment_week_selection import (
    LOCKED_VALIDATION_WEEKS,
    build_week_selection_rationale,
    evaluate_week_candidate,
)
from app.brooks_intraday.experiment_comparison import _conversion_metrics
from app.brooks_intraday.learning_constants import PILOT_RUN_ID


class Phase9FreezeTests(unittest.TestCase):
    def test_freeze_id(self):
        rec = build_freeze_record()
        self.assertEqual(rec["freeze_id"], FREEZE_ID)
        self.assertEqual(rec["ruleset_chain"]["objective_ruleset"], "BROOKS_OBJECTIVE_RULESET_V0_1")
        self.assertEqual(rec["ruleset_chain"]["pattern_ruleset"], "BROOKS_PATTERN_RULESET_V0_3")
        self.assertEqual(rec["ruleset_chain"]["context_ruleset"], "BROOKS_CONTEXT_RULESET_V0_2")
        self.assertEqual(rec["ruleset_chain"]["simulation_ruleset"], "BROOKS_SIMULATION_RULESET_V0_1")

    def test_baseline_not_modified_constant(self):
        rec = build_freeze_record()
        self.assertEqual(rec["baseline_run_id"], PILOT_RUN_ID)


class Phase9WeekSelectionTests(unittest.TestCase):
    def test_at_least_three_locked_weeks(self):
        self.assertGreaterEqual(len(LOCKED_VALIDATION_WEEKS), 3)

    def test_all_locked_weeks_calendar_ready(self):
        for ws in LOCKED_VALIDATION_WEEKS:
            wc = evaluate_week_candidate(ws)
            self.assertEqual(wc.calendar_status, "READY", msg=str(ws))
            self.assertEqual(len(wc.trading_dates), 5)

    def test_weeks_before_baseline(self):
        baseline = date(2026, 7, 20)
        for ws in LOCKED_VALIDATION_WEEKS:
            self.assertLess(ws, baseline)

    def test_rationale_outcome_blind(self):
        doc = build_week_selection_rationale()
        self.assertTrue(doc.get("outcome_blind_lock"))
        self.assertEqual(len(doc.get("locked_validation_weeks") or []), 3)


class Phase9ComparisonTests(unittest.TestCase):
    def test_conversion_metrics_empty(self):
        m = _conversion_metrics([])
        self.assertIsNone(m["no_trade_week_pct"])


if __name__ == "__main__":
    unittest.main()

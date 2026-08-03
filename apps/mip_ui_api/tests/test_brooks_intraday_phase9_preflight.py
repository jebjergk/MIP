"""Phase 9 — IB preflight and staged acquisition helpers."""

from __future__ import annotations

import unittest
from datetime import date

from app.brooks_intraday.experiment_ib_preflight import (
    PHASE2B_REFERENCE,
    compare_phase2b_phase9_config,
)
from app.brooks_intraday.experiment_week_selection import LOCKED_VALIDATION_WEEKS
from app.brooks_intraday.interval_validation import expected_interval_starts_ny as exp_ny


class Phase9IbConfigTests(unittest.TestCase):
    def test_first_validation_week_is_probe_week(self):
        self.assertEqual(LOCKED_VALIDATION_WEEKS[0], date(2026, 7, 13))

    def test_expected_78_intervals(self):
        starts = exp_ny(date(2026, 7, 13))
        self.assertEqual(len(starts), 78)
        self.assertEqual(starts[0].strftime("%H:%M"), "09:30")
        self.assertEqual(starts[-1].strftime("%H:%M"), "15:55")

    def test_config_comparison_structure(self):
        cmp = compare_phase2b_phase9_config()
        self.assertIn("phase9_effective", cmp)
        self.assertEqual(PHASE2B_REFERENCE["use_rth"], True)


if __name__ == "__main__":
    unittest.main()

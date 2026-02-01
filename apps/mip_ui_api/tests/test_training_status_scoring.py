"""
Pure-Python unit tests for Training Status v1 scoring. No Snowflake needed.
"""
import unittest
import sys
from pathlib import Path

# Add app to path so we can import training_status
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.training_status import (
    compute_maturity_score,
    get_maturity_stage,
    score_training_status_row,
    apply_scoring_to_rows,
    DEFAULT_MIN_SIGNALS,
    MAX_HORIZONS,
    POINTS_SAMPLE,
    POINTS_COVERAGE,
    POINTS_HORIZONS,
)


class TestComputeMaturityScore(unittest.TestCase):
    def test_zero_recs(self):
        s_sample, s_cov, s_hor = compute_maturity_score(0, 0, 0)
        self.assertEqual(s_sample, 0.0)
        self.assertEqual(s_cov, 0.0)
        self.assertEqual(s_hor, 0.0)

    def test_full_sample_cap(self):
        s_sample, s_cov, s_hor = compute_maturity_score(DEFAULT_MIN_SIGNALS, 0, 0, DEFAULT_MIN_SIGNALS)
        self.assertAlmostEqual(s_sample, POINTS_SAMPLE)
        self.assertEqual(s_cov, 0.0)

    def test_over_cap_sample(self):
        s_sample, s_cov, s_hor = compute_maturity_score(100, 0, 0, DEFAULT_MIN_SIGNALS)
        self.assertAlmostEqual(s_sample, POINTS_SAMPLE)

    def test_coverage_full(self):
        recs = 10
        outcomes = recs * MAX_HORIZONS
        s_sample, s_cov, s_hor = compute_maturity_score(recs, outcomes, MAX_HORIZONS)
        self.assertAlmostEqual(s_cov, POINTS_COVERAGE)
        self.assertAlmostEqual(s_hor, POINTS_HORIZONS)

    def test_horizons_partial(self):
        s_sample, s_cov, s_hor = compute_maturity_score(10, 20, 3)
        self.assertAlmostEqual(s_hor, POINTS_HORIZONS * (3 / MAX_HORIZONS))


class TestGetMaturityStage(unittest.TestCase):
    def test_insufficient(self):
        self.assertEqual(get_maturity_stage(0), "INSUFFICIENT")
        self.assertEqual(get_maturity_stage(24.9), "INSUFFICIENT")

    def test_warming_up(self):
        self.assertEqual(get_maturity_stage(25), "WARMING_UP")
        self.assertEqual(get_maturity_stage(49.9), "WARMING_UP")

    def test_learning(self):
        self.assertEqual(get_maturity_stage(50), "LEARNING")
        self.assertEqual(get_maturity_stage(74.9), "LEARNING")

    def test_confident(self):
        self.assertEqual(get_maturity_stage(75), "CONFIDENT")
        self.assertEqual(get_maturity_stage(100), "CONFIDENT")


class TestScoreTrainingStatusRow(unittest.TestCase):
    def test_zero_row(self):
        r = score_training_status_row(0, 0, 0)
        self.assertEqual(r.maturity_score, 0.0)
        self.assertEqual(r.maturity_stage, "INSUFFICIENT")
        self.assertIsInstance(r.reasons, list)
        self.assertGreater(len(r.reasons), 0)

    def test_full_row(self):
        recs = DEFAULT_MIN_SIGNALS
        outcomes = recs * MAX_HORIZONS
        r = score_training_status_row(recs, outcomes, MAX_HORIZONS)
        self.assertAlmostEqual(r.maturity_score, 100.0)
        self.assertEqual(r.maturity_stage, "CONFIDENT")
        self.assertIn("strong data coverage", r.reasons[-1].lower() or r.reasons[-1])

    def test_mid_score(self):
        r = score_training_status_row(20, 30, 2, min_signals=40)
        self.assertGreaterEqual(r.maturity_score, 0)
        self.assertLessEqual(r.maturity_score, 100)
        self.assertIn(r.maturity_stage, ("INSUFFICIENT", "WARMING_UP", "LEARNING", "CONFIDENT"))


class TestApplyScoringToRows(unittest.TestCase):
    def test_empty(self):
        out = apply_scoring_to_rows([])
        self.assertEqual(out, [])

    def test_one_row(self):
        rows = [{"recs_total": 10, "outcomes_total": 20, "horizons_covered": 2}]
        out = apply_scoring_to_rows(rows)
        self.assertEqual(len(out), 1)
        self.assertIn("maturity_score", out[0])
        self.assertIn("maturity_stage", out[0])
        self.assertIn("reasons", out[0])
        self.assertIn("recs_total", out[0])

    def test_uppercase_keys(self):
        rows = [{"RECS_TOTAL": 5, "OUTCOMES_TOTAL": 10, "HORIZONS_COVERED": 1}]
        out = apply_scoring_to_rows(rows)
        self.assertEqual(len(out), 1)
        self.assertIn("maturity_score", out[0])


if __name__ == "__main__":
    unittest.main()

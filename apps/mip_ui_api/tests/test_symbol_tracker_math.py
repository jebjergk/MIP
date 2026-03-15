import unittest

from app.routers.symbol_tracker import (
    _build_projection_path,
    _build_stitched_projection_path,
    _thesis_status,
    _volatility_label,
)


class SymbolTrackerMathTests(unittest.TestCase):
    def test_projection_path_long_moves_up_for_positive_avg_return(self):
        out = _build_projection_path(
            baseline_price=100.0,
            avg_return=0.10,
            upper_return=0.15,
            lower_return=0.05,
            horizon_bars=5,
            side="LONG",
        )
        self.assertEqual(len(out["center_path"]), 5)
        self.assertGreater(out["center_path"][-1]["price"], 100.0)
        self.assertGreater(out["upper_path"][-1]["price"], out["center_path"][-1]["price"])
        self.assertLess(out["lower_path"][-1]["price"], out["center_path"][-1]["price"])

    def test_projection_path_short_moves_down_for_positive_avg_return(self):
        out = _build_projection_path(
            baseline_price=100.0,
            avg_return=0.10,
            upper_return=0.15,
            lower_return=0.05,
            horizon_bars=5,
            side="SHORT",
        )
        self.assertEqual(len(out["center_path"]), 5)
        self.assertLess(out["center_path"][-1]["price"], 100.0)
        self.assertLess(out["upper_path"][-1]["price"], out["center_path"][-1]["price"])
        self.assertGreater(out["lower_path"][-1]["price"], out["center_path"][-1]["price"])

    def test_projection_path_uses_geometric_curve(self):
        out = _build_projection_path(
            baseline_price=100.0,
            avg_return=0.21,
            upper_return=0.30,
            lower_return=0.10,
            horizon_bars=2,
            side="LONG",
            projection_mode="geometric",
        )
        # 21% over 2 steps => sqrt(1.21)=1.1 so first step should be exactly 110, not linear 110.5
        self.assertAlmostEqual(out["center_path"][0]["price"], 110.0, places=6)
        self.assertAlmostEqual(out["center_path"][-1]["price"], 121.0, places=6)

    def test_projection_path_linear_mode(self):
        out = _build_projection_path(
            baseline_price=100.0,
            avg_return=0.21,
            upper_return=0.30,
            lower_return=0.10,
            horizon_bars=2,
            side="LONG",
            projection_mode="linear",
        )
        self.assertAlmostEqual(out["center_path"][0]["price"], 110.5, places=6)
        self.assertAlmostEqual(out["center_path"][-1]["price"], 121.0, places=6)

    def test_stitched_projection_uses_horizon_profile(self):
        horizon_stats = {
            1: {"avg_return": 0.01, "sample_size": 40, "stddev_return": 0.02, "p10_return": -0.01, "p90_return": 0.03},
            5: {"avg_return": 0.05, "sample_size": 40, "stddev_return": 0.03, "p10_return": -0.02, "p90_return": 0.08},
            10: {"avg_return": 0.02, "sample_size": 40, "stddev_return": 0.04, "p10_return": -0.05, "p90_return": 0.06},
        }
        out = _build_stitched_projection_path(
            baseline_price=100.0,
            side="LONG",
            horizon_bars=10,
            horizon_stats=horizon_stats,
        )
        self.assertEqual(len(out["center_path"]), 10)
        # With a stitched profile, intermediate steps should not equal simple linear 0->10 target.
        linear_step_3 = 100.0 + ((102.0 - 100.0) * 3 / 10.0)
        self.assertNotAlmostEqual(out["center_path"][2]["price"], linear_step_3, places=6)
        self.assertAlmostEqual(out["center_path"][-1]["price"], 102.0, places=6)

    def test_thesis_invalidates_when_stop_loss_crossed(self):
        long_state = _thesis_status(
            side="LONG",
            entry_price=100.0,
            current_price=95.0,
            sl_price=96.0,
            expectation_center_end=102.0,
            expectation_upper_end=105.0,
            expectation_lower_end=99.0,
        )
        short_state = _thesis_status(
            side="SHORT",
            entry_price=100.0,
            current_price=105.0,
            sl_price=104.0,
            expectation_center_end=98.0,
            expectation_upper_end=100.0,
            expectation_lower_end=95.0,
        )
        self.assertEqual(long_state["status"], "INVALIDATED")
        self.assertEqual(short_state["status"], "INVALIDATED")

    def test_volatility_label_thresholds(self):
        self.assertEqual(
            _volatility_label(0.005, 0.01),
            "LIVE_VOL_BELOW_TRAINED_REGIME",
        )
        self.assertEqual(
            _volatility_label(0.01, 0.01),
            "LIVE_VOL_ALIGNED",
        )
        self.assertEqual(
            _volatility_label(0.02, 0.01),
            "LIVE_VOL_ABOVE_TRAINED_REGIME",
        )


if __name__ == "__main__":
    unittest.main()

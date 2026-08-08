"""Wake contract patch 1: unified predicate eval + edge-trigger fixtures."""

from __future__ import annotations

import unittest

from app.brooks_intraday.adviser_baseline_v01 import WAKE_CONTRACT_PATCH
from app.brooks_intraday.adviser_predicate_eval_v01 import (
    BROOKS_PREDICATE_STATE_TYPES,
    infer_watch_validity_window,
)
from app.brooks_intraday.adviser_setup_contract_v01 import evaluate_mandatory_predicate
from app.brooks_intraday.adviser_wake_contract_fixture_v01 import (
    AMZN_BARS_0930_1025,
    AMZN_FT_PREDICATE_25291,
    MCD_FT_PREDICATE_26659,
    amzn_20260715_ft_replay,
    mcd_20260715_ft_replay,
)
from app.brooks_intraday.adviser_wake_v01 import parse_predicates_from_llm
from app.brooks_intraday.adviser_watch_v01 import (
    evaluate_watch_edge,
    normalize_operational_watch_conditions,
)
from app.brooks_intraday.objective_ruleset_v01 import compute_geometry


class WakeContractPatchTests(unittest.TestCase):
    def test_patch_identifier(self):
        self.assertEqual(WAKE_CONTRACT_PATCH, 1)

    def test_brooks_types_include_forensic_gap_types(self):
        for ptype in (
            "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
            "SIGNAL_BAR_CONFIRMED",
            "BREAKOUT_PULLBACK_HOLD_CONFIRMED",
            "FAILED_BEAR_BREAKOUT_CONFIRMED",
        ):
            self.assertIn(ptype, BROOKS_PREDICATE_STATE_TYPES)

    def test_mandatory_and_watch_edge_agree_on_ft_bar(self):
        bar = AMZN_BARS_0930_1025[6]
        prev = AMZN_BARS_0930_1025[5]
        recent = AMZN_BARS_0930_1025[:7]
        g = compute_geometry(
            open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume
        )
        gp = compute_geometry(
            open_=prev.open,
            high=prev.high,
            low=prev.low,
            close=prev.close,
            volume=prev.volume,
        )
        pg = {"direction": gp.direction, "range": gp.total_range, "high": prev.high, "low": prev.low}
        pred = AMZN_FT_PREDICATE_25291
        self.assertTrue(
            evaluate_mandatory_predicate(
                pred,
                bar=bar,
                bar_index=6,
                recent=recent,
                prev_bar=prev,
                prev_geom=pg,
                g=g,
            )
        )
        self.assertTrue(
            evaluate_watch_edge(
                pred,
                bar=bar,
                bar_index=6,
                prev_bar=prev,
                prev_geom=pg,
                recent=recent,
            )
        )

    def test_amzn_fixture_first_wake_on_ft_not_safety(self):
        result = amzn_20260715_ft_replay()
        self.assertEqual(result["watch_wake_times_et"], ["10:00"])
        self.assertNotIn("10:25", result["watch_wake_times_et"])
        self.assertEqual(result["safety_wake_times_et"], [])

    def test_mcd_fixture_single_watch_edge(self):
        result = mcd_20260715_ft_replay()
        self.assertEqual(result["watch_wake_times_et"], ["09:50"])
        self.assertGreaterEqual(result["watch_blocked_already_fired"], 0)

    def test_prose_only_watch_not_inferred(self):
        parsed = {
            "watch_conditions": ["Wake me if price breaks 300"],
            "watch_predicates": [],
        }
        watch_p, _, _ = parse_predicates_from_llm(parsed)
        self.assertEqual(watch_p, [])

    def test_operational_watch_prose_pairs_with_predicates(self):
        preds = [AMZN_FT_PREDICATE_25291, MCD_FT_PREDICATE_26659]
        lines = ["line one", "line two", "extra prose only"]
        op, notes = normalize_operational_watch_conditions(lines, preds)
        self.assertEqual(len(op), 2)
        self.assertEqual(op[0], "line one")
        self.assertEqual(notes, ["extra prose only"])

    def test_watch_validity_extends_through_quiet_window(self):
        pred = {
            "valid_from_bar_offset": 1,
            "valid_until_bar_offset": 3,
        }
        vf, vu = infer_watch_validity_window(pred, watch_issued_at_bar=0, safety_quiet_bars=10)
        self.assertEqual(vf, 1)
        self.assertGreaterEqual(vu, 9)


if __name__ == "__main__":
    unittest.main()

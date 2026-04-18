import unittest

from app.committee.engine import (
    LiveContext,
    cap_stance_worse,
    compute_hearing_bundle,
    invalidation_breached,
)


class Committee2EngineTests(unittest.TestCase):
    def test_invalidation_breach_long(self):
        self.assertTrue(invalidation_breached("LONG", 99.0, 100.0))
        self.assertFalse(invalidation_breached("LONG", 101.0, 100.0))

    def test_cap_stance_worse(self):
        self.assertEqual(cap_stance_worse("APPROVE", "DEFER"), "DEFER")
        self.assertEqual(cap_stance_worse("DEFER", "DENY"), "DENY")

    def test_bundle_denies_on_breach(self):
        snap = {
            "PROPOSAL_ID": 1,
            "SNAPSHOT_ID": 10,
            "SYMBOL": "TEST",
            "SIDE": "LONG",
            "SETUP_FAMILY": "TREND_PULLBACK_LONG",
            "STRUCTURAL_STATE": "TREND_UP",
            "REGIME_STATE": "GOOD",
            "TRUST_LABEL": "TRUSTED",
            "ENTRY_ZONE_JSON": {"low": 98.0, "high": 102.0},
            "INVALIDATION_JSON": {"level": 97.0, "rule": "BELOW"},
            "PATH_METRICS_JSON": {"pct_adverse_before_favorable": 0.35, "meaningful_hit_rate": 0.5},
            "MFE_MAE_JSON": {},
            "TRAILING_STYLE": "STRUCTURAL",
        }
        live = LiveContext(
            latest_price=96.0,
            open_price=96.5,
            prior_close=95.0,
            structural_state_now="TREND_UP",
            trend_regime_now="UPTREND",
            vol_regime_now="NORMAL",
            bar_dates=["2026-04-10"],
        )
        out = compute_hearing_bundle(snap, live)
        self.assertEqual(out["stance"], "DENY")
        self.assertGreaterEqual(out["confidence"], 0.5)

    def test_confidence_differs_with_path_and_geometry(self):
        base_snap = {
            "PROPOSAL_ID": 1,
            "SNAPSHOT_ID": 10,
            "SYMBOL": "TEST",
            "SIDE": "LONG",
            "SETUP_FAMILY": "TREND_PULLBACK_LONG",
            "STRUCTURAL_STATE": "TREND_UP",
            "REGIME_STATE": "GOOD",
            "TRUST_LABEL": "TRUSTED",
            "ENTRY_ZONE_JSON": {"low": 98.0, "high": 102.0},
            "INVALIDATION_JSON": {"level": 90.0, "rule": "BELOW"},
            "PATH_METRICS_JSON": {"pct_adverse_before_favorable": 0.30, "meaningful_hit_rate": 0.50},
            "MFE_MAE_JSON": {},
            "TRAILING_STYLE": "STRUCTURAL",
        }
        live_a = LiveContext(
            latest_price=100.0,
            open_price=99.0,
            prior_close=98.0,
            structural_state_now="TREND_UP",
            trend_regime_now="UPTREND",
            vol_regime_now="NORMAL",
            bar_dates=["2026-04-10"],
            recent_bar_trace=[
                {"bar_date": "2026-04-08", "close": 97.0},
                {"bar_date": "2026-04-09", "close": 99.0},
                {"bar_date": "2026-04-10", "close": 100.0},
            ],
        )
        out_a = compute_hearing_bundle(base_snap, live_a)
        snap_b = {**base_snap, "PATH_METRICS_JSON": {"pct_adverse_before_favorable": 0.50, "meaningful_hit_rate": 0.30}}
        out_b = compute_hearing_bundle(snap_b, live_a)
        self.assertGreater(abs(out_a["confidence"] - out_b["confidence"]), 0.02)
        self.assertIn("Adverse-before-favorable", " ".join(out_b["chair"]["top_tensions"]))
        ev = out_a["evidence"]
        self.assertIn("recent_bar_trace", ev)
        self.assertEqual(ev.get("regime_continuity"), "ALIGNED")


if __name__ == "__main__":
    unittest.main()

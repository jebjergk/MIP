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


if __name__ == "__main__":
    unittest.main()

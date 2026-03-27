import unittest

from app.services.live_intelligence.ai_committee import run_ai_enrichment
from app.services.live_intelligence.engine import run_deterministic_step
from app.services.live_intelligence.portfolio_regime import detect_portfolio_regime


def _tile(**kwargs):
    base = {
        "symbol": "TEST",
        "side": "LONG",
        "quantity": 10,
        "entry_price": 100.0,
        "current_price": 102.0,
        "unrealized_pnl": 20.0,
        "chart": {
            "interval_minutes": 60,
            "bars": [
                {"ts": "2025-01-01T10:00:00", "close": 99.0, "open": 99, "high": 100, "low": 98},
                {"ts": "2025-01-01T11:00:00", "close": 102.0, "open": 101, "high": 103, "low": 100},
            ],
        },
        "overlays": {"stop_loss": 95.0, "take_profit": 110.0, "entry": 100.0, "current": 102.0},
        "expectation": {"center_path": [{"step": 1, "price": 101}], "lower_path": [{"step": 1, "price": 98}], "upper_path": [{"step": 1, "price": 104}]},
        "thesis": {"status": "THESIS_INTACT", "reason": "ok"},
        "progress_metrics": {"distance_to_sl_pct": 0.07, "distance_to_tp_pct": 0.08, "expected_progress_pct": 0.4},
        "volatility_context": {"status": "LIVE_VOL_ALIGNED", "live_volatility": 0.01, "trained_volatility": 0.012},
        "events": [],
    }
    base.update(kwargs)
    return base


class LiveIntelligenceEngineTests(unittest.TestCase):
    def test_first_cycle_material_feed(self):
        body = {"positions": [_tile()], "prior_intelligence": {}}
        out = run_deterministic_step(body)
        self.assertIn("TEST", out["intelligence_by_symbol"])
        self.assertTrue(out["feed_events"])

    def test_exit_now_near_stop(self):
        t = _tile(
            current_price=95.5,
            overlays={"stop_loss": 95.0, "take_profit": 110.0},
            thesis={"status": "WEAKENING"},
        )
        out = run_deterministic_step({"positions": [t], "prior_intelligence": {}})
        intel = out["intelligence_by_symbol"]["TEST"]
        self.assertIn(intel["exit_urgency"], {"EXIT_NOW", "PREPARE"})

    def test_portfolio_regime_active_when_most_underwater(self):
        positions = [
            {"symbol": "A", "unrealized_pnl": -10, "chart": {"bars": [{"ts": "t1", "close": 1}, {"ts": "t2", "close": 1.01}]}},
            {"symbol": "B", "unrealized_pnl": -20, "chart": {"bars": [{"ts": "t1", "close": 2}, {"ts": "t2", "close": 2.01}]}},
        ]
        pw = {"A": {"B": 0.9}}
        r = detect_portfolio_regime(positions, pairwise_correlation=pw)
        self.assertTrue(r.get("active"))

    def test_ai_cooldown_skips(self):
        out = run_ai_enrichment(
            {"symbol": "TEST", "intelligence": {}, "deterministic_snapshot": {}, "force": False},
            now_ts=1000.0,
            last_ai_by_symbol={"TEST": 999.0},
        )
        self.assertTrue(out.get("skipped"))


if __name__ == "__main__":
    unittest.main()

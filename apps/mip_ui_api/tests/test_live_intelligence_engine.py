import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.services.live_intelligence.ai_committee import run_ai_enrichment
from app.services.live_intelligence import engine as lic_engine
from app.services.live_intelligence.engine import run_deterministic_step
from app.services.live_intelligence.resolver import (
    build_case_file_signature,
    case_file_event_material_override,
    seconds_between_iso,
)
from app.services.live_intelligence.portfolio_regime import detect_portfolio_regime
from app.services.live_intelligence import lic_display


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
    def test_first_cycle_intel_without_feed_rows(self):
        body = {"positions": [_tile()], "prior_intelligence": {}}
        out = run_deterministic_step(body)
        self.assertIn("TEST", out["intelligence_by_symbol"])
        self.assertEqual(out["feed_events"], [])
        intel = out["intelligence_by_symbol"]["TEST"]
        self.assertIn("final_recommendation", intel)
        self.assertIn("feed_fingerprint", intel)

    def test_identical_refresh_does_not_emit_feed(self):
        o1 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": {}})
        prior = o1["intelligence_by_symbol"]
        o2 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": prior})
        self.assertEqual(o2["feed_events"], [])

    def test_thesis_break_emits_feed_when_prior_present(self):
        o1 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": {}})
        prior = o1["intelligence_by_symbol"]
        t2 = _tile(thesis={"status": "INVALIDATED", "reason": "x"})
        o2 = run_deterministic_step({"positions": [t2], "prior_intelligence": prior})
        self.assertTrue(len(o2["feed_events"]) >= 1)
        self.assertEqual(o2["feed_events"][0]["symbol"], "TEST")

    def test_exit_now_near_stop(self):
        t = _tile(
            current_price=95.5,
            overlays={"stop_loss": 95.0, "take_profit": 110.0},
            thesis={"status": "WEAKENING"},
        )
        out = run_deterministic_step({"positions": [t], "prior_intelligence": {}})
        intel = out["intelligence_by_symbol"]["TEST"]
        self.assertIn(intel["exit_urgency"], {"EXIT_NOW", "PREPARE"})
        self.assertIn(intel["final_recommendation"], {"EXIT_NOW", "PREPARE_EXIT"})

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

    def test_recommendation_headline_vocabulary(self):
        self.assertEqual(lic_display.recommendation_headline("STAY_COURSE"), "HOLD")
        self.assertEqual(lic_display.recommendation_headline("EXIT_NOW"), "EXIT OR CUT NOW")

    def test_decision_drivers_no_bottom_line(self):
        body = {"positions": [_tile()], "prior_intelligence": {}}
        out = run_deterministic_step(body)
        intel = out["intelligence_by_symbol"]["TEST"]
        drivers = intel.get("decision_drivers") or []
        joined = " ".join(drivers)
        self.assertNotIn("Bottom line", joined)
        self.assertLessEqual(len(drivers), 4)

    def test_feed_event_has_severity_and_scope_when_emitted(self):
        o1 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": {}})
        prior = o1["intelligence_by_symbol"]
        t2 = _tile(thesis={"status": "INVALIDATED", "reason": "x"})
        o2 = run_deterministic_step({"positions": [t2], "prior_intelligence": prior})
        self.assertTrue(len(o2["feed_events"]) >= 1)
        row = o2["feed_events"][0]
        self.assertIn(row.get("severity"), {"INFO", "WATCH", "ALERT", "CRITICAL"})
        self.assertEqual(row.get("scope"), "symbol")

    def test_case_file_signature_suppresses_feed_when_posture_unchanged(self):
        """Fingerprint/noise can change while coarse posture stays the same — no duplicate case rows."""
        o1 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": {}})
        prior_full = o1["intelligence_by_symbol"]["TEST"]
        sig = prior_full.get("case_file_signature")
        self.assertTrue(sig)
        # Same snapshot, prior carries case_file_signature — no feed on refresh
        o2 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": {"TEST": prior_full}})
        self.assertEqual(o2["feed_events"], [])
        # Fingerprint + delta triggers want a row, but coarse posture unchanged — suppress duplicate case line
        hacked_prior = {
            **prior_full,
            "feed_fingerprint": "stale|fingerprint|value",
            "dominant_world_id": "__bogus__",
        }
        o3 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": {"TEST": hacked_prior}})
        self.assertEqual(
            o3["feed_events"],
            [],
            "feed row should be suppressed when case_file_signature matches prior",
        )

    def test_build_case_file_signature_stable_keys(self):
        sim = {"best_action": {"action": "exit_now", "label": "Exit now"}}
        a = build_case_file_signature(
            final_band="STAY_COURSE",
            sim=sim,
            confidence_headline=0.62,
            thesis_fracture="THESIS_INTACT",
            analog_tier="weak",
            regret_bucket="exit_favored",
            dsl_bucket="L",
            tp_room_bucket="M",
            pf_localized="symbol_specific",
        )
        b = build_case_file_signature(
            final_band="STAY_COURSE",
            sim=sim,
            confidence_headline=0.63,
            thesis_fracture="THESIS_INTACT",
            analog_tier="weak",
            regret_bucket="exit_favored",
            dsl_bucket="L",
            tp_room_bucket="M",
            pf_localized="symbol_specific",
        )
        self.assertEqual(a, b)

    def test_case_file_event_material_override(self):
        base = "STAY_COURSE|hold|M|THESIS_INTACT|L|M|weak|balanced|symbol_specific"
        regret_only = "STAY_COURSE|hold|M|THESIS_INTACT|L|M|weak|exit_favored|symbol_specific"
        self.assertTrue(case_file_event_material_override(base, regret_only))
        band_flip = "WATCH_CLOSELY|hold|M|THESIS_INTACT|L|M|weak|balanced|symbol_specific"
        self.assertTrue(case_file_event_material_override(base, band_flip))
        fb_change = "STAY_COURSE|exit_now|M|THESIS_INTACT|L|M|weak|balanced|symbol_specific"
        self.assertTrue(case_file_event_material_override(base, fb_change))
        thesis = "STAY_COURSE|hold|M|THESIS_DAMAGED|L|M|weak|balanced|symbol_specific"
        self.assertTrue(case_file_event_material_override(base, thesis))
        self.assertTrue(case_file_event_material_override("", base))

    def test_seconds_between_iso(self):
        a = "2025-06-01T12:00:00+00:00"
        b = "2025-06-01T12:02:30+00:00"
        self.assertEqual(seconds_between_iso(a, b), 150.0)

    def test_regret_bucket_change_emits_even_inside_case_file_interval(self):
        """Regret bucket is part of posture signature — a real shift should still produce a row."""
        sig_a = "STAY_COURSE|hold|M|THESIS_INTACT|L|M|weak|balanced|symbol_specific"
        sig_b = "STAY_COURSE|hold|M|THESIS_INTACT|L|M|weak|exit_favored|symbol_specific"
        self.assertTrue(case_file_event_material_override(sig_a, sig_b))
        with patch.object(lic_engine, "build_case_file_signature", return_value=sig_a):
            o1 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": {}})
        prior = dict(o1["intelligence_by_symbol"]["TEST"])
        prior["feed_fingerprint"] = "stale|trigger|emit"
        prior["dominant_world_id"] = "__bogus__"
        prior["last_case_file_emit_at"] = datetime.now(timezone.utc).isoformat()
        with (
            patch.object(lic_engine, "build_case_file_signature", return_value=sig_b),
            patch.object(lic_engine, "seconds_between_iso", return_value=30.0),
            patch.object(lic_engine, "CASE_FILE_MIN_EMIT_INTERVAL_SEC", 3600),
        ):
            o2 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": {"TEST": prior}})
        self.assertEqual(len(o2["feed_events"]), 1)

    def test_rate_limit_override_when_material(self):
        sig_a = "STAY_COURSE|hold|M|THESIS_INTACT|L|M|weak|balanced|symbol_specific"
        sig_b = "WATCH_CLOSELY|hold|M|THESIS_INTACT|L|M|weak|balanced|symbol_specific"
        self.assertTrue(case_file_event_material_override(sig_a, sig_b))
        with patch.object(lic_engine, "build_case_file_signature", return_value=sig_a):
            o1 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": {}})
        prior = dict(o1["intelligence_by_symbol"]["TEST"])
        prior["feed_fingerprint"] = "stale|trigger|emit"
        prior["dominant_world_id"] = "__bogus__"
        prior["last_case_file_emit_at"] = datetime.now(timezone.utc).isoformat()
        with (
            patch.object(lic_engine, "build_case_file_signature", return_value=sig_b),
            patch.object(lic_engine, "seconds_between_iso", return_value=30.0),
            patch.object(lic_engine, "CASE_FILE_MIN_EMIT_INTERVAL_SEC", 3600),
        ):
            o2 = run_deterministic_step({"positions": [_tile()], "prior_intelligence": {"TEST": prior}})
        self.assertEqual(len(o2["feed_events"]), 1)

    def test_simulator_misalignment_flag_when_net_best_differs_from_band(self):
        """STAY_COURSE prefers hold; net winner is often trim/exit — expect possible misalignment."""
        body = {"positions": [_tile()], "prior_intelligence": {}}
        out = run_deterministic_step(body)
        intel = out["intelligence_by_symbol"]["TEST"]
        sim = intel.get("action_simulation") or {}
        self.assertIn("aligns_with_tile_recommendation", sim)
        self.assertIn("misalignment_note", sim)
        ba = sim.get("best_action") or {}
        ranked = sim.get("ranked_by_net_score") or []
        self.assertEqual(ba.get("action"), ranked[0] if ranked else None)


if __name__ == "__main__":
    unittest.main()

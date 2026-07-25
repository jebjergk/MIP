import unittest
from datetime import datetime, timedelta

from app.committee.shadow_intraday_session import (
    VERDICT_CHALLENGES,
    VERDICT_MIXED,
    VERDICT_SUPPORTS,
    build_shadow_intraday_session_picture,
    filter_bars_rth_since_open,
)


def _bar_et(hour: int, minute: int, o, h, l, c, day_offset: int = 0):
    base = datetime(2026, 6, 30, hour, minute, 0)
    base = base + timedelta(days=day_offset)
    return {"TS": base, "OPEN": o, "HIGH": h, "LOW": l, "CLOSE": c}


class ShadowIntradaySessionTests(unittest.TestCase):
    def test_filters_premarket_and_prior_days(self):
        rows = [
            _bar_et(8, 0, 100, 101, 99, 100),
            _bar_et(9, 30, 100, 101, 99.5, 100.5),
            _bar_et(9, 45, 100.5, 101.5, 100, 101),
            _bar_et(9, 30, 100, 101, 99, 100, day_offset=-1),
        ]
        now = datetime(2026, 6, 30, 11, 0, 0)
        filtered, rth_open = filter_bars_rth_since_open(rows, now=now)
        self.assertEqual(rth_open.hour, 9)
        self.assertEqual(rth_open.minute, 30)
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]["c"], 100.5)

    def test_supports_overrides_hostile_dossier_when_reclaim_held(self):
        snap = {
            "SYMBOL": "MRK",
            "SIDE": "LONG",
            "ENTRY_ZONE_JSON": {"low": 125.0, "high": 127.0},
            "INVALIDATION_JSON": {"level": 120.0, "rule": "BELOW"},
        }
        dossier = {
            "actionability_context": {
                "confirmation_needed": True,
                "continuation_quality": "UNCONFIRMED",
                "entry_location_quality": "AT_RESISTANCE",
            },
            "levels": {
                "broken_resistance_as_support": 125.14,
                "nearest_resistance": {"level_price": 130.29},
            },
        }
        bars = [
            _bar_et(9, 30, 125.0, 125.5, 124.8, 125.2),
            _bar_et(9, 45, 125.2, 126.0, 125.1, 125.8),
            _bar_et(10, 0, 125.8, 126.4, 125.6, 126.2),
            _bar_et(10, 15, 126.2, 126.8, 126.0, 126.5),
        ]
        now = datetime(2026, 6, 30, 10, 30, 0)
        pic = build_shadow_intraday_session_picture(
            symbol="MRK",
            side="LONG",
            snapshot=snap,
            dossier_payload=dossier,
            bar_rows=bars,
            now=now,
        )
        self.assertTrue(pic["session_available"])
        self.assertEqual(pic["verdict_bucket"], VERDICT_SUPPORTS)
        self.assertFalse(pic["vs_overnight_dossier"]["overnight_flags_still_binding"])
        self.assertEqual(pic["vs_overnight_dossier"]["reclaim_status"], "HELD")

    def test_challenges_when_choppy_against_long(self):
        snap = {
            "SYMBOL": "TEST",
            "SIDE": "LONG",
            "ENTRY_ZONE_JSON": {"low": 99.0, "high": 101.0},
            "INVALIDATION_JSON": {"level": 90.0, "rule": "BELOW"},
        }
        bars = [
            _bar_et(9, 30, 100, 100.5, 99.5, 100.0),
            _bar_et(9, 45, 100, 100.2, 99.0, 99.2),
            _bar_et(10, 0, 99.2, 99.8, 98.8, 99.6),
            _bar_et(10, 15, 99.6, 99.9, 99.0, 99.1),
        ]
        now = datetime(2026, 6, 30, 10, 30, 0)
        pic = build_shadow_intraday_session_picture(
            symbol="TEST",
            side="LONG",
            snapshot=snap,
            dossier_payload={},
            bar_rows=bars,
            now=now,
        )
        self.assertIn(pic["verdict_bucket"], (VERDICT_CHALLENGES, VERDICT_MIXED))
        self.assertFalse(pic["direction_alignment"]["aligned"])

    def test_unavailable_before_rth_open(self):
        snap = {"SYMBOL": "X", "SIDE": "LONG", "ENTRY_ZONE_JSON": {}, "INVALIDATION_JSON": {}}
        now = datetime(2026, 6, 30, 8, 0, 0)
        pic = build_shadow_intraday_session_picture(
            symbol="X", side="LONG", snapshot=snap, bar_rows=[], now=now
        )
        self.assertFalse(pic["session_available"])
        self.assertEqual(pic["reason"], "BEFORE_RTH_OPEN")

    def test_early_session_single_rth_bar_is_available_low_sample(self):
        snap = {
            "SYMBOL": "CLF",
            "SIDE": "LONG",
            "ENTRY_ZONE_JSON": {"low": 9.0, "high": 10.0},
            "INVALIDATION_JSON": {"level": 8.0, "rule": "BELOW"},
        }
        bars = [{"TS": datetime(2026, 7, 6, 9, 30, 0), "OPEN": 9.5, "HIGH": 9.6, "LOW": 9.4, "CLOSE": 9.55}]
        now = datetime(2026, 7, 6, 9, 36, 0)
        pic = build_shadow_intraday_session_picture(
            symbol="CLF",
            side="LONG",
            snapshot=snap,
            dossier_payload={},
            bar_rows=bars,
            now=now,
            fetch_meta={"status": "SUCCESS"},
        )
        self.assertTrue(pic["session_available"])
        self.assertTrue(pic["low_sample_warning"])
        self.assertEqual(pic["bar_count"], 1)

    def test_jnj_like_stale_dossier_reclaim_uses_invalidation_and_held_when_above(self):
        """Regression: dossier 245 reclaim must not override executable zone / invalidation."""
        snap = {
            "SYMBOL": "JNJ",
            "SIDE": "LONG",
            "ENTRY_ZONE_JSON": {"low": 254.42, "high": 260.2},
            "INVALIDATION_JSON": {"level": 251.71, "rule": "BELOW"},
        }
        dossier = {
            "actionability_context": {
                "confirmation_needed": True,
                "continuation_quality": "UNCONFIRMED",
            },
            "levels": {
                "broken_resistance_as_support": 245.49,
                "nearest_resistance": {"level_price": 269.43},
            },
        }
        bars = [
            _bar_et(9, 30, 261.0, 262.0, 260.5, 261.5, day_offset=24),
        ]
        now = datetime(2026, 7, 24, 9, 36, 0)
        pic = build_shadow_intraday_session_picture(
            symbol="JNJ",
            side="LONG",
            snapshot=snap,
            dossier_payload=dossier,
            bar_rows=bars,
            now=now,
            fetch_meta={"status": "SUCCESS"},
        )
        vs = pic["vs_overnight_dossier"]
        self.assertEqual(vs["reclaim_level"], 251.71)
        self.assertEqual(vs["reclaim_level_source"], "invalidation")
        self.assertEqual(vs["dossier_legacy_reclaim"], 245.49)
        self.assertEqual(vs["reclaim_status"], "HELD")
        self.assertEqual(pic["executable_geometry"]["price_vs_entry_zone"], "ABOVE")
        self.assertTrue(pic["executable_geometry"]["chase_risk"])


if __name__ == "__main__":
    unittest.main()

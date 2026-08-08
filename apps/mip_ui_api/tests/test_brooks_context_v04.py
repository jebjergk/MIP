"""BROOKS_CONTEXT_RULESET_V0_4 certification and timestamp regression."""

from __future__ import annotations

import unittest

from app.brooks_intraday.brooks_timestamp import bar_ts_to_ny_iso, ny_hm
from app.brooks_intraday.context_v04_certification import run_certification
from app.brooks_intraday.learning_view import _format_ny_short, extract_state_transitions


class BrooksContextV04Tests(unittest.TestCase):
    def test_certification_bundle(self):
        rep = run_certification()
        self.assertTrue(rep["ok"], msg=str(rep))

    def test_utc_fallback_amzn_consider_entry_1025_et(self):
        iso = bar_ts_to_ny_iso(bar_ts_utc="2026-07-13T14:25:00")
        self.assertEqual(iso, "2026-07-13T10:25:00")
        self.assertEqual(ny_hm(bar_ts_utc="2026-07-13T14:25:00"), "10:25")
        self.assertEqual(_format_ny_short("2026-07-13T14:25:00"), "10:25")

    def test_rth_row_displays_et_session(self):
        rows = [
            {
                "bar_ts": "2026-07-13T14:25:00",
                "symbol": "AMZN",
                "state_before": "ENTRY_ARMED",
                "state_after": "ENTRY_ARMED",
                "selected_action": "CONSIDER_ENTRY",
            }
        ]
        tr = extract_state_transitions(rows, symbol="AMZN")
        self.assertEqual(tr[0]["time_ny_short"], "10:25")
        self.assertEqual(tr[0]["bar_ts_ny"], "2026-07-13T10:25:00")

    def test_prefers_explicit_bar_ts_ny(self):
        iso = bar_ts_to_ny_iso(bar_ts_ny="2026-07-13T10:25:00", bar_ts_utc="2026-07-13T14:25:00")
        self.assertEqual(iso, "2026-07-13T10:25:00")


if __name__ == "__main__":
    unittest.main()

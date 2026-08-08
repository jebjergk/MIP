"""G2 per-bar narrative acceptance (deterministic, presentation-only)."""

from __future__ import annotations

import unittest

from app.brooks_intraday.learning_bar_narrative import build_bar_narrative

AMZN_TRADE = {
    "symbol": "AMZN",
    "quantity": 4,
    "entry_price": 246.53,
    "exit_price": 248.25,
    "realized_pnl": 6.88,
}


def _row(ts_ny: str, ts_utc: str, **extra) -> dict:
    base = {
        "bar_ts": ts_utc,
        "bar_ts_ny": f"2026-07-13T{ts_ny}:00",
        "symbol": "AMZN",
        "ohlcv": {"open": 246.5, "high": 246.6, "low": 246.4, "close": 246.53},
        "selected_action": "HOLD_POSITION",
        "simulation_effect": "—",
        "objective_terms": [],
        "active_patterns": [],
    }
    base.update(extra)
    return base


class LearningBarNarrativeTests(unittest.TestCase):
    def test_entry_bar_1025_ny(self):
        row = _row(
            "10:25",
            "2026-07-13T14:25:00",
            selected_action="CONSIDER_ENTRY",
            simulation_effect="ENTRY AMZN x4",
            objective_terms=[{"term": "TWO_LEGGED_PULLBACK"}],
            active_patterns=[{"pattern_family": "POSSIBLE_H2_LONG", "lifecycle": "DEVELOPING"}],
        )
        ledger = [{"event": "ENTRY"}, {"event": "INITIAL_STOP_CALCULATED", "stop_price": 246.065}]
        narr = build_bar_narrative(
            row,
            trade=AMZN_TRADE,
            ledger_events=ledger,
            prev_ledger_events=[],
            qty=4,
            stop=None,
            initial_stop=246.065,
        )
        self.assertEqual(narr["headline"], "Entry conditions became valid")
        self.assertIn("pullback", narr["story"]["market_story"].lower())
        self.assertIn("246.53", narr["story"]["decision"])
        self.assertTrue(any("246.065" in x for x in narr["story"]["position_risk"]))
        self.assertNotIn("6.88", narr["story"]["market_story"])

    def test_trail_activation_1120_ny(self):
        row = _row("11:20", "2026-07-13T15:20:00")
        prev = [{"event": "STOP_TRAIL_SCHEDULED", "ts": "2026-07-13T15:15:00", "candidate_stop": 248.17}]
        ledger = [
            {
                "event": "STOP_TRAIL_UPDATE",
                "new_stop": 248.17,
                "calculation_bar": "2026-07-13 15:15:00",
            }
        ]
        narr = build_bar_narrative(
            row,
            trade=AMZN_TRADE,
            ledger_events=ledger,
            prev_ledger_events=prev,
            qty=4,
            stop=248.17,
            initial_stop=246.065,
        )
        self.assertEqual(narr["headline"], "Profit protection increased")
        self.assertIn("248.17", narr["story"]["decision"])
        self.assertIn("previous bar", " ".join(narr["story"]["system_noticed"]).lower())
        self.assertTrue(narr["story"].get("stop_timeline"))

    def test_exit_1145_ny(self):
        row = _row("11:45", "2026-07-13T15:45:00", simulation_effect="EXIT AMZN")
        ledger = [{"event": "EXIT"}, {"event": "STOP_EVAL", "active_stop": 248.25}]
        narr = build_bar_narrative(
            row,
            trade=AMZN_TRADE,
            ledger_events=ledger,
            prev_ledger_events=[],
            qty=0,
            stop=248.25,
            initial_stop=246.065,
            peak_unrealized=11.28,
            peak_unrealized_ts_ny="11:05",
        )
        self.assertEqual(narr["headline"], "Trade closed at the protective stop")
        self.assertIn("248.25", narr["story"]["decision"])
        self.assertTrue(any("6.88" in x for x in narr["story"]["position_risk"]))
        self.assertTrue(any("11.28" in x for x in narr["story"]["position_risk"]))

    def test_ordinary_hold_bar(self):
        row = _row("11:00", "2026-07-13T15:00:00", selected_action="HOLD_POSITION")
        ledger = [{"event": "STOP_EVAL", "active_stop": 246.065}]
        narr = build_bar_narrative(
            row,
            trade=AMZN_TRADE,
            ledger_events=ledger,
            prev_ledger_events=[],
            qty=4,
            stop=246.065,
            initial_stop=246.065,
        )
        self.assertEqual(narr["headline"], "Hold position")
        self.assertNotIn("6.88", narr["story"]["market_story"])


if __name__ == "__main__":
    unittest.main()

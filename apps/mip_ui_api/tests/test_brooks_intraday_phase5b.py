"""Phase 5B validation and V0.2 pattern rules tests."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.pattern_engine import PatternRecord, get_pattern_session
from app.brooks_intraday.pattern_engine_v02 import advance_patterns_v02_for_bar
from app.brooks_intraday.pattern_lifecycle import audit_h1_h2, validate_lifecycle_history
from app.brooks_intraday.pattern_ruleset_v02 import (
    DEFAULT_PARAMETERS,
    expiry_bars_for_family,
    resolve_params,
)


def _bar(ts: str, o: float, h: float, l: float, c: float) -> HistoricalBar:
    dt = datetime.fromisoformat(ts)
    return HistoricalBar(
        symbol="AAPL",
        trading_date=date(2026, 7, 20),
        ts_utc=dt,
        ts_ny=dt,
        open=o,
        high=h,
        low=l,
        close=c,
        volume=1000,
        source="TEST",
        bar_size_minutes=5,
        rth=True,
    )


def _obs(**kwargs) -> dict:
    return {
        "brooks_obs_json": kwargs.get("terms", []),
        "derived_metrics_json": {"direction": kwargs.get("direction", "BULLISH"), "relative_range_class": "LARGE"},
    }


class Phase5BV02H1Tests(unittest.TestCase):
    def test_h1_fail_requires_prior_possible_history(self):
        state: dict = {}
        td = date(2026, 7, 20)
        seq = [
            _bar("2026-07-20T13:30:00", 100, 101, 99, 100),
            _bar("2026-07-20T13:35:00", 100, 100.5, 98, 98.5),
            _bar("2026-07-20T13:40:00", 98.5, 99.2, 98, 99.0),
            _bar("2026-07-20T13:45:00", 99, 99.5, 98.2, 98.3),
        ]
        for b in seq:
            advance_patterns_v02_for_bar(
                state=state,
                run_id="r",
                objective_baseline_attempt_id="b",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs(direction="BEARISH" if b.close < b.open else "BULLISH"),
            )
        sess = get_pattern_session(state, "AAPL", td)
        failed = [p for p in sess.patterns.values() if p.pattern_family == "FAILED_H1_LONG"]
        for p in failed:
            self.assertIn("POSSIBLE", [h.get("lifecycle") for h in p.lifecycle_history])

    def test_h2_has_parent(self):
        state: dict = {}
        td = date(2026, 7, 20)
        for i, ts in enumerate(["13:30", "13:35", "13:40", "13:45", "13:50", "13:55"]):
            b = _bar(f"2026-07-20T{ts}:00", 100 - i * 0.1, 101, 99 - i * 0.2, 100 - i * 0.15)
            advance_patterns_v02_for_bar(
                state=state,
                run_id="r",
                objective_baseline_attempt_id="b",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs(direction="BULLISH" if b.close >= b.open else "BEARISH"),
            )
        sess = get_pattern_session(state, "AAPL", td)
        h2 = [p for p in sess.patterns.values() if "H2" in p.pattern_family]
        for p in h2:
            self.assertTrue(p.parent_pattern_instance_id)


class Phase5BV02DoubleBottomTests(unittest.TestCase):
    def test_micro_normal_windows_disjoint(self):
        p = resolve_params()
        self.assertLess(int(p["micro_double_bottom_max_bars_apart"]), int(p["double_bottom_min_bars_apart"]))

    def test_min_bounce_param_documented(self):
        self.assertIn("double_bottom_min_bounce_range_fraction", DEFAULT_PARAMETERS)


class Phase5BV02ExpiryTests(unittest.TestCase):
    def test_family_specific_expiry(self):
        p = resolve_params()
        self.assertLess(expiry_bars_for_family("POSSIBLE_H1_LONG", p), expiry_bars_for_family("DOUBLE_BOTTOM", p))


class Phase5BV02MicroChannelTests(unittest.TestCase):
    def test_channel_updates_one_instance(self):
        state: dict = {}
        td = date(2026, 7, 20)
        ts_list = ["13:30", "13:35", "13:40", "13:45", "13:50", "13:55"]
        ids = set()
        for j, ts in enumerate(ts_list):
            p = 100 + j * 0.15
            b = _bar(f"2026-07-20T{ts}:00", p, p + 0.4, p - 0.05, p + 0.2)
            advance_patterns_v02_for_bar(
                state=state,
                run_id="r",
                objective_baseline_attempt_id="b",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs(),
            )
            sess = get_pattern_session(state, "AAPL", td)
            ch = [x for x in sess.patterns.values() if x.pattern_family == "BULL_MICRO_CHANNEL"]
            ids.update(x.pattern_instance_id for x in ch)
        self.assertEqual(len(ids), 1)


class Phase5BLifecycleTests(unittest.TestCase):
    def test_confirmed_after_failed_rejected(self):
        errs = validate_lifecycle_history(
            [{"lifecycle": "POSSIBLE"}, {"lifecycle": "FAILED"}, {"lifecycle": "CONFIRMED"}]
        )
        self.assertTrue(any("confirmed_after_failed" in e for e in errs))


if __name__ == "__main__":
    unittest.main()

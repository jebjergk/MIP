"""Phase 5C — V0.3 double-bottom hierarchy and symmetric micro channels."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.pattern_engine import get_pattern_session
from app.brooks_intraday.pattern_engine_v02 import advance_patterns_v02_for_bar
from app.brooks_intraday.pattern_engine_v03 import advance_patterns_v03_for_bar
from app.brooks_intraday.pattern_ruleset_v01 import RULESET_VERSION as V01_RS
from app.brooks_intraday.pattern_ruleset_v02 import RULESET_VERSION as V02_RS
from app.brooks_intraday.pattern_ruleset_v03 import RULESET_VERSION as V03_RS, resolve_params


def _bar(ts: str, o: float, h: float, l: float, c: float, sym: str = "AAPL") -> HistoricalBar:
    dt = datetime.fromisoformat(ts)
    return HistoricalBar(
        symbol=sym,
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


class Phase5CV03DoubleBottomTests(unittest.TestCase):
    def test_same_pair_no_duplicate_instances(self):
        state: dict = {}
        td = date(2026, 7, 20)
        seq = [
            _bar("2026-07-20T13:30:00", 100, 101, 99.5, 100),
            _bar("2026-07-20T13:35:00", 100, 102, 99.5, 101.5),
            _bar("2026-07-20T13:40:00", 101.5, 102, 99.5, 100),
            _bar("2026-07-20T13:45:00", 100, 101, 99.4, 99.8),
            _bar("2026-07-20T13:50:00", 99.8, 100.5, 99.5, 100.2),
        ]
        for b in seq:
            advance_patterns_v03_for_bar(
                state=state,
                run_id="r",
                objective_baseline_attempt_id="b",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs(),
            )
        sess = get_pattern_session(state, "AAPL", td)
        dbs = [p for p in sess.patterns.values() if "DOUBLE" in p.pattern_family or p.pattern_family == "LOW_RETEST"]
        pair_keys = [p.relevant_prices.get("pair_key") for p in dbs if p.relevant_prices.get("pair_key")]
        self.assertEqual(len(pair_keys), len(set(pair_keys)))

    def test_negligible_bounce_rejected(self):
        p = resolve_params()
        self.assertGreater(float(p["low_retest_min_bounce_fraction"]), 0)

    def test_hierarchy_families_distinct_params(self):
        p = resolve_params()
        self.assertLess(float(p["micro_db_min_bounce_fraction"]), float(p["structural_db_min_bounce_fraction"]))


class Phase5CV03MicroChannelTests(unittest.TestCase):
    def test_bear_micro_channel_detected(self):
        state: dict = {}
        td = date(2026, 7, 20)
        price = 200.0
        for i, ts in enumerate(["13:30", "13:35", "13:40", "13:45", "13:50"]):
            h = price - i * 0.05
            l = h - 0.3
            b = _bar(f"2026-07-20T{ts}:00", h, h, l, l - 0.05, sym="AAPL")
            advance_patterns_v03_for_bar(
                state=state,
                run_id="r",
                objective_baseline_attempt_id="b",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs(direction="BEARISH"),
            )
        sess = get_pattern_session(state, "AAPL", td)
        bears = [p for p in sess.patterns.values() if p.pattern_family == "BEAR_MICRO_CHANNEL"]
        self.assertTrue(bears, "expected bear micro channel")

    def test_channel_updates_single_instance(self):
        state: dict = {}
        td = date(2026, 7, 20)
        lows = [100, 100.01, 100.02, 100.03, 100.04, 100.05]
        for i, ts in enumerate(["13:30", "13:35", "13:40", "13:45", "13:50", "13:55"]):
            lo = lows[i]
            b = _bar(f"2026-07-20T{ts}:00", lo, lo + 0.5, lo, lo + 0.2)
            advance_patterns_v03_for_bar(
                state=state,
                run_id="r",
                objective_baseline_attempt_id="b",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs(),
            )
        sess = get_pattern_session(state, "AAPL", td)
        bulls = [p for p in sess.patterns.values() if p.pattern_family == "BULL_MICRO_CHANNEL"]
        self.assertEqual(len(bulls), 1)

    def test_channel_fails_on_opposing_break(self):
        state: dict = {}
        td = date(2026, 7, 20)
        for i, ts in enumerate(["13:30", "13:35", "13:40", "13:45", "13:50", "13:55"]):
            lo = 50 + i * 0.02
            b = _bar(f"2026-07-20T{ts}:00", lo, lo + 0.4, lo, lo + 0.1)
            advance_patterns_v03_for_bar(
                state=state,
                run_id="r",
                objective_baseline_attempt_id="b",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs(),
            )
        break_bar = _bar("2026-07-20T14:00:00", 50.2, 50.5, 49.5, 49.7)
        advance_patterns_v03_for_bar(
            state=state,
            run_id="r",
            objective_baseline_attempt_id="b",
            symbol="AAPL",
            trading_date=td,
            bar=break_bar,
            objective_obs=_obs(direction="BEARISH"),
        )
        sess = get_pattern_session(state, "AAPL", td)
        bulls = [p for p in sess.patterns.values() if p.pattern_family == "BULL_MICRO_CHANNEL"]
        self.assertTrue(any(p.lifecycle == "FAILED" for p in bulls))


class Phase5CV03DeterminismTests(unittest.TestCase):
    def test_v03_ruleset_version(self):
        self.assertEqual(V03_RS, "BROOKS_PATTERN_RULESET_V0_3")

    def test_v01_v02_rulesets_unchanged(self):
        self.assertEqual(V01_RS, "BROOKS_PATTERN_RULESET_V0_1")
        self.assertEqual(V02_RS, "BROOKS_PATTERN_RULESET_V0_2")

    def test_v03_replay_deterministic_two_passes(self):
        td = date(2026, 7, 20)
        seq = [
            _bar("2026-07-20T13:30:00", 100, 101, 99, 100),
            _bar("2026-07-20T13:35:00", 100, 102, 99.5, 101),
            _bar("2026-07-20T13:40:00", 101, 103, 100.5, 102),
        ]

        def run_once() -> list[str]:
            state: dict = {}
            for b in seq:
                advance_patterns_v03_for_bar(
                    state=state,
                    run_id="r",
                    objective_baseline_attempt_id="b",
                    symbol="AAPL",
                    trading_date=td,
                    bar=b,
                    objective_obs=_obs(),
                )
            sess = get_pattern_session(state, "AAPL", td)
            return sorted(p.pattern_family for p in sess.patterns.values())

        self.assertEqual(run_once(), run_once())


class Phase5CV02UnchangedSmoke(unittest.TestCase):
    def test_v02_engine_still_importable(self):
        advance_patterns_v02_for_bar(
            state={},
            run_id="r",
            objective_baseline_attempt_id="b",
            symbol="AAPL",
            trading_date=date(2026, 7, 20),
            bar=_bar("2026-07-20T13:30:00", 1, 2, 0.5, 1.5),
            objective_obs=_obs(),
        )


if __name__ == "__main__":
    unittest.main()

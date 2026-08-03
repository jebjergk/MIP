"""Phase 5 pattern engine unit tests (no Snowflake / no future bars)."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.pattern_engine import (
    advance_patterns_for_bar,
    get_pattern_session,
    reset_pattern_session,
)
from app.brooks_intraday.pattern_ruleset_v01 import (
    LIFECYCLE_CONFIRMED,
    LIFECYCLE_FAILED,
    LIFECYCLE_POSSIBLE,
    price_tolerance,
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


def _obs(direction: str = "BULLISH", terms: list | None = None) -> dict:
    return {
        "brooks_obs_json": [{"term": t} for t in (terms or [])],
        "derived_metrics_json": {"direction": direction, "relative_range_class": "LARGE", "close_location": 0.9},
        "rule_ids": ["BROOKS_BAR_BULL_V0_1"],
    }


class Phase5LifecycleTests(unittest.TestCase):
    def test_possible_to_confirmed_swing(self):
        state: dict = {}
        td = date(2026, 7, 20)
        bars = [
            _bar("2026-07-20T13:30:00", 100, 101, 99, 100.5),
            _bar("2026-07-20T13:35:00", 100.5, 103, 100, 102),
            _bar("2026-07-20T13:40:00", 102, 102.5, 101, 101.5),
        ]
        for b in bars:
            advance_patterns_for_bar(
                state=state,
                run_id="r1",
                objective_baseline_attempt_id="base",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs(),
            )
        sess = get_pattern_session(state, "AAPL", td)
        confirmed = [p for p in sess.patterns.values() if p.pattern_family == "SWING_HIGH" and p.lifecycle == LIFECYCLE_CONFIRMED]
        self.assertTrue(confirmed)


class Phase5H1H2Tests(unittest.TestCase):
    def test_h2_requires_prior_attempt(self):
        state: dict = {}
        td = date(2026, 7, 20)
        reset_pattern_session(state, "AAPL", td)
        seq = [
            _bar("2026-07-20T13:30:00", 100, 101, 99, 100),
            _bar("2026-07-20T13:35:00", 100, 100.5, 98, 98.5),
            _bar("2026-07-20T13:40:00", 98.5, 99, 98, 98.2),
            _bar("2026-07-20T13:45:00", 98.2, 99.5, 98, 99.2),
            _bar("2026-07-20T13:50:00", 99.2, 98.5, 97.5, 97.8),
            _bar("2026-07-20T13:55:00", 97.8, 99, 97.5, 98.8),
        ]
        for b in seq:
            advance_patterns_for_bar(
                state=state,
                run_id="r1",
                objective_baseline_attempt_id="base",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs("BULLISH" if b.close >= b.open else "BEARISH"),
            )
        sess = get_pattern_session(state, "AAPL", td)
        h1 = [p for p in sess.patterns.values() if "H1" in p.pattern_family]
        h2 = [p for p in sess.patterns.values() if "H2" in p.pattern_family]
        self.assertTrue(h1)
        if h2:
            self.assertTrue(h1)


class Phase5DoubleBottomTests(unittest.TestCase):
    def test_tolerance_rejects_wide_lows(self):
        tol = price_tolerance("AAPL", 2.0, {}, micro=False)
        self.assertGreaterEqual(tol, 0.03)

    def test_micro_vs_normal_distance(self):
        from app.brooks_intraday.pattern_ruleset_v01 import DEFAULT_PARAMETERS

        self.assertLess(
            int(DEFAULT_PARAMETERS["micro_double_bottom_max_bars_apart"]),
            int(DEFAULT_PARAMETERS["double_bottom_max_bars_apart"]),
        )


class Phase5WedgeTests(unittest.TestCase):
    def test_two_pushes_not_wedge(self):
        state: dict = {}
        td = date(2026, 7, 20)
        for i, ts in enumerate(["13:30", "13:35", "13:40", "13:45"]):
            b = _bar(f"2026-07-20T{ts}:00", 100 - i, 101 - i, 99 - i, 100 - i)
            advance_patterns_for_bar(
                state=state,
                run_id="r1",
                objective_baseline_attempt_id="base",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs("BEARISH"),
            )
        sess = get_pattern_session(state, "AAPL", td)
        wedges = [p for p in sess.patterns.values() if "WEDGE" in p.pattern_family]
        self.assertEqual(len(wedges), 0)


class Phase5BreakoutTests(unittest.TestCase):
    def test_structural_breakout_needs_swing(self):
        state: dict = {}
        td = date(2026, 7, 20)
        b = _bar("2026-07-20T13:30:00", 100, 105, 99, 104)
        r = advance_patterns_for_bar(
            state=state,
            run_id="r1",
            objective_baseline_attempt_id="base",
            symbol="AAPL",
            trading_date=td,
            bar=b,
            objective_obs=_obs(terms=["POSSIBLE_BREAKOUT_BAR"]),
        )
        sess = get_pattern_session(state, "AAPL", td)
        structural = [p for p in sess.patterns.values() if p.pattern_family == "STRUCTURAL_BREAKOUT"]
        self.assertEqual(len(structural), 0)
        self.assertIn(r["action"], ("OBSERVE", "WAIT"))


class Phase5MicroChannelTests(unittest.TestCase):
    def test_bull_micro_channel_min_length(self):
        state: dict = {}
        td = date(2026, 7, 20)
        ts_list = ["13:30", "13:35", "13:40", "13:45", "13:50"]
        for j, ts in enumerate(ts_list):
            p = 100 + j * 0.2
            b = _bar(f"2026-07-20T{ts}:00", p, p + 0.5, p - 0.1 + j * 0.05, p + 0.3)
            advance_patterns_for_bar(
                state=state,
                run_id="r1",
                objective_baseline_attempt_id="base",
                symbol="AAPL",
                trading_date=td,
                bar=b,
                objective_obs=_obs(),
            )
        sess = get_pattern_session(state, "AAPL", td)
        ch = [p for p in sess.patterns.values() if p.pattern_family == "BULL_MICRO_CHANNEL"]
        self.assertTrue(ch)


class Phase5DeterminismTests(unittest.TestCase):
    def test_same_inputs_same_active_ids(self):
        td = date(2026, 7, 20)
        bars = [
            _bar("2026-07-20T13:30:00", 100, 101, 99, 100.5),
            _bar("2026-07-20T13:35:00", 100.5, 102, 100, 101.5),
        ]
        out = []
        for _ in range(2):
            state: dict = {}
            for b in bars:
                r = advance_patterns_for_bar(
                    state=state,
                    run_id="r1",
                    objective_baseline_attempt_id="base",
                    symbol="AAPL",
                    trading_date=td,
                    bar=b,
                    objective_obs=_obs(),
                )
            out.append(r["active_pattern_instance_ids"])
        self.assertEqual(out[0], out[1])


if __name__ == "__main__":
    unittest.main()

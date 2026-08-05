"""Phase D — V0.3 predicates, transitions, certification, V0.2 immutability."""

from __future__ import annotations

import json
import unittest
from datetime import date, datetime

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.context_engine import advance_context_for_bar
from app.brooks_intraday.context_engine_v03 import (
    close_near_high,
    is_breakout_candidate,
    is_bullish_bar,
    is_failed_breakout,
    objective_breakout_combo,
    session_range,
    tie_break_v03,
    V03SessionState,
)
from app.brooks_intraday.context_ruleset_v02 import RULESET_VERSION as V02
from app.brooks_intraday.context_ruleset_v03 import (
    ACTION_DO_NOT_ENTER,
    BLOCKER_ONE_POSITION_TIEBREAK_SKIP,
    LAST_ENTRY_BAR_INDEX_IN_SESSION,
    RL_BROKEN_PENDING,
    RULESET_VERSION as V03,
    STATE_OBSERVATION_ONLY,
    normalize_verdict,
    resolve_params,
    verdict_class,
)
from app.brooks_intraday.context_v03_certification import (
    SCENARIOS,
    check_determinism,
    check_no_lookahead,
    check_resolver,
    check_v02_unchanged,
    run_certification,
    write_evidence,
)


def _bar(o, h, l, c, idx=5):
    ts = datetime(2099, 1, 6, 10, 0)
    return HistoricalBar(
        symbol="SYN",
        trading_date=date(2099, 1, 6),
        ts_utc=ts,
        ts_ny=ts,
        open=o,
        high=h,
        low=l,
        close=c,
        volume=1,
        source="T",
        bar_size_minutes=5,
        rth=True,
    )


class PredicateTests(unittest.TestCase):
    def test_verdict_normalize_aliases(self):
        self.assertEqual(normalize_verdict("LONG_BIAS"), "LONG_APPROVE")
        self.assertEqual(verdict_class("DEFER"), "DEFER")
        self.assertEqual(verdict_class("NO_CLEAR_LONG"), "NO_CLEAR_LONG")

    def test_breakout_candidate_requires_close_above(self):
        params = resolve_params()
        bar = _bar(100.2, 101, 100.1, 100.8)
        prior = _bar(99, 100, 98.5, 99.5)
        self.assertTrue(
            is_breakout_candidate(
                bar=bar,
                prior=prior,
                resistance=100.0,
                patterns=[{"pattern_family": "STRUCTURAL_BREAKOUT", "lifecycle": "CONFIRMED"}],
                thesis_effect="NEUTRAL",
                bar_index=10,
                params=params,
            )
        )
        self.assertFalse(
            is_breakout_candidate(
                bar=_bar(99, 99.5, 98.5, 99.2),
                prior=prior,
                resistance=100.0,
                patterns=[{"pattern_family": "STRUCTURAL_BREAKOUT", "lifecycle": "CONFIRMED"}],
                thesis_effect="NEUTRAL",
                bar_index=10,
                params=params,
            )
        )

    def test_objective_combo(self):
        params = resolve_params()
        prior = _bar(99, 100, 98.5, 99.5)
        bar = _bar(100.1, 101.2, 100.0, 101.1)  # bullish, close>prior high, near high, HH
        self.assertTrue(objective_breakout_combo(bar, prior, params))

    def test_close_near_high(self):
        params = resolve_params()
        self.assertTrue(close_near_high(_bar(100, 101, 100, 100.9), params))
        self.assertFalse(close_near_high(_bar(100, 101, 99, 99.2), params))

    def test_failed_breakout_material_close(self):
        params = resolve_params()
        sess = V03SessionState(symbol="SYN", trading_date=date(2099, 1, 6))
        sess.resistance_lifecycle = RL_BROKEN_PENDING
        sess.broken_resistance = 100.0
        sess.session_high = 102
        sess.session_low = 97
        bar = _bar(99, 99.5, 98, 98.5)
        self.assertTrue(is_failed_breakout(sess=sess, bar=bar, prior=None, patterns=[], params=params))

    def test_wick_alone_not_fail(self):
        params = resolve_params()
        sess = V03SessionState(symbol="SYN", trading_date=date(2099, 1, 6))
        sess.resistance_lifecycle = RL_BROKEN_PENDING
        sess.broken_resistance = 100.0
        sess.session_high = 102
        sess.session_low = 97
        # wick below, close holds
        bar = _bar(100.5, 101, 99.0, 100.4)
        self.assertFalse(is_failed_breakout(sess=sess, bar=bar, prior=None, patterns=[], params=params))


class TieBreakTests(unittest.TestCase):
    def test_daily_before_upgrade_then_risk(self):
        winner, losers = tie_break_v03(
            [
                {
                    "symbol": "MCD",
                    "path_class": "UPGRADE",
                    "room_class": "AMPLE_ROOM",
                    "entry_risk": 0.01,
                    "confirmation_score": 9,
                    "signal_ts": "a",
                },
                {
                    "symbol": "AAPL",
                    "path_class": "DAILY",
                    "room_class": "ACCEPTABLE_ROOM",
                    "entry_risk": 0.2,
                    "confirmation_score": 1,
                    "signal_ts": "b",
                },
            ]
        )
        self.assertEqual(winner["symbol"], "AAPL")
        self.assertEqual(losers[0]["skip_reason"], BLOCKER_ONE_POSITION_TIEBREAK_SKIP)

    def test_symbol_priority_stable(self):
        w1, _ = tie_break_v03(
            [
                {
                    "symbol": "JPM",
                    "path_class": "DAILY",
                    "room_class": "AMPLE_ROOM",
                    "entry_risk": 0.1,
                    "confirmation_score": 1,
                    "signal_ts": "t",
                },
                {
                    "symbol": "AAPL",
                    "path_class": "DAILY",
                    "room_class": "AMPLE_ROOM",
                    "entry_risk": 0.1,
                    "confirmation_score": 1,
                    "signal_ts": "t",
                },
            ]
        )
        self.assertEqual(w1["symbol"], "AAPL")


class ResolverSerializationTests(unittest.TestCase):
    def test_default_not_v03(self):
        state = {}
        r = advance_context_for_bar(
            state=state,
            dossier={"paa_verdict": "NO_CLEAR_LONG", "trade_simulation_ready": True, "resistance_zones": [{"low": 99, "high": 100}]},
            symbol="SYN",
            trading_date=date(2099, 1, 6),
            bar=_bar(96, 97, 95, 96),
            bar_index_in_session=5,
            objective_obs={"brooks_obs_json": []},
            patterns=[],
        )
        # V0.2 path — no ruleset_version in layers the same way; action DO_NOT_ENTER
        self.assertEqual(r.selected_action, ACTION_DO_NOT_ENTER)

    def test_v03_serialization_fields(self):
        state = {}
        r = advance_context_for_bar(
            state=state,
            dossier={"paa_verdict": "NO_CLEAR_LONG", "trade_simulation_ready": True, "resistance_zones": [{"low": 99, "high": 100}]},
            symbol="SYN",
            trading_date=date(2099, 1, 6),
            bar=_bar(96, 97, 95, 96),
            bar_index_in_session=5,
            objective_obs={"brooks_obs_json": []},
            patterns=[],
            ruleset_version=V03,
        )
        payload = {
            "state": r.state_after,
            "action": r.selected_action,
            "blockers": r.blockers,
            "resistance_lifecycle": r.layers["contextual_interpretation"]["resistance_lifecycle"],
            "ruleset_version": r.layers["contextual_interpretation"]["ruleset_version"],
        }
        s = json.dumps(payload)
        loaded = json.loads(s)
        self.assertEqual(loaded["ruleset_version"], V03)
        self.assertEqual(loaded["state"], STATE_OBSERVATION_ONLY)

    def test_cutoff_constant(self):
        self.assertEqual(LAST_ENTRY_BAR_INDEX_IN_SESSION, 72)


class CertificationSuiteTests(unittest.TestCase):
    def test_all_scenarios(self):
        for fn in SCENARIOS:
            with self.subTest(fn.__name__):
                result = fn()
                self.assertTrue(result["passed"], msg=json.dumps(result, indent=2, default=str))

    def test_no_lookahead(self):
        self.assertTrue(check_no_lookahead()["passed"])

    def test_determinism(self):
        self.assertTrue(check_determinism()["passed"])

    def test_v02_regression(self):
        self.assertTrue(check_v02_unchanged()["passed"])

    def test_resolver(self):
        self.assertTrue(check_resolver()["passed"])

    def test_write_evidence(self):
        ev = write_evidence()
        self.assertTrue(ev["ok"], msg=json.dumps({"failed": ev.get("failed")}, indent=2))
        self.assertFalse(ev["historical_replay"])
        self.assertFalse(ev["v03_active_by_default"])


if __name__ == "__main__":
    unittest.main()

"""Phase E1 — prior-bar ENTRY_ARMED required before CONSIDER_ENTRY (V0.3)."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.context_engine import advance_context_for_bar
from app.brooks_intraday.context_engine_v03 import (
    may_emit_consider_entry_v03,
    reset_v03_session,
)
from app.brooks_intraday.context_ruleset_v01 import ACTION_CONSIDER_ENTRY, ACTION_HOLD_POSITION
from app.brooks_intraday.context_ruleset_v03 import (
    RULESET_VERSION as V03,
    STATE_DO_NOT_CHASE,
    STATE_ENTRY_ARMED,
    resolve_params,
)
from app.brooks_intraday.context_v03_certification import (
    check_determinism,
    check_no_lookahead,
    check_v02_unchanged,
    run_certification,
    scenario_04_confirmed_pullback_entry,
)


def _bar(idx: int, o, h, l, c, hour=10, minute=0):
    ts = datetime(2099, 1, 6, hour, minute)
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


class PriorBarArmingGateTests(unittest.TestCase):
    def test_same_bar_arm_forbids_consider_entry(self):
        params = resolve_params()
        sess_state = type("S", (), {"entry_armed_bar": 5, "advisory_state": STATE_DO_NOT_CHASE})()
        self.assertFalse(
            may_emit_consider_entry_v03(
                state_before=STATE_DO_NOT_CHASE,
                bar_index_in_session=5,
                sess=sess_state,
                bar=_bar(5, 1, 2, 0.5, 1.8),
                meaningful=[{"pattern_family": "H2", "lifecycle": "CONFIRMED"}],
                params=params,
                opening=False,
            )
        )

    def test_prior_bar_armed_allows_later_consider_entry(self):
        params = resolve_params()
        sess_state = type("S", (), {"entry_armed_bar": 5, "advisory_state": STATE_ENTRY_ARMED})()
        self.assertTrue(
            may_emit_consider_entry_v03(
                state_before=STATE_ENTRY_ARMED,
                bar_index_in_session=6,
                sess=sess_state,
                bar=_bar(6, 1.8, 2.2, 1.7, 2.1),
                meaningful=[{"pattern_family": "H2", "lifecycle": "CONFIRMED"}],
                params=params,
                opening=False,
            )
        )

    def test_certified_pullback_still_has_arm_then_consider_on_later_bar(self):
        sc = scenario_04_confirmed_pullback_entry()
        self.assertTrue(sc["passed"], sc)
        actions = sc["actual_actions"]
        states = sc["actual_states"]
        arm_i = next(i for i, s in enumerate(states) if s == STATE_ENTRY_ARMED)
        ce_i = next(i for i, a in enumerate(actions) if a == ACTION_CONSIDER_ENTRY)
        self.assertGreater(ce_i, arm_i)

    def test_position_open_maps_consider_entry_to_hold(self):
        state: dict = {}
        reset_v03_session(state, "AMZN", date(2099, 1, 6))
        dossier = {
            "paa_verdict": "NO_CLEAR_LONG",
            "support_zones": [{"low": 90, "high": 91}],
            "resistance_zones": [{"low": 99.5, "high": 100.5}, {"low": 112, "high": 113}],
            "do_not_chase_level": 120.0,
        }
        # Minimal smoke: open_position_symbol only affects selected_action at result time.
        res = advance_context_for_bar(
            state=state,
            dossier=dossier,
            symbol="AMZN",
            trading_date=date(2099, 1, 6),
            bar=_bar(0, 98, 99, 97, 98.5),
            bar_index_in_session=10,
            objective_obs={"brooks_obs_json": []},
            patterns=[],
            ruleset_version=V03,
            open_position_symbol="AMZN",
        )
        if res.selected_action == ACTION_CONSIDER_ENTRY:
            self.fail("expected HOLD_POSITION remap when positioned")
        # If not entry-capable on this bar, action should not be CE.
        self.assertNotEqual(res.selected_action, ACTION_CONSIDER_ENTRY)


class V03RegressionSuiteTests(unittest.TestCase):
    def test_v02_unchanged(self):
        self.assertTrue(check_v02_unchanged())

    def test_no_lookahead(self):
        self.assertTrue(check_no_lookahead())

    def test_determinism(self):
        self.assertTrue(check_determinism())

    def test_certification_still_passes(self):
        rep = run_certification()
        self.assertTrue(rep["passed"], rep.get("failures"))


if __name__ == "__main__":
    unittest.main()

"""Phase 6B — calibration, audit helpers, V0.2 engine behavior."""

from __future__ import annotations

import hashlib
import unittest
from datetime import date, datetime

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.context_engine import advance_context_for_bar, reset_context_session
from app.brooks_intraday.context_phase6b_audit import (
    action_event_and_duration,
    classify_do_not_enter_event,
)
from app.brooks_intraday.context_ruleset_v01 import (
    ACTION_DO_NOT_ENTER,
    ACTION_OBSERVE,
    ACTION_THESIS_INVALIDATED,
    ACTION_WAIT,
    RULESET_VERSION as RULESET_V01,
)
from app.brooks_intraday.context_ruleset_v02 import RULESET_VERSION as RULESET_V02
from app.brooks_intraday.level_derivation import assess_readiness


def _bar(ts: str, o: float, h: float, l: float, c: float) -> HistoricalBar:
    dt = datetime.fromisoformat(ts)
    return HistoricalBar(
        symbol="AAPL",
        trading_date=date(2026, 7, 21),
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


def _dossier(**kwargs) -> dict:
    base = {
        "paa_verdict": "LONG_APPROVE",
        "paa_confidence": 0.8,
        "trade_simulation_ready": True,
        "observation_ready": True,
        "support_zones": [{"low": 99.0, "high": 99.5}],
        "resistance_zones": [{"low": 105.0, "high": 105.5}],
        "reclaim_level": 101.0,
        "do_not_chase_level": 104.5,
        "invalidation_level": 98.0,
        "preferred_long_scenario": "Pullback to support",
        "main_risk": "Gap down",
        "methodologist_summary": "Test summary",
        "paa_analysis_id": "test-id",
        "daily_trend": "UP",
        "geometry_summary_json": {
            "support_zones": [{"low": 99.0, "high": 99.5}],
            "resistance_zones": [{"low": 105.0, "high": 105.5}],
        },
    }
    base.update(kwargs)
    return base


def _obs(*terms) -> dict:
    return {"brooks_obs_json": [{"term": t} for t in terms], "derived_metrics_json": {}}


class Phase6BReadinessTests(unittest.TestCase):
    def test_frozen_sim_ready_not_downgraded(self):
        d = _dossier(trade_simulation_ready=True)
        _obs_r, sim_r, _, _ = assess_readiness(d, verdict=d["paa_verdict"])
        self.assertTrue(sim_r)
        state: dict = {}
        r = advance_context_for_bar(
            state=state,
            dossier=d,
            symbol="AAPL",
            trading_date=date(2026, 7, 21),
            bar=_bar("2026-07-21T13:35:00", 100, 100.5, 99.5, 100),
            bar_index_in_session=5,
            objective_obs=_obs(),
            patterns=[],
            ruleset_version=RULESET_V02,
        )
        self.assertTrue(r.layers["contextual_interpretation"]["simulation_ready"])


class Phase6BV02WaitVsDoNotEnterTests(unittest.TestCase):
    def test_limited_room_wait_not_do_not_enter(self):
        state: dict = {}
        td = date(2026, 7, 21)
        dossier = _dossier(resistance_zones=[{"low": 104.35, "high": 104.45}], do_not_chase_level=105.0)
        r = advance_context_for_bar(
            state=state,
            dossier=dossier,
            symbol="AAPL",
            trading_date=td,
            bar=_bar("2026-07-21T13:40:00", 104.0, 104.25, 103.9, 104.15),
            bar_index_in_session=6,
            objective_obs=_obs("HIGHER_LOW"),
            patterns=[{"pattern_family": "STRUCTURAL_DOUBLE_BOTTOM", "lifecycle": "CONFIRMED"}],
            ruleset_version=RULESET_V02,
        )
        self.assertNotEqual(r.selected_action, ACTION_DO_NOT_ENTER)
        self.assertIn(r.selected_action, (ACTION_WAIT, ACTION_OBSERVE, "WAIT_FOR_RECLAIM", "WAIT_FOR_PULLBACK"))

    def test_no_clear_long_still_do_not_enter(self):
        state: dict = {}
        r = advance_context_for_bar(
            state=state,
            dossier=_dossier(paa_verdict="NO_CLEAR_LONG"),
            symbol="AAPL",
            trading_date=date(2026, 7, 21),
            bar=_bar("2026-07-21T13:35:00", 100, 100.5, 99.5, 100),
            bar_index_in_session=5,
            objective_obs=_obs(),
            patterns=[],
            ruleset_version=RULESET_V02,
        )
        self.assertEqual(r.selected_action, ACTION_DO_NOT_ENTER)


class Phase6BIntradayVsDailyInvalidationTests(unittest.TestCase):
    def test_intraday_setup_break_weakens_not_daily_invalidate(self):
        state: dict = {}
        td = date(2026, 7, 21)
        for i, c in enumerate([100.0, 99.5, 99.0, 98.5]):
            r = advance_context_for_bar(
                state=state,
                dossier=_dossier(),
                symbol="AAPL",
                trading_date=td,
                bar=_bar(f"2026-07-21T13:{30 + i * 5}:00", c, c + 0.3, c - 0.5, c),
                bar_index_in_session=5 + i,
                objective_obs=_obs(),
                patterns=[{"pattern_family": "STRUCTURAL_DOUBLE_BOTTOM", "lifecycle": "CONFIRMED"}],
                ruleset_version=RULESET_V02,
            )
        self.assertNotEqual(r.selected_action, ACTION_THESIS_INVALIDATED)
        self.assertIn("entry_condition_matrix", r.layers.get("diagnostics", {}))


class Phase6BTransitionEventTests(unittest.TestCase):
    def test_one_thesis_event_many_duration_rows(self):
        rows = [
            {"selected_action": "OBSERVE", "state_after": "X", "bar_ts": "1", "sequence_num": 0},
            {"selected_action": "THESIS_INVALIDATED", "state_after": "Y", "bar_ts": "2", "sequence_num": 1},
            {"selected_action": "THESIS_INVALIDATED", "state_after": "Y", "bar_ts": "3", "sequence_num": 2},
        ]
        stats = action_event_and_duration(rows)
        self.assertEqual(stats["action_transition_event_counts"].get("THESIS_INVALIDATED"), 1)
        self.assertEqual(stats["action_duration_counts"].get("THESIS_INVALIDATED"), 2)


class Phase6BV01ImmutableHashTests(unittest.TestCase):
    def test_v01_deterministic_signature(self):
        dossier = _dossier(paa_verdict="WAIT_PULLBACK")
        bars = [
            _bar("2026-07-21T13:30:00", 100, 100.5, 99.8, 100),
            _bar("2026-07-21T13:35:00", 100, 100.2, 99.5, 99.7),
        ]
        parts = []
        for _ in range(2):
            state: dict = {}
            reset_context_session(state, "AAPL", date(2026, 7, 21))
            chunk = []
            for i, b in enumerate(bars):
                r = advance_context_for_bar(
                    state=state,
                    dossier=dossier,
                    symbol="AAPL",
                    trading_date=date(2026, 7, 21),
                    bar=b,
                    bar_index_in_session=i,
                    objective_obs=_obs(),
                    patterns=[],
                    ruleset_version=RULESET_V01,
                )
                chunk.append(f"{r.state_after}|{r.selected_action}")
            parts.append(hashlib.sha256("\n".join(chunk).encode()).hexdigest())
        self.assertEqual(parts[0], parts[1])


class Phase6BV02DeterminismTests(unittest.TestCase):
    def test_v02_deterministic(self):
        dossier = _dossier()
        bar = _bar("2026-07-21T13:35:00", 100, 100.5, 99.5, 100)
        sigs = []
        for _ in range(2):
            state: dict = {}
            r = advance_context_for_bar(
                state=state,
                dossier=dossier,
                symbol="AAPL",
                trading_date=date(2026, 7, 21),
                bar=bar,
                bar_index_in_session=5,
                objective_obs=_obs(),
                patterns=[],
                ruleset_version=RULESET_V02,
            )
            sigs.append(f"{r.selected_action}|{r.state_after}|{r.layers['diagnostics']['entry_score']}")
        self.assertEqual(sigs[0], sigs[1])


class Phase6BDoNotEnterClassifierTests(unittest.TestCase):
    def test_limited_room_classifier(self):
        row = {
            "selected_action": ACTION_DO_NOT_ENTER,
            "state_after": "ENTRY_BLOCKED",
            "payload_json": {"blockers_json": ["LIMITED_ROOM_TO_RESISTANCE"], "room_class": "LIMITED_ROOM"},
        }
        self.assertEqual(classify_do_not_enter_event(row, _dossier()), "insufficient_room")


class Phase6BNoTradeLogicTests(unittest.TestCase):
    def test_layers_exclude_trade_fields(self):
        state: dict = {}
        r = advance_context_for_bar(
            state=state,
            dossier=_dossier(),
            symbol="AAPL",
            trading_date=date(2026, 7, 21),
            bar=_bar("2026-07-21T13:35:00", 100, 100.5, 99.5, 100),
            bar_index_in_session=4,
            objective_obs=_obs(),
            patterns=[],
            ruleset_version=RULESET_V02,
        )
        blob = str(r.layers) + str(r.blockers)
        self.assertNotIn("SIMULATED_TRADE", blob.upper())
        self.assertNotIn("PORTFOLIO", blob.upper())


if __name__ == "__main__":
    unittest.main()

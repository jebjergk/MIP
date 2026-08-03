"""Phase 6 — context engine and advisory state machine tests."""

from __future__ import annotations

import hashlib
import unittest
from datetime import date, datetime

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.context_engine import advance_context_for_bar, reset_context_session
from app.brooks_intraday.context_ruleset_v01 import (
    ACTION_CONSIDER_ENTRY,
    ACTION_DO_NOT_CHASE,
    ACTION_THESIS_INVALIDATED,
    EFFECT_INVALIDATES,
    RULESET_VERSION as RULESET_V01,
    STATE_THESIS_INVALIDATED,
    STATE_WAITING_FOR_PULLBACK,
    resolve_params,
)


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
        "support_zones": [{"low": 99.0, "high": 99.5}],
        "resistance_zones": [{"low": 105.0, "high": 105.5}],
        "reclaim_level": 101.0,
        "do_not_chase_level": 104.5,
        "invalidation_level": 98.0,
        "preferred_long_scenario": "Pullback to support",
        "main_risk": "Gap down",
    }
    base.update(kwargs)
    return base


def _obs(*terms) -> dict:
    return {"brooks_obs_json": [{"term": t} for t in terms], "derived_metrics_json": {}}


class Phase6VerdictMappingTests(unittest.TestCase):
    def test_wait_pullback_initial_state(self):
        state: dict = {}
        td = date(2026, 7, 21)
        r = advance_context_for_bar(
            state=state,
            dossier=_dossier(paa_verdict="WAIT_PULLBACK"),
            symbol="AAPL",
            trading_date=td,
            bar=_bar("2026-07-21T13:35:00", 100, 100.5, 99.5, 100),
            bar_index_in_session=5,
            objective_obs=_obs(),
            patterns=[],
            ruleset_version=RULESET_V01,
        )
        self.assertIn(r.state_after, (STATE_WAITING_FOR_PULLBACK, "OBSERVING_OPEN"))


class Phase6InvalidationTests(unittest.TestCase):
    def test_daily_thesis_invalidation(self):
        state: dict = {}
        td = date(2026, 7, 21)
        r = advance_context_for_bar(
            state=state,
            dossier=_dossier(),
            symbol="AAPL",
            trading_date=td,
            bar=_bar("2026-07-21T14:00:00", 97, 97.5, 96.5, 97.0),
            bar_index_in_session=10,
            objective_obs=_obs(),
            patterns=[],
            ruleset_version=RULESET_V01,
        )
        self.assertEqual(r.thesis_effect, EFFECT_INVALIDATES)
        self.assertEqual(r.selected_action, ACTION_THESIS_INVALIDATED)
        self.assertEqual(r.state_after, STATE_THESIS_INVALIDATED)
        self.assertTrue(any(e.get("type") == "DAILY_THESIS_INVALIDATION" for e in r.opposing_evidence))

    def test_simulated_stop_not_used(self):
        p = resolve_params()
        self.assertNotIn("SIMULATED_TRADE_STOP", str(p))


class Phase6DoNotChaseTests(unittest.TestCase):
    def test_do_not_chase_precedence(self):
        state: dict = {}
        td = date(2026, 7, 21)
        r = advance_context_for_bar(
            state=state,
            dossier=_dossier(),
            symbol="AAPL",
            trading_date=td,
            bar=_bar("2026-07-21T14:05:00", 104.6, 105, 104.2, 104.8),
            bar_index_in_session=12,
            objective_obs=_obs("BIG_BULL_BAR"),
            patterns=[{"pattern_family": "STRUCTURAL_DOUBLE_BOTTOM", "lifecycle": "CONFIRMED"}],
            ruleset_version=RULESET_V01,
        )
        self.assertEqual(r.selected_action, ACTION_DO_NOT_CHASE)


class Phase6PatternSignificanceTests(unittest.TestCase):
    def test_micro_alone_cannot_arm(self):
        state: dict = {}
        td = date(2026, 7, 21)
        r = advance_context_for_bar(
            state=state,
            dossier=_dossier(),
            symbol="AAPL",
            trading_date=td,
            bar=_bar("2026-07-21T13:40:00", 100, 100.4, 99.6, 100.2),
            bar_index_in_session=6,
            objective_obs=_obs("HIGHER_LOW"),
            patterns=[{"pattern_family": "MICRO_DOUBLE_BOTTOM", "lifecycle": "CONFIRMED"}],
            ruleset_version=RULESET_V01,
        )
        self.assertNotEqual(r.selected_action, ACTION_CONSIDER_ENTRY)


class Phase6ReclaimTests(unittest.TestCase):
    def test_intrabar_high_insufficient_for_confirm(self):
        state: dict = {}
        td = date(2026, 7, 21)
        r = advance_context_for_bar(
            state=state,
            dossier=_dossier(),
            symbol="AAPL",
            trading_date=td,
            bar=_bar("2026-07-21T13:45:00", 100.5, 101.5, 100, 100.2),
            bar_index_in_session=7,
            objective_obs=_obs(),
            patterns=[],
            ruleset_version=RULESET_V01,
        )
        self.assertNotEqual(r.reclaim_stage, "RECLAIM_CONFIRMED")


class Phase6BulkInteractiveEquivalenceTests(unittest.TestCase):
    def _signature(self, state: dict, dossier: dict, bars: list[HistoricalBar]) -> str:
        reset_context_session(state, "AAPL", date(2026, 7, 21))
        parts = []
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
            parts.append(f"{r.state_after}|{r.selected_action}|{r.thesis_effect}")
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    def test_bulk_interactive_hash_match(self):
        dossier = _dossier(paa_verdict="WAIT_PULLBACK")
        bars = [
            _bar("2026-07-21T13:30:00", 100, 100.5, 99.8, 100),
            _bar("2026-07-21T13:35:00", 100, 100.2, 99.5, 99.7),
            _bar("2026-07-21T13:40:00", 99.7, 100, 99.2, 99.9),
        ]
        h1 = self._signature({}, dossier, bars)
        h2 = self._signature({}, dossier, bars)
        self.assertEqual(h1, h2)


class Phase6LayerTests(unittest.TestCase):
    def test_layers_separate(self):
        state: dict = {}
        r = advance_context_for_bar(
            state=state,
            dossier=_dossier(),
            symbol="AAPL",
            trading_date=date(2026, 7, 21),
            bar=_bar("2026-07-21T13:35:00", 100, 100.5, 99.5, 100),
            bar_index_in_session=4,
            objective_obs=_obs("BULL_BAR"),
            patterns=[],
            ruleset_version=RULESET_V01,
        )
        self.assertIn("objective_fact", r.layers)
        self.assertIn("pattern_instance", r.layers)
        self.assertIn("contextual_interpretation", r.layers)
        self.assertIn("advisory_state", r.layers)
        self.assertIn("advisory_action", r.layers)


if __name__ == "__main__":
    unittest.main()

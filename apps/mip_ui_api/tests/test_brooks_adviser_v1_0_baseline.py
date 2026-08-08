"""Regression: single operational Adviser — BROOKS_INTRADAY_ADVISER_V1_0."""

from __future__ import annotations

import inspect
import unittest
from dataclasses import dataclass
from pathlib import Path

import app.brooks_intraday.adviser_poc_v01 as adviser_session_module
from app.brooks_intraday.adviser_baseline_v01 import (
    ADVISER_VERSION,
    CANONICAL_QUERY_TAG,
    CONFIRMATION_CONTRACT_VERSION,
    CONFIRMATION_LOGIC_KIND,
    EXECUTION_DIRECTION,
    LAB_STARTING_CASH,
    RAG_CORPUS_VERSION,
    canonical_attempt_config,
    canonical_cost_metadata,
    canonical_run_pin,
    is_canonical_adviser_foundation,
)
from app.brooks_intraday.adviser_foundation_ui import (
    FOUNDATION_CONTEXT_RULESET,
    filter_chains_for_ui,
    is_adviser_foundation_chain,
)
from app.brooks_intraday.adviser_runner_v1_0 import run_adviser_session
from app.brooks_intraday.adviser_sim_lab_v01 import LAB_STARTING_CASH as SIM_LAB_CASH
from app.brooks_intraday.adviser_wake_v01 import ThesisWakeState, evaluate_wake, legacy_material_change
from app.brooks_intraday.objective_ruleset_v01 import compute_geometry

REPO_ROOT = Path(__file__).resolve().parents[4]


@dataclass
class _Bar:
    open: float
    high: float
    low: float
    close: float
    volume: float = 1.0


def _g(bar: _Bar):
    return compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)


class AdviserV1_0BaselineTests(unittest.TestCase):
    def test_single_adviser_version_constant(self):
        self.assertEqual(ADVISER_VERSION, "BROOKS_INTRADAY_ADVISER_V1_0")
        self.assertEqual(FOUNDATION_CONTEXT_RULESET, ADVISER_VERSION)

    def test_no_poc_version_selector_on_session_runner(self):
        sig = inspect.signature(run_adviser_session)
        self.assertNotIn("poc_version", sig.parameters)

    def test_run_amzn_poc_removed(self):
        self.assertFalse(hasattr(adviser_session_module, "run_amzn_poc"))

    def test_obsolete_poc_runners_deleted(self):
        cursorfiles = REPO_ROOT / "cursorfiles"
        matches = list(cursorfiles.glob("run_amzn_adviser_poc*"))
        self.assertEqual(matches, [], f"obsolete runners remain: {matches}")

    def test_canonical_dev_runner_exists(self):
        path = REPO_ROOT / "cursorfiles" / "run_brooks_adviser_session.py"
        self.assertTrue(path.is_file(), "expected generic V1.0 dev runner")


class CanonicalMetadataTests(unittest.TestCase):
    def test_attempt_config_carries_v1_0_stack(self):
        cfg = canonical_attempt_config(symbol="AMZN", trading_date="2026-07-13")
        self.assertEqual(cfg["adviser_version"], ADVISER_VERSION)
        self.assertEqual(cfg["confirmation_contract_version"], CONFIRMATION_CONTRACT_VERSION)
        self.assertEqual(cfg["confirmation_logic"], CONFIRMATION_LOGIC_KIND)
        self.assertEqual(cfg["lab_starting_cash"], LAB_STARTING_CASH)
        self.assertEqual(cfg["execution_direction"], EXECUTION_DIRECTION)
        self.assertEqual(cfg["rag_corpus_version"], RAG_CORPUS_VERSION)

    def test_run_pin_and_query_tag(self):
        pin = canonical_run_pin(adviser_attempt_id="a1", simulation_attempt_id="s1")
        self.assertEqual(pin["query_tag"], CANONICAL_QUERY_TAG)
        self.assertEqual(pin["query_tag"], "BROOKS_ADVISER_INTRADAY_V1_0")
        self.assertTrue(is_canonical_adviser_foundation(pin))

    def test_cost_metadata_matches_baseline(self):
        meta = canonical_cost_metadata()
        self.assertEqual(meta["adviser_version"], ADVISER_VERSION)
        self.assertEqual(meta["wake_policy"], "THESIS_DRIVEN")


class FoundationUiTests(unittest.TestCase):
    def test_only_v1_0_pins_are_foundation_chains(self):
        self.assertFalse(
            is_adviser_foundation_chain(
                {
                    "adviser_attempt_id": "old",
                    "context_ruleset": "BROOKS_INTRADAY_ADVISER_V0_1",
                    "query_tag": "BROOKS_ADVISER_AMZN_POC_V06",
                }
            )
        )
        self.assertTrue(
            is_adviser_foundation_chain(
                {
                    "adviser_attempt_id": "new",
                    "adviser_version": ADVISER_VERSION,
                }
            )
        )

    def test_obsolete_adviser_hidden_even_in_diagnostic_legacy(self):
        chains = [
            {
                "adviser_attempt_id": "poc",
                "context_ruleset": "BROOKS_INTRADAY_ADVISER_V0_1",
            },
            {
                "adviser_attempt_id": "v1",
                "adviser_version": ADVISER_VERSION,
            },
        ]
        visible = filter_chains_for_ui(chains, diagnostic_legacy=True)
        self.assertEqual(len(visible), 1)
        self.assertEqual(visible[0]["adviser_attempt_id"], "v1")


class ObsoleteWakePathTests(unittest.TestCase):
    def test_geometry_change_does_not_wake_via_evaluate_wake(self):
        state = ThesisWakeState()
        state.bars_since_meaningful_wake = 0
        bar = _Bar(10.0, 10.5, 9.8, 10.2)
        prev = {"direction": "NEUTRAL", "range": 0.4, "high": 10.0, "low": 9.8}
        g = _g(bar)
        self.assertIsNotNone(legacy_material_change(prev, bar, g))
        decision = evaluate_wake(
            state,
            bar_index=5,
            bar=bar,
            g=g,
            prev_geom=prev,
            recent=[bar],
            prev_bar=None,
        )
        self.assertIsNone(decision)


class LabCapitalTests(unittest.TestCase):
    def test_lab_cash_is_1000(self):
        self.assertEqual(SIM_LAB_CASH, 1000)
        self.assertNotEqual(SIM_LAB_CASH, 100_000)


if __name__ == "__main__":
    unittest.main()

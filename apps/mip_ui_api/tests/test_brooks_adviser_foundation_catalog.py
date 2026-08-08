"""Regression: normal catalog hides legacy chains; V1.0 Adviser only in foundation mode."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from app.brooks_intraday.adviser_baseline_v01 import ADVISER_VERSION
from app.brooks_intraday.adviser_foundation_ui import (
    filter_chains_for_ui,
    is_adviser_foundation_chain,
    is_legacy_review_chain,
)
from app.brooks_intraday.review_catalog import build_review_catalog


class AdviserFoundationCatalogTests(unittest.TestCase):
    def test_legacy_chain_detection(self):
        self.assertTrue(
            is_legacy_review_chain(
                {"official": True, "context_ruleset": "BROOKS_CONTEXT_RULESET_V0_3"}
            )
        )
        self.assertFalse(
            is_adviser_foundation_chain(
                {
                    "context_ruleset": ADVISER_VERSION,
                    "adviser_attempt_id": "poc5",
                    "query_tag": "BROOKS_ADVISER_AMZN_POC_V05",
                }
            )
        )
        self.assertTrue(
            is_adviser_foundation_chain(
                {
                    "context_ruleset": ADVISER_VERSION,
                    "adviser_attempt_id": "canon",
                    "adviser_version": ADVISER_VERSION,
                    "confirmation_contract_version": 2,
                    "confirmation_logic": "BOOLEAN_TREE",
                }
            )
        )
        self.assertFalse(
            is_legacy_review_chain({"context_ruleset": ADVISER_VERSION, "adviser_foundation": True})
        )

    def test_filter_shows_only_v1_0_in_foundation_mode(self):
        chains = [
            {"context_ruleset": "BROOKS_CONTEXT_RULESET_V0_3", "official": True},
            {
                "context_ruleset": ADVISER_VERSION,
                "adviser_attempt_id": "a1",
                "adviser_version": ADVISER_VERSION,
            },
        ]
        out = filter_chains_for_ui(chains, diagnostic_legacy=False)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["adviser_version"], ADVISER_VERSION)

    @patch("app.brooks_intraday.review_catalog.list_v1_validation_sessions")
    @patch("app.brooks_intraday.review_catalog.list_validation_run_ids")
    @patch("app.brooks_intraday.review_catalog.build_run_catalog_entry")
    def test_catalog_mode_foundation(self, mock_entry, mock_runs, mock_v1):
        mock_v1.return_value = [
            {
                "symbol": "MCD",
                "trading_date": "2026-07-14",
                "adviser_attempt_id": "81482bb9-0171-481b-9368-a3b0a4b4e50b",
                "simulation_attempt_id": "758f00ab-2d62-46fb-8746-bd46fb00a178",
                "run_id": "a669144d-0d59-4431-af3a-958f691334d0",
                "selector_label": "MCD · 2026-07-14 · COMPLETE · 0 trades",
            }
        ]
        mock_runs.return_value = [{"run_id": "r1"}]
        mock_entry.return_value = {
            "run_id": "r1",
            "week_start": "2026-07-07",
            "available_chains": [],
            "available_symbol_sessions": [{"symbol": "AMZN", "trading_date": "2026-07-13"}],
            "symbols": ["AMZN"],
        }
        cat = build_review_catalog(diagnostic_legacy=False)
        self.assertEqual(cat["catalog_mode"], "adviser_foundation")
        self.assertEqual(cat["runs"], [])
        self.assertEqual(len(cat["v1_validation_sessions"]), 1)
        self.assertEqual(cat["defaults"]["adviser_attempt_id"], "81482bb9-0171-481b-9368-a3b0a4b4e50b")

    @patch("app.brooks_intraday.review_catalog.list_v1_validation_sessions", return_value=[])
    def test_catalog_empty_v1_sessions_after_archive(self, _mock_v1):
        from app.brooks_intraday.adviser_foundation_ui import V1_NO_ACTIVE_SESSIONS_MESSAGE

        cat = build_review_catalog(diagnostic_legacy=False)
        self.assertEqual(cat["v1_validation_sessions"], [])
        self.assertIsNone(cat["defaults"].get("adviser_attempt_id"))
        self.assertIn("New Validation", cat["adviser_empty_state"])
        self.assertEqual(cat["adviser_empty_state"], V1_NO_ACTIVE_SESSIONS_MESSAGE)


if __name__ == "__main__":
    unittest.main()

"""Review catalog for G2.4 lab selection."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from app.brooks_intraday.learning_constants import (
    PHASE_E1_VALIDATION_RUN_ID,
    PM_V01_CERT_SIMULATION_ATTEMPT_ID,
    PM_V01_OBSOLETE_CERT_SIMULATION_ATTEMPT_ID,
)
from app.brooks_intraday.review_catalog import (
    _chain_primary_label,
    _flatten_chains,
    build_review_catalog,
)


class ReviewCatalogLabelTests(unittest.TestCase):
    def test_official_label(self):
        self.assertEqual(_chain_primary_label({"official": True}), "Official baseline")

    def test_canonical_pm_label_without_sim_uuid(self):
        lab = _chain_primary_label(
            {"canonical_pm_certification": True, "realized_pnl": 6.88}
        )
        self.assertIn("Canonical position-management review", lab)
        self.assertIn("+$6.88", lab)
        self.assertNotIn("125eb282", lab)

    def test_obsolete_label(self):
        lab = _chain_primary_label({"obsolete_certification": True})
        self.assertIn("Obsolete certification", lab)


class ReviewCatalogFlattenTests(unittest.TestCase):
    def test_obsolete_sorted_last(self):
        payload = {
            "official": {
                "context_attempt_id": "ctx-off",
                "simulation_attempt_id": "sim-off",
                "label": "official",
            },
            "alternatives": [
                {
                    "context_attempt_id": "ctx-a",
                    "simulation_attempt_id": PM_V01_OBSOLETE_CERT_SIMULATION_ATTEMPT_ID,
                    "obsolete_certification": True,
                    "label": "old",
                },
                {
                    "context_attempt_id": "ctx-b",
                    "simulation_attempt_id": PM_V01_CERT_SIMULATION_ATTEMPT_ID,
                    "canonical_pm_certification": True,
                    "realized_pnl": 6.88,
                    "label": "canonical",
                },
            ],
        }
        flat = _flatten_chains(payload)
        self.assertTrue(flat[0].get("official") or flat[0].get("canonical_pm_certification"))
        self.assertTrue(flat[-1].get("obsolete_certification"))
        self.assertTrue(flat[-1].get("disabled"))


class ReviewCatalogBuildTests(unittest.TestCase):
    @patch("app.brooks_intraday.review_catalog.list_v1_validation_sessions", return_value=[])
    @patch("app.brooks_intraday.review_catalog.list_validation_run_ids")
    @patch("app.brooks_intraday.review_catalog.build_run_catalog_entry")
    def test_build_includes_defaults(self, mock_entry, mock_list, _mock_v1):
        mock_list.return_value = [{"run_id": PHASE_E1_VALIDATION_RUN_ID}]
        mock_entry.return_value = {
            "run_id": PHASE_E1_VALIDATION_RUN_ID,
            "week_start": "2026-07-13",
            "available_chains": [],
        }
        out = build_review_catalog(diagnostic_legacy=True)
        self.assertEqual(len(out["runs"]), 1)
        self.assertEqual(out["defaults"]["run_id"], PHASE_E1_VALIDATION_RUN_ID)


if __name__ == "__main__":
    unittest.main()

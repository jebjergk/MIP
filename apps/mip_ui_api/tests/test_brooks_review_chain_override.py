"""Review-only alternate attempt-chain selection (no pin mutation)."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from app.brooks_intraday.learning_constants import (
    PHASE_E1_VALIDATION_RUN_ID,
    PM_V01_CERT_SIMULATION_ATTEMPT_ID,
    PM_V01_OBSOLETE_CERT_SIMULATION_ATTEMPT_ID,
)
from app.brooks_intraday.learning_view import (
    _finalize_review_chain_alternatives,
    _merge_review_chain_supplements,
    resolve_attempt_chain,
    validate_review_attempt_override,
)


class ReviewChainOverrideTests(unittest.TestCase):
    def test_official_resolve_has_no_override_flag(self):
        attempts = resolve_attempt_chain("unknown-run", {}, {}, diagnostic_legacy=True)
        self.assertFalse(attempts.get("review_override"))
        self.assertTrue(attempts.get("review_only"))

    def test_foundation_resolve_empty_without_diagnostic(self):
        attempts = resolve_attempt_chain("unknown-run", {}, {})
        self.assertTrue(attempts.get("adviser_foundation_empty"))
        self.assertIsNone(attempts.get("context_attempt_id"))

    @patch("app.brooks_intraday.learning_view.load_simulation_attempt")
    @patch("app.brooks_intraday.learning_view._load_context_attempt_meta")
    def test_resolve_prefers_phase_e1_pins_from_config(self, mock_ctx, mock_sim):
        mock_ctx.return_value = {
            "context_ruleset_version": "BROOKS_CONTEXT_RULESET_V0_3",
        }
        mock_sim.return_value = {
            "simulation_ruleset_version": "BROOKS_SIMULATION_RULESET_V0_1",
        }
        cfg = {
            "phase_e1_context_v03_attempt_id": "3defa3de-d699-424a-8ceb-78020b453284",
            "phase_e1_simulation_v03_attempt_id": "8de2e63f-99f8-46ed-a2ec-f5b8c321c651",
            "phase6b_context_attempt_id": "4948b422-957d-487d-a699-c4644bd937bf",
            "phase7_simulation_attempt_id": "3a35b267-e48b-4259-a888-9c71579571b7",
        }
        attempts = resolve_attempt_chain(PHASE_E1_VALIDATION_RUN_ID, {}, cfg, diagnostic_legacy=True)
        self.assertEqual(attempts["context_attempt_id"], cfg["phase_e1_context_v03_attempt_id"])
        self.assertEqual(attempts["simulation_attempt_id"], cfg["phase_e1_simulation_v03_attempt_id"])
        self.assertEqual(attempts["context_ruleset"], "BROOKS_CONTEXT_RULESET_V0_3")

    def test_merge_supplements_adds_pm_v01_chain(self):
        alts = _merge_review_chain_supplements(PHASE_E1_VALIDATION_RUN_ID, [])
        sim_ids = [a["simulation_attempt_id"] for a in alts]
        self.assertIn(PM_V01_CERT_SIMULATION_ATTEMPT_ID, sim_ids)
        pm = next(a for a in alts if a["simulation_attempt_id"] == PM_V01_CERT_SIMULATION_ATTEMPT_ID)
        self.assertIn("canonical PM certification", pm["label"])
        self.assertTrue(pm.get("canonical_pm_certification"))

    def test_obsolete_pm_not_canonical_in_finalize(self):
        alts = _finalize_review_chain_alternatives(
            [
                {
                    "simulation_attempt_id": PM_V01_OBSOLETE_CERT_SIMULATION_ATTEMPT_ID,
                    "label": "x",
                    "simulation_ruleset": "BROOKS_POSITION_MANAGEMENT_RULESET_V0_1",
                }
            ]
        )
        self.assertTrue(alts[0].get("obsolete_certification"))
        self.assertFalse(alts[0].get("canonical_pm_certification"))

    def test_override_requires_both_ids(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_attempt_chain(
                "run-1",
                {},
                {},
                context_attempt_id="ctx-only",
                simulation_attempt_id=None,
            )
        self.assertIn("Both context_attempt_id and simulation_attempt_id", str(ctx.exception))

    @patch("app.brooks_intraday.learning_view.load_simulation_attempt")
    @patch("app.brooks_intraday.learning_view._load_context_attempt_meta")
    def test_validate_same_run_completed_linked(self, mock_ctx, mock_sim):
        mock_ctx.return_value = {
            "context_attempt_id": "db054c9a-5a28-43af-b0aa-74e92606902e",
            "run_id": "6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e",
            "context_ruleset_version": "BROOKS_CONTEXT_RULESET_V0_3",
            "status": "COMPLETED",
            "objective_attempt_id": "obj-1",
            "pattern_attempt_id": "pat-1",
            "notes": "phase_e",
        }
        mock_sim.return_value = {
            "simulation_attempt_id": "b3be6da1-ee4a-4b11-970d-dad7fee43bb2",
            "run_id": "6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e",
            "context_attempt_id": "db054c9a-5a28-43af-b0aa-74e92606902e",
            "status": "COMPLETED",
            "simulation_ruleset_version": "BROOKS_SIMULATION_RULESET_V0_1",
            "trade_count": 1,
            "realized_pnl": 3.76,
        }
        validated = validate_review_attempt_override(
            run_id="6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e",
            context_attempt_id="db054c9a-5a28-43af-b0aa-74e92606902e",
            simulation_attempt_id="b3be6da1-ee4a-4b11-970d-dad7fee43bb2",
        )
        self.assertEqual(validated["context_ruleset"], "BROOKS_CONTEXT_RULESET_V0_3")
        self.assertEqual(validated["trade_count"], 1)
        self.assertAlmostEqual(validated["realized_pnl"], 3.76)

    @patch("app.brooks_intraday.learning_view.load_simulation_attempt")
    @patch("app.brooks_intraday.learning_view._load_context_attempt_meta")
    def test_validate_rejects_wrong_run(self, mock_ctx, mock_sim):
        mock_ctx.return_value = {
            "run_id": "other-run",
            "status": "COMPLETED",
            "context_ruleset_version": "BROOKS_CONTEXT_RULESET_V0_3",
        }
        with self.assertRaises(ValueError) as ctx:
            validate_review_attempt_override(
                run_id="6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e",
                context_attempt_id="db054c9a",
                simulation_attempt_id="b3be6da1",
            )
        self.assertIn("does not belong", str(ctx.exception))
        mock_sim.assert_not_called()

    @patch("app.brooks_intraday.learning_view.load_simulation_attempt")
    @patch("app.brooks_intraday.learning_view._load_context_attempt_meta")
    def test_resolve_override_sets_flag_and_ruleset(self, mock_ctx, mock_sim):
        mock_ctx.return_value = {
            "context_attempt_id": "db054c9a-5a28-43af-b0aa-74e92606902e",
            "run_id": "6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e",
            "context_ruleset_version": "BROOKS_CONTEXT_RULESET_V0_3",
            "status": "COMPLETED",
            "objective_attempt_id": "obj-1",
            "pattern_attempt_id": "pat-1",
        }
        mock_sim.return_value = {
            "simulation_attempt_id": "b3be6da1-ee4a-4b11-970d-dad7fee43bb2",
            "run_id": "6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e",
            "context_attempt_id": "db054c9a-5a28-43af-b0aa-74e92606902e",
            "status": "COMPLETED",
            "simulation_ruleset_version": "BROOKS_SIMULATION_RULESET_V0_1",
            "trade_count": 1,
            "realized_pnl": 3.76,
        }
        attempts = resolve_attempt_chain(
            "6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e",
            {
                "phase6b_context_attempt_id": "4948b422-official-ctx",
                "phase7_simulation_attempt_id": "3a35b267-official-sim",
            },
            {},
            context_attempt_id="db054c9a-5a28-43af-b0aa-74e92606902e",
            simulation_attempt_id="b3be6da1-ee4a-4b11-970d-dad7fee43bb2",
            diagnostic_legacy=True,
        )
        self.assertTrue(attempts["review_override"])
        self.assertEqual(attempts["context_attempt_id"], "db054c9a-5a28-43af-b0aa-74e92606902e")
        self.assertEqual(attempts["simulation_attempt_id"], "b3be6da1-ee4a-4b11-970d-dad7fee43bb2")
        self.assertEqual(attempts["context_ruleset"], "BROOKS_CONTEXT_RULESET_V0_3")
        # Official pins in state are not overwritten by this function (read-only return)


if __name__ == "__main__":
    unittest.main()

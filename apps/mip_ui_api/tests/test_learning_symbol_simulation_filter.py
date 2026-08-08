"""Symbol-scoped simulation overlays and review-chain canonical PM labeling."""

from __future__ import annotations

import unittest

from app.brooks_intraday.learning_constants import (
    PM_V01_CERT_SIMULATION_ATTEMPT_ID,
    PM_V01_OBSOLETE_CERT_SIMULATION_ATTEMPT_ID,
)
from app.brooks_intraday.learning_view import (
    _finalize_review_chain_alternatives,
    _simulation_effect_for_bar,
)


class SimulationEffectSymbolFilterTests(unittest.TestCase):
    def test_aapl_grid_no_amzn_entry(self):
        trades = [
            {
                "symbol": "AMZN",
                "quantity": 4,
                "entry_ts": "2026-07-13T14:25:00",
                "exit_ts": "2026-07-13T15:45:00",
                "exit_reason": "PROTECTIVE_STOP_HIT",
            }
        ]
        eff = _simulation_effect_for_bar(
            "2026-07-13T14:25:00",
            trades,
            [],
            symbol="AAPL",
        )
        self.assertEqual(eff, "—")

    def test_amzn_grid_shows_amzn_entry(self):
        trades = [
            {
                "symbol": "AMZN",
                "quantity": 4,
                "entry_ts": "2026-07-13T14:25:00",
            }
        ]
        eff = _simulation_effect_for_bar(
            "2026-07-13T14:25:00",
            trades,
            [],
            symbol="AMZN",
        )
        self.assertIn("ENTRY AMZN", eff)


class ReviewChainCanonicalPmTests(unittest.TestCase):
    def test_obsolete_not_marked_canonical(self):
        alts = _finalize_review_chain_alternatives(
            [
                {
                    "simulation_attempt_id": PM_V01_OBSOLETE_CERT_SIMULATION_ATTEMPT_ID,
                    "context_attempt_id": "3defa3de-d699-424a-8ceb-78020b453284",
                    "label": "PM old",
                    "simulation_ruleset": "BROOKS_POSITION_MANAGEMENT_RULESET_V0_1",
                },
                {
                    "simulation_attempt_id": PM_V01_CERT_SIMULATION_ATTEMPT_ID,
                    "context_attempt_id": "3defa3de-d699-424a-8ceb-78020b453284",
                    "label": "PM new",
                    "simulation_ruleset": "BROOKS_POSITION_MANAGEMENT_RULESET_V0_1",
                },
            ]
        )
        canonical = next(a for a in alts if a["simulation_attempt_id"] == PM_V01_CERT_SIMULATION_ATTEMPT_ID)
        obsolete = next(a for a in alts if a["simulation_attempt_id"] == PM_V01_OBSOLETE_CERT_SIMULATION_ATTEMPT_ID)
        self.assertTrue(canonical.get("canonical_pm_certification"))
        self.assertTrue(obsolete.get("obsolete_certification"))
        self.assertFalse(obsolete.get("canonical_pm_certification"))
        self.assertIn("obsolete certification", obsolete["label"].lower())
        self.assertEqual(alts[0]["simulation_attempt_id"], PM_V01_CERT_SIMULATION_ATTEMPT_ID)


if __name__ == "__main__":
    unittest.main()

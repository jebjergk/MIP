"""Trade browser must honor review-only attempt-chain overrides."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from app.brooks_intraday.context_phase6b_audit import parse_ui_locator, ui_locator
from app.brooks_intraday.experiment_comparison import list_trades_filtered

RUN_ID = "6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e"
OFFICIAL_CTX = "4948b422-957d-487d-a699-c4644bd937bf"
OFFICIAL_SIM = "3a35b267-e48b-4259-a888-9c71579571b7"
ALT_CTX = "db054c9a-5a28-43af-b0aa-74e92606902e"
ALT_SIM = "b3be6da1-ee4a-4b11-970d-dad7fee43bb2"


class TradeBrowserReviewOverrideTests(unittest.TestCase):
    def test_locator_includes_and_parses_attempt_ids(self):
        loc = ui_locator(
            RUN_ID,
            "AMZN",
            "2026-07-13T14:20:00",
            context_attempt_id=ALT_CTX,
            simulation_attempt_id=ALT_SIM,
        )
        self.assertIn(f"context_attempt_id={ALT_CTX}", loc)
        self.assertIn(f"simulation_attempt_id={ALT_SIM}", loc)
        parsed = parse_ui_locator(loc)
        self.assertEqual(parsed["run_id"], RUN_ID)
        self.assertEqual(parsed["symbol"], "AMZN")
        self.assertEqual(parsed["bar_ts"], "2026-07-13T14:20:00")
        self.assertEqual(parsed["context_attempt_id"], ALT_CTX)
        self.assertEqual(parsed["simulation_attempt_id"], ALT_SIM)

    @patch("app.brooks_intraday.learning_view.validate_review_attempt_override")
    @patch("app.brooks_intraday.experiment_analytics.build_trade_reviews")
    @patch("app.brooks_intraday.experiment_analytics._run_config")
    @patch("app.brooks_intraday.store.get_run")
    def test_official_chain_empty_then_alternate_amzn_then_back(
        self,
        mock_get_run,
        mock_cfg,
        mock_build,
        mock_validate,
    ):
        mock_get_run.return_value = type("R", (), {"selected_week_start": "2026-07-13"})()
        mock_cfg.return_value = {
            "phase6b_context_attempt_id": OFFICIAL_CTX,
            "phase7_simulation_attempt_id": OFFICIAL_SIM,
            "selected_week_start": "2026-07-13",
        }

        def _build(run_id, week, *, simulation_attempt_id=None, context_attempt_id=None):
            sim = simulation_attempt_id or OFFICIAL_SIM
            ctx = context_attempt_id or OFFICIAL_CTX
            if sim == OFFICIAL_SIM:
                return []
            return [
                {
                    "week": week,
                    "symbol": "AMZN",
                    "entry_timestamp": "2026-07-13T14:20:00",
                    "realized_pnl": 3.76,
                    "context_attempt_id": ctx,
                    "simulation_attempt_id": sim,
                    "ui_locator": ui_locator(
                        run_id,
                        "AMZN",
                        "2026-07-13T14:20:00",
                        context_attempt_id=ctx,
                        simulation_attempt_id=sim,
                    ),
                }
            ]

        mock_build.side_effect = _build

        official = list_trades_filtered(run_id=RUN_ID)
        self.assertEqual(official["count"], 0)
        self.assertFalse(official["review_override"])
        self.assertEqual(mock_build.call_args.kwargs.get("simulation_attempt_id"), None)

        alternate = list_trades_filtered(
            run_id=RUN_ID,
            context_attempt_id=ALT_CTX,
            simulation_attempt_id=ALT_SIM,
        )
        self.assertEqual(alternate["count"], 1)
        self.assertTrue(alternate["review_override"])
        self.assertEqual(alternate["trades"][0]["symbol"], "AMZN")
        self.assertEqual(alternate["trades"][0]["simulation_attempt_id"], ALT_SIM)
        self.assertAlmostEqual(float(alternate["trades"][0]["realized_pnl"]), 3.76)
        mock_validate.assert_called_with(
            run_id=RUN_ID,
            context_attempt_id=ALT_CTX,
            simulation_attempt_id=ALT_SIM,
        )

        loc = parse_ui_locator(alternate["trades"][0]["ui_locator"])
        self.assertEqual(loc["context_attempt_id"], ALT_CTX)
        self.assertEqual(loc["simulation_attempt_id"], ALT_SIM)
        self.assertEqual(loc["symbol"], "AMZN")

        back = list_trades_filtered(run_id=RUN_ID)
        self.assertEqual(back["count"], 0)
        self.assertFalse(back["review_override"])

    def test_override_requires_run_id(self):
        with self.assertRaises(ValueError):
            list_trades_filtered(
                context_attempt_id=ALT_CTX,
                simulation_attempt_id=ALT_SIM,
            )


if __name__ == "__main__":
    unittest.main()

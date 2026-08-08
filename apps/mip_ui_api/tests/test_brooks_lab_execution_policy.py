"""Guards block DIAGNOSTIC_LEGACY bulk on Adviser-foundation runs."""

from __future__ import annotations

import unittest

from app.brooks_intraday.errors import BrooksIntradayError
from app.brooks_intraday.lab_execution_policy import (
    LAB_EXECUTION_MODE_ADVISER_FOUNDATION,
    LAB_EXECUTION_MODE_DIAGNOSTIC_LEGACY,
    adviser_foundation_defaults,
    require_diagnostic_legacy,
)
from app.brooks_intraday import store
from app.brooks_intraday.models import CreateRunRequest


class LabExecutionPolicyTests(unittest.TestCase):
    def test_new_run_defaults_adviser_foundation(self):
        run = store.create_run(
            CreateRunRequest(
                symbols=["AAPL", "AMZN", "JPM", "MCD"],
                created_by="test",
            )
        )
        cfg = run.configuration or {}
        self.assertEqual(cfg.get("lab_execution_mode"), LAB_EXECUTION_MODE_ADVISER_FOUNDATION)
        self.assertIn("lab_pipeline_profile", cfg)

    def test_bulk_context_blocked_without_opt_in(self):
        run = store.create_run(
            CreateRunRequest(symbols=["AAPL", "AMZN", "JPM", "MCD"], created_by="test")
        )
        with self.assertRaises(BrooksIntradayError) as ctx:
            store.run_context_bulk_v03(run.run_id)
        self.assertEqual(ctx.exception.code, "DIAGNOSTIC_LEGACY_REQUIRED")
        self.assertEqual(ctx.exception.status_code, 403)

    def test_bulk_allowed_with_query_override(self):
        run = store.create_run(
            CreateRunRequest(symbols=["AAPL", "AMZN", "JPM", "MCD"], created_by="test")
        )
        # Will fail later (not prepared) but must pass policy gate first.
        try:
            store.run_context_bulk_v03(run.run_id, allow_diagnostic_legacy=True)
        except BrooksIntradayError as exc:
            self.assertNotEqual(exc.code, "DIAGNOSTIC_LEGACY_REQUIRED")

    def test_diagnostic_legacy_mode_run_skips_gate(self):
        state = {
            "run_id": "r1",
            "configuration": {
                **adviser_foundation_defaults(),
                "lab_execution_mode": LAB_EXECUTION_MODE_DIAGNOSTIC_LEGACY,
            },
        }
        require_diagnostic_legacy(state, "phase6_context_bulk_v04")


if __name__ == "__main__":
    unittest.main()

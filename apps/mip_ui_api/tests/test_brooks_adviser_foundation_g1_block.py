"""Foundation mode blocks legacy trade-learning without diagnostic flag."""

from __future__ import annotations

import unittest

from app.brooks_intraday.learning_constants import (
    PHASE_E1_CONTEXT_ATTEMPT_ID,
    PHASE_E1_SIMULATION_ATTEMPT_ID,
    PHASE_E1_VALIDATION_RUN_ID,
)
from app.brooks_intraday.trade_learning_g1 import build_trade_learning_g1_payload


class AdviserFoundationG1BlockTests(unittest.TestCase):
    def test_legacy_g1_blocked_in_foundation_mode(self):
        with self.assertRaises(ValueError) as ctx:
            build_trade_learning_g1_payload(
                run_id=PHASE_E1_VALIDATION_RUN_ID,
                state={},
                cfg={},
                context_attempt_id=PHASE_E1_CONTEXT_ATTEMPT_ID,
                simulation_attempt_id=PHASE_E1_SIMULATION_ATTEMPT_ID,
                symbol="AMZN",
                trading_date="2026-07-13",
                diagnostic_legacy=False,
            )
        self.assertIn("diagnostic_legacy", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()

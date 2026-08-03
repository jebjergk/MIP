"""Phase 7B — simulation certification fixtures."""

from __future__ import annotations

import unittest

from app.brooks_intraday.simulation_certification import (
    ALL_SCENARIOS,
    HISTORICAL_SIMULATION,
    run_certification,
)


class Phase7BCertificationTests(unittest.TestCase):
    def test_all_fixtures_pass(self):
        report = run_certification()
        failures = [f["fixture"] for f in report["fixtures"] if not f["pass"]]
        self.assertEqual(failures, [], msg=f"Failed fixtures: {failures}\n{report}")

    def test_each_scenario_callable(self):
        self.assertEqual(len(ALL_SCENARIOS), 14)

    def test_historical_attempt_id_constant(self):
        self.assertEqual(HISTORICAL_SIMULATION, "4bd264d3-c061-4bee-8534-c4eb99532438")


if __name__ == "__main__":
    unittest.main()

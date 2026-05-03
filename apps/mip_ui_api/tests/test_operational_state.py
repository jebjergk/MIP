"""Golden tests for cockpit operational_state derivation (API-only contract)."""

import unittest

from app.services.cockpit.operational_state import (
    ACTIONABLE_PROPOSAL,
    EXECUTED,
    INVALIDATED,
    MONITOR,
    NOT_ACTIONABLE,
    UNKNOWN,
    derive_operational_state,
)


class OperationalStateTests(unittest.TestCase):
    def test_executed_wins_before_invalidated(self):
        self.assertEqual(
            derive_operational_state(
                latest_board_action="PROPOSE_LONG",
                phase4_health_present=True,
                executed=True,
                live_invalidated=True,
            ),
            EXECUTED,
        )

    def test_live_invalidated_before_phase4_mapping(self):
        self.assertEqual(
            derive_operational_state(
                latest_board_action="PROPOSE_LONG",
                phase4_health_present=True,
                executed=False,
                live_invalidated=True,
            ),
            INVALIDATED,
        )

    def test_watch_long_failure_is_monitor_not_actionable(self):
        self.assertEqual(
            derive_operational_state(
                latest_board_action="WATCH_LONG_FAILURE",
                phase4_health_present=True,
                executed=False,
                live_invalidated=False,
            ),
            MONITOR,
        )

    def test_no_trade_maps_not_actionable(self):
        self.assertEqual(
            derive_operational_state(
                latest_board_action="NO_TRADE",
                phase4_health_present=True,
                executed=False,
                live_invalidated=False,
            ),
            NOT_ACTIONABLE,
        )

    def test_reject_maps_not_actionable(self):
        self.assertEqual(
            derive_operational_state(
                latest_board_action="REJECT",
                phase4_health_present=True,
                executed=False,
                live_invalidated=False,
            ),
            NOT_ACTIONABLE,
        )

    def test_missing_phase4_is_unknown_even_if_propose_action_supplied(self):
        self.assertEqual(
            derive_operational_state(
                latest_board_action="PROPOSE_LONG",
                phase4_health_present=False,
                executed=False,
                live_invalidated=False,
            ),
            UNKNOWN,
        )

    def test_actionable_invariant_no_watch_with_actionable(self):
        """Phase-4 watch-family actions never yield ACTIONABLE_PROPOSAL."""
        watch_family = (
            "WATCH_LONG",
            "WATCH_SHORT",
            "WATCH_LONG_FAILURE",
            "WATCH_SHORT_FAILURE",
            "WAIT_FOR_CONFIRMATION",
        )
        for act in watch_family:
            with self.subTest(action=act):
                self.assertNotEqual(
                    derive_operational_state(
                        latest_board_action=act,
                        phase4_health_present=True,
                        executed=False,
                        live_invalidated=False,
                    ),
                    ACTIONABLE_PROPOSAL,
                )


if __name__ == "__main__":
    unittest.main()

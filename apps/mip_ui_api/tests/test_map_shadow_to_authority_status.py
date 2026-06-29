"""Tests for chair-complete degraded shadow sessions."""

import unittest

from app.committee.agentic_authority import (
    AGENTIC_DEFER,
    AGENTIC_DEGRADED_NO_AUTHORITY,
    AGENTIC_REJECT,
    map_shadow_to_authority_status,
)


class MapShadowToAuthorityStatusTests(unittest.TestCase):
    def test_degraded_chair_deny_maps_to_reject(self):
        session = {
            "status": "DEGRADED",
            "degraded": True,
            "stage_reached": 5,
            "shadow_stance": "DENY",
            "shadow_confidence": 0.76,
            "pack_version": "2.0.0",
        }
        status, reason = map_shadow_to_authority_status(session, min_confidence_threshold=0.4)
        self.assertEqual(status, AGENTIC_REJECT)
        self.assertEqual(reason, "SHADOW_DEGRADED_CHAIR_DENY")

    def test_degraded_incomplete_stage_still_blocks(self):
        session = {
            "status": "DEGRADED",
            "degraded": True,
            "stage_reached": 3,
            "shadow_stance": "DEFER",
            "shadow_confidence": 0.0,
            "pack_version": "2.0.0",
        }
        status, reason = map_shadow_to_authority_status(session, min_confidence_threshold=0.4)
        self.assertEqual(status, AGENTIC_DEGRADED_NO_AUTHORITY)
        self.assertEqual(reason, "STAGE_REACHED_3")

    def test_degraded_chair_defer_maps_to_defer(self):
        session = {
            "status": "DEGRADED",
            "degraded": True,
            "stage_reached": 5,
            "shadow_stance": "DEFER",
            "shadow_confidence": 0.0,
            "pack_version": "2.0.0",
        }
        status, reason = map_shadow_to_authority_status(session, min_confidence_threshold=0.4)
        self.assertEqual(status, AGENTIC_DEFER)
        self.assertEqual(reason, "SHADOW_DEGRADED_CHAIR_DEFER")


if __name__ == "__main__":
    unittest.main()

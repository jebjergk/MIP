"""Phase 5C — auto-promote eligibility for DEGRADED chair APPROVE / APPROVE_REDUCED."""

import unittest

from app.committee.agentic_authority import (
    AGENTIC_APPROVE,
    AGENTIC_APPROVE_REDUCED,
    _auto_promote_eligible,
)


class TestAutoPromoteEligible(unittest.TestCase):
    def _base_audit(self, **overrides):
        row = {
            "committed": True,
            "authority_status": AGENTIC_APPROVE_REDUCED,
            "is_stale": False,
            "pack_version_ok": True,
            "shadow_degraded": True,
            "action_id": "act-1",
            "session_id": "sess-1",
            "hearing_id": "hear-1",
        }
        row.update(overrides)
        return row

    def test_degraded_approve_reduced_still_eligible(self):
        eligible, reason = _auto_promote_eligible(self._base_audit())
        self.assertTrue(eligible)
        self.assertEqual(reason, "ELIGIBLE")

    def test_degraded_full_approve_still_eligible(self):
        eligible, reason = _auto_promote_eligible(
            self._base_audit(authority_status=AGENTIC_APPROVE),
        )
        self.assertTrue(eligible)
        self.assertEqual(reason, "ELIGIBLE")

    def test_non_approving_status_still_blocked(self):
        eligible, reason = _auto_promote_eligible(
            self._base_audit(authority_status="AGENTIC_DEFER"),
        )
        self.assertFalse(eligible)
        self.assertTrue(str(reason).startswith("NON_APPROVING_STATUS"))


if __name__ == "__main__":
    unittest.main()

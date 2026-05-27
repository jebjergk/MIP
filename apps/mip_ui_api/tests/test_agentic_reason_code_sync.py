"""Agentic authority reason-code sync — stale LPA tag cleanup."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.committee.agentic_authority import (
    AGENTIC_APPROVE_REDUCED,
    AGENTIC_FAILED_NO_AUTHORITY,
    GATE_REASON_STALE,
    reconcile_agentic_authority_reason_codes,
)


class TestReconcileAgenticReasonCodes(unittest.TestCase):
    def test_clears_stale_failed_when_gate_ok(self):
        existing = [
            "REVALIDATION_PRICE_FROM_IBKR_DIRECT",
            "AGENTIC_AUTHORITY_AGENTIC_FAILED_NO_AUTHORITY",
            GATE_REASON_STALE,
            "LIVE_RISK_REWARD_TOO_LOW",
        ]
        merged = reconcile_agentic_authority_reason_codes(
            existing,
            authority_status=AGENTIC_APPROVE_REDUCED,
            is_stale=False,
            gate_ok=True,
        )
        self.assertIn("REVALIDATION_PRICE_FROM_IBKR_DIRECT", merged)
        self.assertNotIn("AGENTIC_AUTHORITY_AGENTIC_FAILED_NO_AUTHORITY", merged)
        self.assertNotIn(GATE_REASON_STALE, merged)
        self.assertIn("LIVE_RISK_REWARD_TOO_LOW", merged)
        self.assertIn("AGENTIC_AUTHORITY_AGENTIC_APPROVE_REDUCED", merged)
        self.assertIn("STRUCTURAL_AGENTIC_REVIEWED", merged)
        self.assertIn("AGENTIC_SIZE_POSTURE_REDUCED", merged)

    def test_keeps_block_tags_when_gate_not_ok(self):
        existing = ["REVALIDATION_PRICE_FROM_IBKR_DIRECT"]
        merged = reconcile_agentic_authority_reason_codes(
            existing,
            authority_status=AGENTIC_FAILED_NO_AUTHORITY,
            is_stale=True,
            gate_ok=False,
        )
        self.assertIn("AGENTIC_AUTHORITY_AGENTIC_FAILED_NO_AUTHORITY", merged)
        self.assertIn(GATE_REASON_STALE, merged)

    def test_bracket_codes_preserved_on_positive_sync(self):
        existing = ["STRUCT_SUBMIT_CONTRACT_INCOMPLETE", "IBKR_BAR_FETCH_INSTRUMENTATION_V1"]
        merged = reconcile_agentic_authority_reason_codes(
            existing,
            authority_status=AGENTIC_APPROVE_REDUCED,
            is_stale=False,
            gate_ok=True,
        )
        self.assertIn("STRUCT_SUBMIT_CONTRACT_INCOMPLETE", merged)
        self.assertIn("IBKR_BAR_FETCH_INSTRUMENTATION_V1", merged)


if __name__ == "__main__":
    unittest.main()

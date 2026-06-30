"""Chair plurality fallback when SHADOW_CHAIR_AGENT returns empty JSON."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.committee.shadow_types import (
    SpecialistPosition,
    synthesize_chair_plurality_fallback,
    specialists_ready_for_plurality_fallback,
)


def _pos(role: str, stance: str, confidence: float = 0.7) -> SpecialistPosition:
    return SpecialistPosition(
        role=role,
        stance=stance,
        confidence=confidence,
        rationale=f"{role} rationale",
        evidence_used=[],
    )


class TestChairPluralityFallback(unittest.TestCase):
    def test_five_approve_reduced_one_wait_reclaim(self):
        positions = {
            "STRUCTURAL_THESIS": _pos("STRUCTURAL_THESIS", "APPROVE_REDUCED", 0.62),
            "ENTRY_GEOMETRY": _pos("ENTRY_GEOMETRY", "WAIT_RECLAIM", 0.75),
            "REGIME": _pos("REGIME", "APPROVE_REDUCED", 0.72),
            "PATH_TRADEABILITY": _pos("PATH_TRADEABILITY", "APPROVE_REDUCED", 0.72),
            "PROTECTION_EXIT": _pos("PROTECTION_EXIT", "APPROVE_REDUCED", 0.75),
            "SYMBOL_BEHAVIOR": _pos("SYMBOL_BEHAVIOR", "APPROVE_REDUCED", 0.70),
        }
        self.assertTrue(specialists_ready_for_plurality_fallback(positions))
        ruling = synthesize_chair_plurality_fallback(positions, [], [])
        self.assertEqual(ruling.shadow_stance, "APPROVE_REDUCED")
        self.assertTrue(ruling.parse_ok)
        self.assertFalse(ruling.degraded)
        self.assertIn("Plurality fallback", ruling.plurality_basis)

    def test_not_ready_when_specialist_missing(self):
        positions = {"REGIME": _pos("REGIME", "APPROVE")}
        self.assertFalse(specialists_ready_for_plurality_fallback(positions))


if __name__ == "__main__":
    unittest.main()

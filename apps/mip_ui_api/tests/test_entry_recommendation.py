import unittest

from app.price_action.entry_recommendation import build_entry_recommendation


def _geometry(support=(98.0, 100.0), resistance=(108.0, 110.0)):
    return {
        "support_zones": [{"lower": support[0], "upper": support[1]}],
        "resistance_zones": [{"lower": resistance[0], "upper": resistance[1]}],
    }


class EntryRecommendationTests(unittest.TestCase):
    def test_reject_verdict_blocks_entry(self):
        result = build_entry_recommendation(
            daily_verdict="REJECT",
            daily_confidence=0.8,
            geometry=_geometry(),
            situation={},
            intraday=None,
            position=None,
        )
        self.assertEqual(result["recommendation"], "INVALIDATED")
        self.assertIn("advisory only", result["limitations"])

    def test_long_approve_with_supportive_intraday(self):
        result = build_entry_recommendation(
            daily_verdict="LONG_APPROVE",
            daily_confidence=0.82,
            geometry=_geometry(),
            situation={},
            intraday={"enabled": True, "verdict": "INTRADAY_SUPPORTIVE", "effect": "Supportive."},
            position=None,
        )
        self.assertEqual(result["recommendation"], "CONSIDER_ENTRY_NOW")

    def test_long_approve_with_invalidating_intraday(self):
        result = build_entry_recommendation(
            daily_verdict="LONG_APPROVE",
            daily_confidence=0.82,
            geometry=_geometry(),
            situation={},
            intraday={"enabled": True, "verdict": "INTRADAY_INVALIDATING", "effect": "Bad timing."},
            position=None,
        )
        self.assertEqual(result["recommendation"], "DO_NOT_ENTER")

    def test_reduced_size_when_intraday_supportive(self):
        result = build_entry_recommendation(
            daily_verdict="LONG_APPROVE_REDUCED",
            daily_confidence=0.6,
            geometry=_geometry(),
            situation={},
            intraday={"enabled": True, "verdict": "INTRADAY_SUPPORTIVE", "effect": "Supportive."},
            position=None,
        )
        self.assertEqual(result["recommendation"], "CONSIDER_ENTRY_REDUCED_SIZE")

    def test_wait_reclaim_stays_wait(self):
        result = build_entry_recommendation(
            daily_verdict="WAIT_RECLAIM",
            daily_confidence=0.55,
            geometry=_geometry(),
            situation={},
            intraday={"enabled": True, "verdict": "INTRADAY_NEUTRAL", "effect": "Neutral."},
            position=None,
        )
        self.assertEqual(result["recommendation"], "WAIT_FOR_RECLAIM")

    def test_existing_position_downgrades_new_entry(self):
        result = build_entry_recommendation(
            daily_verdict="LONG_APPROVE",
            daily_confidence=0.82,
            geometry=_geometry(),
            situation={},
            intraday=None,
            position={"has_position": True, "position_effect": "Already long."},
        )
        self.assertEqual(result["recommendation"], "HOLD_ONLY_IF_ALREADY_IN_POSITION")


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

# Add app root to path for direct module imports.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.live_execution_utils import is_close_like_execution, to_dt_utc
from app.services.live_intelligence.structural_committee import build_structural_exit_execution_only_verdict


class TestLiveExecutionRegression(unittest.TestCase):
    def test_exit_execution_is_close_like(self):
        self.assertTrue(is_close_like_execution("EXIT", "BUY", "BUY"))
        self.assertTrue(is_close_like_execution("EXIT", "SELL", "SELL"))

    def test_entry_flip_side_is_close_like(self):
        self.assertTrue(is_close_like_execution("ENTRY", "BUY", "SELL"))
        self.assertTrue(is_close_like_execution("ENTRY", "SELL", "BUY"))

    def test_entry_same_side_is_not_close_like(self):
        self.assertFalse(is_close_like_execution("ENTRY", "BUY", "BUY"))
        self.assertFalse(is_close_like_execution("ENTRY", "SELL", "SELL"))

    def test_unknown_intent_is_not_close_like(self):
        self.assertFalse(is_close_like_execution(None, "BUY", "SELL"))
        self.assertFalse(is_close_like_execution("HOLD", "BUY", "SELL"))

    def test_to_dt_utc_parses_iso_z(self):
        parsed = to_dt_utc("2026-03-18T18:09:50+00:00")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.tzinfo, timezone.utc)
        self.assertEqual(parsed.isoformat(), "2026-03-18T18:09:50+00:00")

    def test_to_dt_utc_keeps_aware_datetime_in_utc(self):
        aware = datetime(2026, 3, 18, 18, 9, 50, tzinfo=timezone.utc)
        parsed = to_dt_utc(aware)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.tzinfo, timezone.utc)
        self.assertEqual(parsed.isoformat(), "2026-03-18T18:09:50+00:00")

    def test_structural_exit_execution_only_proceeds_when_position(self):
        action = {"ACTION_INTENT": "EXIT", "EXIT_STYLE": "MKT", "MAX_HOLD_BARS": 5}
        v = build_structural_exit_execution_only_verdict(action, exit_position_qty=100.0)
        self.assertFalse(v["blocked"])
        self.assertEqual(v["recommendation"], "PROCEED")
        self.assertIn("STRUCTURAL_EXIT_EXECUTION_ONLY", v["reason_codes"])
        self.assertNotIn("EXIT_POSITION_MISSING", v["reason_codes"])
        jd = v["joint_decision"]
        self.assertTrue(jd.get("should_execute_exit"))
        self.assertEqual(v.get("committee_model"), "STRUCTURAL_EXIT_EXECUTION_ONLY")

    def test_structural_exit_execution_only_blocks_when_flat(self):
        action = {"ACTION_INTENT": "EXIT"}
        v = build_structural_exit_execution_only_verdict(action, exit_position_qty=0.0)
        self.assertTrue(v["blocked"])
        self.assertEqual(v["recommendation"], "BLOCK")
        self.assertIn("EXIT_POSITION_MISSING", v["reason_codes"])
        self.assertIn("STRUCTURAL_EXIT_EXECUTION_ONLY", v["reason_codes"])
        self.assertFalse(v["joint_decision"].get("should_execute_exit"))


if __name__ == "__main__":
    unittest.main()

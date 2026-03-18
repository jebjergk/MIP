import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

# Add app root to path for direct module imports.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.live_execution_utils import is_close_like_execution, to_dt_utc


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


if __name__ == "__main__":
    unittest.main()

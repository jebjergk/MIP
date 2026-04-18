"""Unit tests for Phase 2 public disclosure context (deterministic rules, no Snowflake)."""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.committee.public_disclosure_context import (  # noqa: E402
    build_exhibit_public_disclosure_context,
    compute_tone_vs_trade,
)


class TestToneVsTrade(unittest.TestCase):
    def test_unknown_direction(self):
        self.assertEqual(compute_tone_vs_trade("", ["BUY"]), "UNKNOWN")
        self.assertEqual(compute_tone_vs_trade("FLAT", ["BUY"]), "UNKNOWN")

    def test_long_buy_heavy_supportive(self):
        self.assertEqual(compute_tone_vs_trade("LONG", ["BUY", "BUY", "SELL"]), "SUPPORTIVE")

    def test_long_sell_heavy_contradictory(self):
        self.assertEqual(compute_tone_vs_trade("LONG", ["SELL", "SELL", "BUY"]), "CONTRADICTORY")

    def test_short_sell_heavy_supportive(self):
        self.assertEqual(compute_tone_vs_trade("SHORT", ["SELL", "SELL", "BUY"]), "SUPPORTIVE")

    def test_short_buy_heavy_contradictory(self):
        self.assertEqual(compute_tone_vs_trade("SHORT", ["BUY", "BUY", "SELL"]), "CONTRADICTORY")

    def test_neutral_when_equal(self):
        self.assertEqual(compute_tone_vs_trade("LONG", ["BUY", "SELL"]), "NEUTRAL")

    def test_unknown_when_no_usable_sides(self):
        self.assertEqual(compute_tone_vs_trade("LONG", ["", "EXCHANGE"]), "UNKNOWN")

    def test_purchase_and_sale_normalized(self):
        self.assertEqual(compute_tone_vs_trade("LONG", ["PURCHASE", "SALE"]), "NEUTRAL")


class TestBuildExhibit(unittest.TestCase):
    def test_flag_off_returns_none(self):
        cur = MagicMock()
        with patch(
            "app.committee.public_disclosure_context.fetch_all",
            return_value=[{"CONFIG_VALUE": "false"}],
        ):
            out = build_exhibit_public_disclosure_context(cur, {"SYMBOL": "ABC", "DIRECTION": "LONG"})
        self.assertIsNone(out)
        cur.execute.assert_called()

    def test_flag_on_empty_symbol(self):
        cur = MagicMock()
        with patch(
            "app.committee.public_disclosure_context.fetch_all",
            return_value=[{"CONFIG_VALUE": "true"}],
        ):
            out = build_exhibit_public_disclosure_context(cur, {"SYMBOL": "", "DIRECTION": "LONG"})
        self.assertIsNotNone(out)
        self.assertEqual(out["mapping_quality"], "UNMAPPED")
        self.assertEqual(out["tone_vs_trade"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()

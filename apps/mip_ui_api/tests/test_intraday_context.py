import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from app.price_action.intraday_context import build_intraday_context

_ET = ZoneInfo("America/New_York")


def _geometry(support=100.0, resistance=110.0):
    return {
        "support_zones": [{"lower": support - 1, "upper": support + 1}],
        "resistance_zones": [{"lower": resistance - 1, "upper": resistance + 1}],
    }


class IntradayContextTests(unittest.TestCase):
    def test_before_rth_returns_no_bars(self):
        before_open = datetime(2026, 7, 27, 8, 30, tzinfo=_ET)
        result = build_intraday_context("AAPL", _geometry(), now=before_open)
        self.assertEqual(result["verdict"], "NO_RTH_BARS_AVAILABLE")
        self.assertTrue(result["enabled"])

    @patch("app.price_action.intraday_context.fetch_rth_15m_bars")
    def test_below_support_is_invalidating(self, mock_fetch):
        mock_fetch.return_value = (
            [
                {"c": 101.0, "o": 101.0, "h": 101.5, "l": 100.5},
                {"c": 99.5, "o": 99.8, "h": 100.0, "l": 99.0},
                {"c": 98.0, "o": 98.5, "h": 98.8, "l": 97.8},
            ],
            {"status": "SUCCESS", "ib_connect": {"host": "127.0.0.1", "port": 7496}},
        )
        during_rth = datetime(2026, 7, 27, 11, 0, tzinfo=_ET)
        result = build_intraday_context("AAPL", _geometry(support=100.0), now=during_rth)
        self.assertEqual(result["verdict"], "INTRADAY_INVALIDATING")
        mock_fetch.assert_called_once_with("AAPL", "STOCK", portfolio_id=None)

    @patch("app.price_action.intraday_context.fetch_rth_15m_bars")
    def test_holding_support_with_higher_lows_is_supportive(self, mock_fetch):
        mock_fetch.return_value = (
            [{"c": 100.5}, {"c": 100.2}, {"c": 100.8}, {"c": 101.1}, {"c": 101.4}],
            {"status": "SUCCESS"},
        )
        during_rth = datetime(2026, 7, 27, 14, 0, tzinfo=_ET)
        result = build_intraday_context("AAPL", _geometry(support=100.0), now=during_rth)
        self.assertEqual(result["verdict"], "INTRADAY_SUPPORTIVE")

    @patch("app.price_action.intraday_context.fetch_rth_15m_bars")
    def test_fetch_failure_does_not_raise(self, mock_fetch):
        mock_fetch.return_value = (
            [],
            {"status": "FETCH_FAILED", "fetch_error": "connection refused", "ib_connect": {"host": "127.0.0.1", "port": 7497}},
        )
        during_rth = datetime(2026, 7, 27, 12, 0, tzinfo=_ET)
        result = build_intraday_context("AAPL", _geometry(), now=during_rth)
        self.assertEqual(result["verdict"], "NO_RTH_BARS_AVAILABLE")
        self.assertIn("IBKR", result["plain_language_context"])


if __name__ == "__main__":
    unittest.main()

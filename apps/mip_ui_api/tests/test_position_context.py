import unittest
from unittest.mock import MagicMock, patch

from app.price_action.position_context import load_position_context


class PositionContextTests(unittest.TestCase):
    @patch("app.price_action.position_context.get_connection")
    @patch("app.price_action.position_context.fetch_all")
    def test_loads_jnj_for_portfolio_two(self, mock_fetch_all, mock_get_connection):
        mock_fetch_all.return_value = [{
            "SYMBOL": "JNJ",
            "PORTFOLIO_ID": 2,
            "QUANTITY": 1.0,
            "AVG_COST": 263.86,
            "MARKET_VALUE": 263.6,
            "UNREALIZED_PNL": -0.26,
            "ENTRY_PRICE": 263.86,
        }]
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        result = load_position_context(
            "JNJ",
            portfolio_id=2,
            geometry={"support_zones": [{"lower": 260.0, "upper": 262.0}]},
            daily_verdict="LONG_APPROVE",
            intraday=None,
        )

        self.assertTrue(result["has_position"])
        self.assertEqual(result["position_direction"], "LONG")
        self.assertEqual(result["quantity"], 1.0)
        sql = mock_conn.cursor.return_value.execute.call_args[0][0]
        self.assertNotIn("DIRECTION", sql.upper())

    def test_no_portfolio_returns_guidance(self):
        result = load_position_context(
            "JNJ",
            portfolio_id=None,
            geometry={},
            daily_verdict="LONG_APPROVE",
            intraday=None,
        )
        self.assertFalse(result["has_position"])
        self.assertEqual(result["position_health"], "NO_ACTIVE_POSITION")


if __name__ == "__main__":
    unittest.main()

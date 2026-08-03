import unittest
from unittest.mock import MagicMock, patch

from app.services import ibkr_live_bars


class ResolveLiveBarsConnectTests(unittest.TestCase):
    @patch("app.services.ibkr_live_bars.get_live_bars_subprocess_args")
    @patch("app.services.ibkr_live_bars._preferred_market_data_gateway")
    def test_portfolio_agnostic_prefers_configured_live_gateway(self, mock_gateway, mock_env):
        mock_env.return_value = {
            "host": "127.0.0.1",
            "port": 7497,
            "client_id": 9436,
            "connect_timeout_sec": 10,
        }
        mock_gateway.return_value = {"host": "127.0.0.1", "port": 7496, "adapter_mode": "LIVE"}

        conn = ibkr_live_bars.resolve_live_bars_connect(None)

        self.assertEqual(conn["port"], 7496)
        mock_gateway.assert_called_once()

    @patch("app.services.ibkr_live_bars.get_live_bars_subprocess_args")
    def test_portfolio_specific_overrides_env(self, mock_env):
        mock_env.return_value = {
            "host": "127.0.0.1",
            "port": 7497,
            "client_id": 9436,
            "connect_timeout_sec": 10,
        }
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        with patch("app.db.get_connection", return_value=mock_conn), patch(
            "app.db.fetch_all",
            return_value=[{"IB_GATEWAY_HOST": "127.0.0.1", "IB_GATEWAY_PORT": 7496}],
        ):
            conn = ibkr_live_bars.resolve_live_bars_connect(2)

        self.assertEqual(conn["port"], 7496)


if __name__ == "__main__":
    unittest.main()

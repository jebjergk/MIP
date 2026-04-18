"""Tests for optional live politician disclosure enrichment (silent failure)."""
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.committee.live_politician_disclosure import (  # noqa: E402
    build_exhibit_live_politician_disclosure_context,
)


class TestLivePoliticianDisclosure(unittest.TestCase):
    @patch("app.committee.live_politician_disclosure.disclosure_context_flag_enabled", return_value=True)
    @patch("app.committee.live_politician_disclosure._live_flag_enabled", return_value=True)
    @patch("app.committee.live_politician_disclosure._http_get_json")
    @patch("app.committee.live_politician_disclosure.get_connection")
    def test_builds_exhibit_when_json_ok(self, mock_gc, mock_http, _lf, _dc):
        mock_gc.return_value = MagicMock()
        mock_http.return_value = {
            "summary_lines": ["Recent filing noted.", "Second line."],
            "source_label": "Test source",
            "link_url": "https://example.com/ptr",
        }
        with patch.dict(
            os.environ,
            {
                "MIP_POLITICIAN_DISCLOSURE_LIVE_DEMO": "",
                "MIP_POLITICIAN_DISCLOSURE_LIVE_URL_TEMPLATE": "http://x/{symbol}",
            },
            clear=False,
        ):
            out = build_exhibit_live_politician_disclosure_context({"SYMBOL": "ABC", "DIRECTION": "LONG"})
        self.assertIsNotNone(out)
        self.assertEqual(out["symbol"], "ABC")
        self.assertEqual(out["source_label"], "Test source")
        self.assertEqual(len(out["summary_lines"]), 2)
        self.assertEqual(out["link_url"], "https://example.com/ptr")

    def test_no_template_returns_none(self):
        with patch("app.committee.live_politician_disclosure.disclosure_context_flag_enabled", return_value=True):
            with patch("app.committee.live_politician_disclosure._live_flag_enabled", return_value=True):
                with patch("app.committee.live_politician_disclosure.get_connection") as mock_gc:
                    mock_gc.return_value = MagicMock()
                    with patch.dict(
                        os.environ,
                        {
                            "MIP_POLITICIAN_DISCLOSURE_LIVE_DEMO": "",
                            "MIP_POLITICIAN_DISCLOSURE_LIVE_URL_TEMPLATE": "",
                        },
                        clear=False,
                    ):
                        out = build_exhibit_live_politician_disclosure_context({"SYMBOL": "ABC"})
        self.assertIsNone(out)

    @patch("app.committee.live_politician_disclosure.disclosure_context_flag_enabled", return_value=True)
    @patch("app.committee.live_politician_disclosure._live_flag_enabled", return_value=True)
    @patch("app.committee.live_politician_disclosure.get_connection")
    def test_demo_env_returns_stub(self, mock_gc, _lf, _dc):
        mock_gc.return_value = MagicMock()
        with patch.dict(os.environ, {"MIP_POLITICIAN_DISCLOSURE_LIVE_DEMO": "true"}):
            out = build_exhibit_live_politician_disclosure_context({"SYMBOL": "XYZ", "DIRECTION": "LONG"})
        self.assertIsNotNone(out)
        self.assertEqual(out["symbol"], "XYZ")
        self.assertIn("demo", out["summary_lines"][0].lower())
        self.assertEqual(out["source_label"], "Local demo (no HTTP)")

    @patch("app.committee.live_politician_disclosure.disclosure_context_flag_enabled", return_value=True)
    @patch("app.committee.live_politician_disclosure._live_flag_enabled", return_value=True)
    @patch("app.committee.live_politician_disclosure._http_get_json", return_value=None)
    @patch("app.committee.live_politician_disclosure.get_connection")
    def test_http_failure_returns_none(self, mock_gc, _http, _lf, _dc):
        mock_gc.return_value = MagicMock()
        with patch.dict(
            os.environ,
            {
                "MIP_POLITICIAN_DISCLOSURE_LIVE_DEMO": "",
                "MIP_POLITICIAN_DISCLOSURE_LIVE_URL_TEMPLATE": "http://x/{symbol}",
            },
            clear=False,
        ):
            out = build_exhibit_live_politician_disclosure_context({"SYMBOL": "ZZZ"})
        self.assertIsNone(out)

    @patch("app.committee.live_politician_disclosure.disclosure_context_flag_enabled", return_value=False)
    @patch("app.committee.live_politician_disclosure.get_connection")
    def test_main_flag_off_returns_none(self, mock_gc, _dc):
        mock_gc.return_value = MagicMock()
        with patch.dict(
            os.environ,
            {
                "MIP_POLITICIAN_DISCLOSURE_LIVE_DEMO": "",
                "MIP_POLITICIAN_DISCLOSURE_LIVE_URL_TEMPLATE": "http://x/{symbol}",
            },
            clear=False,
        ):
            out = build_exhibit_live_politician_disclosure_context({"SYMBOL": "ABC"})
        self.assertIsNone(out)
        mock_gc.assert_called_once()


if __name__ == "__main__":
    unittest.main()

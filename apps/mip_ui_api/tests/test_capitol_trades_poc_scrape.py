"""Unit tests for Capitol Trades POC HTML scrape (no live HTTP in CI)."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.committee.capitol_trades_poc_scrape import (  # noqa: E402
    scrape_capitol_trades_for_symbol,
)

# Minimal page: trade array (site often uses backslash-escaped quotes; plain JSON also parses after unicode_escape).
_FIXTURE = """<!DOCTYPE html><html><body>
<p>noise</p>
[{"_txId":9001,"txType":"buy","txDate":"2026-04-01","issuerTicker":"ZZZ:US",
"politician":{"firstName":"Pat","lastName":"Public","party":"democrat"}}]
</body></html>"""


class TestCapitolTradesPocScrape(unittest.TestCase):
    @patch("app.committee.capitol_trades_poc_scrape._http_get", return_value=_FIXTURE)
    def test_parses_matching_ticker(self, _m):
        rows, url, matched = scrape_capitol_trades_for_symbol("ZZZ", timeout_s=1.0)
        self.assertTrue(matched)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("txType"), "buy")
        self.assertIn("ticker=zzz", url.lower())

    @patch("app.committee.capitol_trades_poc_scrape._http_get", return_value=None)
    def test_http_fail(self, _m):
        rows, url, matched = scrape_capitol_trades_for_symbol("ZZZ")
        self.assertIsNone(rows)
        self.assertFalse(matched)


if __name__ == "__main__":
    unittest.main()

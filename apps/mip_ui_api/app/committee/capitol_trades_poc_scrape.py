"""
POC: extract recent trade rows from Capitol Trades HTML (symbol-filtered URL).

Fetches https://www.capitoltrades.com/trades?ticker={symbol} and parses embedded
escaped JSON trade arrays from the document. For informational display only; fragile
if the site changes shape. Not a substitute for licensed data or ETL.
"""
from __future__ import annotations

import codecs
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

CAPITOL_TRADES_TRADES_URL = "https://www.capitoltrades.com/trades?ticker={symbol}"

_UA = "Mozilla/5.0 (compatible; MIP-Committee2-POC/1.0; +https://example.invalid)"


def _sym_base(sym: str) -> str:
    return sym.upper().strip().replace("-", "/")


def _tick_base(ticker: str) -> str:
    if not ticker:
        return ""
    return ticker.split(":")[0].upper().replace("-", "/")


def _trade_matches_symbol(trade: Dict[str, Any], symbol: str) -> bool:
    want = _sym_base(symbol)
    iss = trade.get("issuer") or {}
    tick = trade.get("issuerTicker") or iss.get("issuerTicker") or ""
    return _tick_base(tick) == want


def _http_get(url: str, timeout_s: float = 10.0) -> Optional[str]:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": _UA,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return None


def _candidate_array_starts(html: str) -> List[int]:
    """Positions of '[' that precede embedded trade objects."""
    starts: List[int] = []
    seen = set()
    for m in re.finditer(r'"_txId"\s*:', html):
        pos = m.start()
        start = html.rfind("[", 0, pos)
        if start < 0 or start in seen:
            continue
        seen.add(start)
        starts.append(start)
    return starts


def _try_decode_trade_array(html: str, start: int) -> Optional[List[Dict[str, Any]]]:
    chunk = html[start:]
    try:
        decoded = codecs.decode(chunk, "unicode_escape")
    except (UnicodeDecodeError, UnicodeError):
        return None
    try:
        arr, _end = json.JSONDecoder().raw_decode(decoded)
    except json.JSONDecodeError:
        return None
    if not isinstance(arr, list) or not arr:
        return None
    if not isinstance(arr[0], dict) or "_txId" not in arr[0]:
        return None
    return arr


def _pick_best_trades_array(html: str, symbol: str) -> Optional[List[Dict[str, Any]]]:
    best: Optional[List[Dict[str, Any]]] = None
    best_score = (-1, -1)  # (match_count, len)

    for start in _candidate_array_starts(html):
        arr = _try_decode_trade_array(html, start)
        if not arr:
            continue
        matches = [t for t in arr if _trade_matches_symbol(t, symbol)]
        score = (len(matches), len(arr))
        if score > best_score:
            best_score = score
            best = matches if matches else arr

    return best


def scrape_capitol_trades_for_symbol(
    symbol: str, timeout_s: float = 10.0
) -> Tuple[Optional[List[Dict[str, Any]]], str, bool]:
    """
    Returns (trades_or_none, page_url, issuer_matched).

    When issuer_matched is False, rows are a site SSR batch that may not all be for
    ``symbol`` (Capitol Trades applies some filters client-side).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return None, "", False
    url = CAPITOL_TRADES_TRADES_URL.replace("{symbol}", urllib.parse.quote(sym, safe=""))
    html = _http_get(url, timeout_s=timeout_s)
    if not html:
        return None, url, False
    arr = _pick_best_trades_array(html, sym)
    if not arr:
        return None, url, False
    matched = [t for t in arr if _trade_matches_symbol(t, sym)]
    if matched:
        return matched[:20], url, True
    return arr[:20], url, False

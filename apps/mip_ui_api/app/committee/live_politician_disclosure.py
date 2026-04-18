"""
Optional silent live enrichment for politician trade disclosures (Phase 2).

Not a hard dependency: any failure or empty response yields None (no exhibit, no user-visible error).
Does not scrape third-party sites by default — wire MIP_POLITICIAN_DISCLOSURE_LIVE_URL_TEMPLATE to a
trusted JSON endpoint that returns { summary_lines, link_url?, source_label? }.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.committee.public_disclosure_context import DISCLAIMER, disclosure_context_flag_enabled
from app.db import fetch_all, get_connection

CONFIG_LIVE = "COMMITTEE2_LIVE_POLITICIAN_DISCLOSURE_ENABLED"


def _env_truthy(name: str) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    return v in ("1", "true", "yes")


def _live_flag_enabled(cur) -> bool:
    cur.execute(
        "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
        (CONFIG_LIVE,),
    )
    rows = fetch_all(cur)
    if not rows:
        return False
    val = (rows[0].get("CONFIG_VALUE") or "").strip().lower()
    return val in ("1", "true", "yes")


def _http_get_json(url: str, timeout_s: float = 2.5) -> Optional[Dict[str, Any]]:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "MIP-Committee2-Phase2/1.0",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        data = json.loads(body)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None


def build_exhibit_live_politician_disclosure_context(proposal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Returns None when disabled, misconfigured, or on any fetch/parse failure (silent).
    Only runs when COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED is true (same Phase 2 gate as stored exhibit).
    """
    symbol = (proposal.get("SYMBOL") or "").strip().upper()
    if not symbol:
        return None
    template = (os.environ.get("MIP_POLITICIAN_DISCLOSURE_LIVE_URL_TEMPLATE") or "").strip()
    if not template or "{symbol}" not in template:
        return None

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not disclosure_context_flag_enabled(cur):
            return None
        if not _live_flag_enabled(cur):
            return None
    finally:
        conn.close()

    url = template.replace("{symbol}", urllib.parse.quote(symbol, safe=""))
    data = _http_get_json(url)
    if not data:
        return None

    lines = data.get("summary_lines")
    if not isinstance(lines, list):
        return None
    clean: List[str] = []
    for x in lines:
        if isinstance(x, str) and x.strip():
            clean.append(x.strip())
        if len(clean) >= 3:
            break
    if not clean:
        return None

    link = data.get("link_url")
    link_out: Optional[str] = None
    if isinstance(link, str):
        u = link.strip()
        if u.startswith("http://") or u.startswith("https://"):
            link_out = u

    src = data.get("source_label")
    src_l = str(src).strip()[:120] if isinstance(src, str) and str(src).strip() else "Live disclosure lookup"

    return {
        "schema_version": "1",
        "symbol": symbol,
        "fetched_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
     
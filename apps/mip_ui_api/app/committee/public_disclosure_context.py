"""
Phase 2 — deterministic politician trade disclosure context for Committee 2.0 hearings.

Snowflake-curated STOCK Act–style / Capitol Trades–class rows; read-only exhibit;
never fed into stance, chair, or engine inputs.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from app.db import fetch_all

DISCLAIMER = "Politician trade disclosures only; not a trade signal."
SCHEMA_VERSION = "1"
CONFIG_KEY = "COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED"


def disclosure_context_flag_enabled(cur) -> bool:
    cur.execute(
        "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
        (CONFIG_KEY,),
    )
    rows = fetch_all(cur)
    if not rows:
        return False
    val = (rows[0].get("CONFIG_VALUE") or "").strip().lower()
    return val in ("1", "true", "yes")


def _norm_side(transaction_type: Optional[str]) -> str:
    u = (transaction_type or "").strip().upper()
    if u in ("BUY", "PURCHASE", "PURCHASED", "ACQUIRE", "ACQUIRED"):
        return "BUY"
    if u in ("SELL", "SALE", "SOLD", "DISPOSE", "DISPOSED", "EXCHANGE_SELL"):
        return "SELL"
    return "UNKNOWN"


def compute_tone_vs_trade(proposal_direction: str, transaction_types: List[Optional[str]]) -> str:
    """
    Conservative deterministic rule: SUPPORTIVE / CONTRADICTORY only when
    the buy/sell mix is strict; otherwise NEUTRAL or UNKNOWN.
    """
    d = (proposal_direction or "").strip().upper()
    if d not in ("LONG", "SHORT"):
        return "UNKNOWN"
    sides = [_norm_side(t) for t in transaction_types]
    usable = [s for s in sides if s != "UNKNOWN"]
    if not usable:
        return "UNKNOWN"
    buys = sum(1 for s in usable if s == "BUY")
    sells = sum(1 for s in usable if s == "SELL")
    if buys == sells:
        return "NEUTRAL"
    if d == "LONG":
        if buys > sells:
            return "SUPPORTIVE"
        return "CONTRADICTORY"
    # SHORT
    if sells > buys:
        return "SUPPORTIVE"
    return "CONTRADICTORY"


def _iso_utc(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(dt, date):
        return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    s = str(dt).strip()
    return s if s else None


def _date_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, datetime):
        return v.date().isoformat()
    s = str(v).strip()
    return s[:10] if s else None


def _fetch_rows(cur, symbol_upper: str, limit: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT DISCLOSURE_ID, SOURCE_URL, SOURCE_NAME, DISCLOSURE_DATE, TRADE_DATE, PERSON_NAME, CHAMBER,
               TRANSACTION_TYPE, ASSET_SYMBOL, ASSET_CLASS, NOTIONAL_RANGE, INGESTED_AT
          FROM MIP.APP.PUBLIC_DISCLOSURE_TRANSACTION
         WHERE UPPER(TRIM(ASSET_SYMBOL)) = %s
         ORDER BY DISCLOSURE_DATE DESC, INGESTED_AT DESC
         LIMIT %s
        """,
        (symbol_upper, limit),
    )
    return fetch_all(cur)


def build_exhibit_public_disclosure_context(cur, proposal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Returns None when COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED is false (omit from JSON).
    When enabled, always returns a versioned dict (including empty / unmapped states).
    """
    if not disclosure_context_flag_enabled(cur):
        return None

    symbol = (proposal.get("SYMBOL") or "").strip()
    direction = (proposal.get("DIRECTION") or "").strip()
    market_type = "STOCK"
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not symbol:
        return {
            "schema_version": SCHEMA_VERSION,
            "symbol": "",
            "market_type": market_type,
            "as_of_utc": now_utc,
            "mapping_quality": "UNMAPPED",
            "tone_vs_trade": "UNKNOWN",
            "summary_lines": ["No proposal symbol — politician trade disclosure rows are not joined."],
            "recent_transactions": [],
            "disclaimer": DISCLAIMER,
        }

    sym_u = symbol.upper()
    rows = _fetch_rows(cur, sym_u, 20)
    if not rows:
        return {
            "schema_version": SCHEMA_VERSION,
            "symbol": sym_u,
            "market_type": market_type,
            "as_of_utc": now_utc,
            "mapping_quality": "UNMAPPED",
            "tone_vs_trade": "UNKNOWN",
            "summary_lines": [f"No mapped politician trade disclosure rows for symbol {sym_u}."],
            "recent_transactions": [],
            "disclaimer": DISCLAIMER,
        }

    ingested = [r.get("INGESTED_AT") for r in rows if r.get("INGESTED_AT")]
    as_of = max(ingested) if ingested else None
    tone = compute_tone_vs_trade(direction, [r.get("TRANSACTION_TYPE") for r in rows])

    dates = [r.get("DISCLOSURE_DATE") for r in rows if r.get("DISCLOSURE_DATE")]
    max_d = max(dates) if dates else None
    sides = [_norm_side(r.get("TRANSACTION_TYPE")) for r in rows]
    usable = [s for s in sides if s != "UNKNOWN"]
    buys = sum(1 for s in usable if s == "BUY")
    sells = sum(1 for s in usable if s == "SELL")

    summary_lines: List[str] = [f"{len(rows)} mapped politician trade disclosure row(s) for {sym_u}."]
    if max_d is not None:
        summary_lines.append(f"Latest disclosure date: {_date_str(max_d)}.")
    if buys or sells:
        summary_lines.append(f"In this window: {buys} purchase(s), {sells} sale(s) (known sides only).")
    summary_lines = summary_lines[:3]

    recent: List[Dict[str, Any]] = []
    for r in rows[:5]:
        side = _norm_side(r.get("TRANSACTION_TYPE"))
        filed = _date_str(r.get("DISCLOSURE_DATE"))
        traded = _date_str(r.get("TRADE_DATE"))
        recent.append(
            {
                "filed_date": filed,
                "transaction_date": traded or filed,
                "side": side,
                "filer_display_name": (r.get("PERSON_NAME") or "") or "—",
                "source_url": r.get("SOURCE_URL"),
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "symbol": sym_u,
        "market_type": market_type,
        "as_of_utc": _iso_utc(as_of) or now_utc,
        "mapping_quality": "MAPPED",
        "tone_vs_trade": tone,
        "summary_lines": summary_lines,
        "recent_transactions": recent,
        "disclaimer": DISCLAIMER,
    }

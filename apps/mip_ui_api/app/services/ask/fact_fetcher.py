"""
Read-only Snowflake facts for Ask MIP — narrow queries, parameterized IDs only.

Authz (MVP): UI API uses the existing read-only Snowflake role. We only run SELECTs
with bound parameters; no user-supplied SQL. Callers must pass portfolio_id/symbol
from trusted session context (browser sends what the UI already shows).
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from app.db import get_connection

logger = logging.getLogger(__name__)

_SAFE_SYMBOL = re.compile(r"^[A-Za-z0-9.\-]{1,32}$")
_SAFE_MARKET = re.compile(r"^[A-Z0-9_]{2,16}$")


def _sanitize_symbol(symbol: str | None) -> str | None:
    if not symbol:
        return None
    s = str(symbol).strip().upper()
    if not _SAFE_SYMBOL.match(s):
        return None
    return s


def _sanitize_market_type(market_type: str | None) -> str | None:
    if not market_type:
        return None
    s = str(market_type).strip().upper()
    if not _SAFE_MARKET.match(s):
        return None
    return s


def fetch_training_digest_facts(symbol: str | None, market_type: str | None) -> dict[str, Any] | None:
    """Latest SYMBOL_TRAINING digest row (metadata only; JSON may be large — trim)."""
    sym = _sanitize_symbol(symbol)
    mt = _sanitize_market_type(market_type)
    if not sym or not mt:
        return None
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              s.SYMBOL,
              s.MARKET_TYPE,
              s.PATTERN_ID,
              s.AS_OF_TS,
              s.RUN_ID,
              s.CREATED_AT
            FROM MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT s
            WHERE s.SCOPE = 'SYMBOL_TRAINING'
              AND s.SYMBOL = %s
              AND s.MARKET_TYPE = %s
            ORDER BY s.CREATED_AT DESC
            LIMIT 1
            """,
            (sym, mt),
        )
        row = cur.fetchone()
        if not row or not cur.description:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    except Exception:
        logger.debug("fetch_training_digest_facts failed (non-fatal)", exc_info=True)
        return None
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()


def fetch_live_portfolio_facts(portfolio_id: int | None) -> dict[str, Any] | None:
    if portfolio_id is None or portfolio_id < 1 or portfolio_id > 10_000_000:
        return None
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              PORTFOLIO_ID,
              SIM_PORTFOLIO_ID,
              ADAPTER_MODE,
              BASE_CURRENCY,
              MAX_POSITIONS,
              MAX_POSITION_PCT,
              CASH_BUFFER_PCT,
              MAX_SLIPPAGE_PCT,
              IS_ACTIVE,
              DRIFT_STATUS,
              UPDATED_AT
            FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            WHERE PORTFOLIO_ID = %s
            LIMIT 1
            """,
            (portfolio_id,),
        )
        row = cur.fetchone()
        if not row or not cur.description:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    except Exception:
        logger.debug("fetch_live_portfolio_facts failed (non-fatal)", exc_info=True)
        return None
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()


def fetch_ask_facts(
    portfolio_id: int | None,
    symbol: str | None,
    session_mode: str | None = None,
    market_type: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """
    Returns (facts_dict, lookup_attempted).
    facts_dict may be empty; never raises for Snowflake errors.
    """
    sym = _sanitize_symbol(symbol)
    mt = _sanitize_market_type(market_type)
    payload: dict[str, Any] = {
        "session_mode": session_mode,
        "portfolio": None,
        "symbol": sym,
        "market_type": mt,
        "note": "Snowflake snapshot for current context; values are factual rows, not advice.",
    }
    attempted = False
    pf = fetch_live_portfolio_facts(portfolio_id)
    if portfolio_id is not None:
        attempted = True
    if pf:
        payload["portfolio"] = pf
    if sym and mt:
        td = fetch_training_digest_facts(sym, mt)
        attempted = True
        if td:
            payload["training_digest_latest"] = td
    if sym and portfolio_id:
        payload["symbol_context"] = {"symbol": sym, "portfolio_id": portfolio_id}
    return payload, attempted


def facts_block_for_prompt(facts: dict[str, Any]) -> str:
    return (
        "<snowflake_runtime_facts>\n"
        "Use only these JSON values as quantitative/config truth for the user's session; "
        "do not invent other live numbers.\n"
        f"{json.dumps(facts, default=str, indent=2)}\n"
        "</snowflake_runtime_facts>"
    )

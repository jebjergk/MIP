"""
Training Digest endpoints: GET /training/digest/latest, GET /training/digest,
GET /training/digest/symbol/latest, GET /training/digest/symbol
Training Journey Digest — AI narrative grounded in deterministic training snapshots.
Read-only; never writes to Snowflake.
"""
import json

from fastapi import APIRouter, Query, HTTPException

from app.db import get_connection, serialize_row


router = APIRouter(prefix="/training/digest", tags=["training-digest"])


def _parse_json(value):
    """Parse JSON value — handles string, dict, or None."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def _build_links(symbol=None, market_type=None) -> dict:
    """Build drill-down links for training digest."""
    base = {
        "training": "/training",
        "signals": "/signals",
        "digest": "/digest",
        "market_timeline": "/market-timeline",
    }
    if symbol and market_type:
        base["symbol_training"] = f"/training?symbol={symbol}&market_type={market_type}"
    return base


def _format_response(data, scope="GLOBAL_TRAINING"):
    """Build standard training digest response from a row dict."""
    snapshot_json = _parse_json(data.get("snapshot_json"))
    narrative_json = _parse_json(data.get("narrative_json"))
    narrative_text = data.get("narrative_text") or ""
    model_info = data.get("model_info")
    is_ai_narrative = model_info is not None and model_info != "DETERMINISTIC_FALLBACK"

    row_scope = data.get("scope") or scope
    symbol = data.get("symbol")
    market_type = data.get("market_type")
    pattern_id = data.get("pattern_id")

    return {
        "found": True,
        "scope": row_scope,
        "symbol": symbol,
        "market_type": market_type,
        "pattern_id": pattern_id,
        "as_of_ts": data.get("as_of_ts"),
        "run_id": data.get("run_id"),
        "snapshot_created_at": data.get("snapshot_created_at"),
        "narrative_created_at": data.get("narrative_created_at"),
        "source_facts_hash": data.get("source_facts_hash"),
        # Narrative
        "narrative": narrative_json,
        "narrative_text": narrative_text,
        "model_info": model_info,
        "agent_name": data.get("agent_name"),
        "is_ai_narrative": is_ai_narrative,
        # Snapshot (full for training)
        "snapshot": snapshot_json,
        # Links
        "links": _build_links(symbol, market_type),
    }


# ── Global Training Digest ──────────────────────────────────

@router.get("/latest")
def get_training_digest_latest():
    """
    Latest global training digest.
    Returns narrative JSON + text, snapshot JSON, and drill-down links.
    """
    sql = """
    select
        s.SCOPE,
        s.SYMBOL,
        s.MARKET_TYPE,
        s.PATTERN_ID,
        s.AS_OF_TS,
        s.RUN_ID,
        s.SNAPSHOT_JSON,
        s.SOURCE_FACTS_HASH,
        s.CREATED_AT          as SNAPSHOT_CREATED_AT,
        n.NARRATIVE_TEXT,
        n.NARRATIVE_JSON,
        n.MODEL_INFO,
        n.AGENT_NAME,
        n.CREATED_AT          as NARRATIVE_CREATED_AT
    from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT s
    left join MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE n
        on  n.SCOPE         = s.SCOPE
        and n.SYMBOL is null and s.SYMBOL is null
        and n.MARKET_TYPE is null and s.MARKET_TYPE is null
        and n.PATTERN_ID is null and s.PATTERN_ID is null
        and n.AS_OF_TS      = s.AS_OF_TS
        and n.RUN_ID        = s.RUN_ID
    where s.SCOPE = 'GLOBAL_TRAINING'
      and s.SYMBOL is null
    order by s.CREATED_AT desc
    limit 1
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
            return {
                "found": False,
                "message": "No training digest exists yet. Run the daily pipeline to generate one.",
            }
        columns = [d[0].lower() for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))
        return _format_response(data, "GLOBAL_TRAINING")
    finally:
        conn.close()


@router.get("")
def get_training_digest(as_of_ts: str = Query(None)):
    """
    Historical global training digest lookup. Filters by as_of_ts.
    """
    conditions = ["s.SCOPE = 'GLOBAL_TRAINING'", "s.SYMBOL is null"]
    params = []

    if as_of_ts is not None:
        conditions.append("s.AS_OF_TS::date = %s::date")
        params.append(as_of_ts)

    where_clause = " and ".join(conditions)

    sql = f"""
    select
        s.SCOPE,
        s.SYMBOL,
        s.MARKET_TYPE,
        s.PATTERN_ID,
        s.AS_OF_TS,
        s.RUN_ID,
        s.SNAPSHOT_JSON,
        s.SOURCE_FACTS_HASH,
        s.CREATED_AT          as SNAPSHOT_CREATED_AT,
        n.NARRATIVE_TEXT,
        n.NARRATIVE_JSON,
        n.MODEL_INFO,
        n.AGENT_NAME,
        n.CREATED_AT          as NARRATIVE_CREATED_AT
    from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT s
    left join MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE n
        on  n.SCOPE         = s.SCOPE
        and n.SYMBOL is null and s.SYMBOL is null
        and n.MARKET_TYPE is null and s.MARKET_TYPE is null
        and n.PATTERN_ID is null and s.PATTERN_ID is null
        and n.AS_OF_TS      = s.AS_OF_TS
        and n.RUN_ID        = s.RUN_ID
    where {where_clause}
    order by s.CREATED_AT desc
    limit 1
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No training digest found for the given filters.")
        columns = [d[0].lower() for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))
        return _format_response(data, "GLOBAL_TRAINING")
    finally:
        conn.close()


# ── Per-Symbol Training Digest ──────────────────────────────

@router.get("/symbol/latest")
def get_symbol_training_digest_latest(
    symbol: str = Query(..., description="Symbol to query"),
    market_type: str = Query(..., description="Market type (STOCK, ETF, FX)"),
    pattern_id: int = Query(None, description="Pattern ID (required when symbol has multiple patterns)"),
):
    """
    Latest per-symbol training digest. When pattern_id is provided,
    returns the digest for that specific pattern; otherwise the latest one.
    """
    conditions = [
        "s.SCOPE = 'SYMBOL_TRAINING'",
        "s.SYMBOL = %s",
        "s.MARKET_TYPE = %s",
    ]
    params = [symbol, market_type]

    if pattern_id is not None:
        conditions.append("s.PATTERN_ID = %s")
        params.append(pattern_id)

    where_clause = " and ".join(conditions)

    sql = f"""
    select
        s.SCOPE,
        s.SYMBOL,
        s.MARKET_TYPE,
        s.PATTERN_ID,
        s.AS_OF_TS,
        s.RUN_ID,
        s.SNAPSHOT_JSON,
        s.SOURCE_FACTS_HASH,
        s.CREATED_AT          as SNAPSHOT_CREATED_AT,
        n.NARRATIVE_TEXT,
        n.NARRATIVE_JSON,
        n.MODEL_INFO,
        n.AGENT_NAME,
        n.CREATED_AT          as NARRATIVE_CREATED_AT
    from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT s
    left join MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE n
        on  n.SCOPE         = s.SCOPE
        and n.SYMBOL        = s.SYMBOL
        and n.MARKET_TYPE   = s.MARKET_TYPE
        and coalesce(n.PATTERN_ID, -1) = coalesce(s.PATTERN_ID, -1)
        and n.AS_OF_TS      = s.AS_OF_TS
        and n.RUN_ID        = s.RUN_ID
    where {where_clause}
    order by s.CREATED_AT desc
    limit 1
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        if not row:
            return {
                "found": False,
                "message": f"No training digest for {symbol} ({market_type}) yet.",
            }
        columns = [d[0].lower() for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))
        return _format_response(data, "SYMBOL_TRAINING")
    finally:
        conn.close()


@router.get("/symbol")
def get_symbol_training_digest(
    symbol: str = Query(...),
    market_type: str = Query(...),
    pattern_id: int = Query(None),
    as_of_ts: str = Query(None),
):
    """
    Historical per-symbol training digest lookup.
    """
    conditions = [
        "s.SCOPE = 'SYMBOL_TRAINING'",
        "s.SYMBOL = %s",
        "s.MARKET_TYPE = %s",
    ]
    params = [symbol, market_type]

    if pattern_id is not None:
        conditions.append("s.PATTERN_ID = %s")
        params.append(pattern_id)

    if as_of_ts is not None:
        conditions.append("s.AS_OF_TS::date = %s::date")
        params.append(as_of_ts)

    where_clause = " and ".join(conditions)

    sql = f"""
    select
        s.SCOPE,
        s.SYMBOL,
        s.MARKET_TYPE,
        s.PATTERN_ID,
        s.AS_OF_TS,
        s.RUN_ID,
        s.SNAPSHOT_JSON,
        s.SOURCE_FACTS_HASH,
        s.CREATED_AT          as SNAPSHOT_CREATED_AT,
        n.NARRATIVE_TEXT,
        n.NARRATIVE_JSON,
        n.MODEL_INFO,
        n.AGENT_NAME,
        n.CREATED_AT          as NARRATIVE_CREATED_AT
    from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT s
    left join MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE n
        on  n.SCOPE         = s.SCOPE
        and n.SYMBOL        = s.SYMBOL
        and n.MARKET_TYPE   = s.MARKET_TYPE
        and coalesce(n.PATTERN_ID, -1) = coalesce(s.PATTERN_ID, -1)
        and n.AS_OF_TS      = s.AS_OF_TS
        and n.RUN_ID        = s.RUN_ID
    where {where_clause}
    order by s.CREATED_AT desc
    limit 1
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No training digest found.")
        columns = [d[0].lower() for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))
        return _format_response(data, "SYMBOL_TRAINING")
    finally:
        conn.close()

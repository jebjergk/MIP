"""
Digest endpoints: GET /digest/latest, GET /digest
Daily Intelligence Digest — narrative + deterministic snapshot.
Supports both portfolio-scoped and global-scoped digests.
Read-only; never writes to Snowflake.
"""
import json

from fastapi import APIRouter, Query, HTTPException

from app.db import get_connection, serialize_row


router = APIRouter(prefix="/digest", tags=["digest"])


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


def _build_links(portfolio_id=None, scope="PORTFOLIO") -> dict:
    """Build drill-down links for digest page."""
    base = {
        "signals": "/signals",
        "training": "/training",
        "market_timeline": "/market-timeline",
        "suggestions": "/suggestions",
        "runs": "/runs",
    }
    if scope == "GLOBAL":
        base["digest"] = "/digest"
    if portfolio_id:
        base["portfolio"] = f"/portfolios/{portfolio_id}"
    return base


def _format_response(data, scope="PORTFOLIO"):
    """Build standard digest response from a row dict."""
    snapshot_json = _parse_json(data.get("snapshot_json"))
    narrative_json = _parse_json(data.get("narrative_json"))
    narrative_text = data.get("narrative_text") or ""
    model_info = data.get("model_info")
    is_ai_narrative = model_info is not None and model_info != "DETERMINISTIC_FALLBACK"

    pid = data.get("portfolio_id")
    row_scope = data.get("scope") or scope

    # Build snapshot subset based on scope
    if row_scope == "GLOBAL":
        snapshot_subset = {
            "system": snapshot_json.get("system", {}),
            "gates": snapshot_json.get("gates", {}),
            "capacity": snapshot_json.get("capacity", {}),
            "signals": snapshot_json.get("signals", {}),
            "proposals": snapshot_json.get("proposals", {}),
            "trades": snapshot_json.get("trades", {}),
            "training": snapshot_json.get("training", {}),
            "pipeline": snapshot_json.get("pipeline", {}),
            "detectors": snapshot_json.get("detectors", []),
        }
    else:
        snapshot_subset = {
            "gate": snapshot_json.get("gate", {}),
            "capacity": snapshot_json.get("capacity", {}),
            "signals": snapshot_json.get("signals", {}),
            "proposals": snapshot_json.get("proposals", {}),
            "trades": snapshot_json.get("trades", {}),
            "training": snapshot_json.get("training", {}),
            "kpis": snapshot_json.get("kpis", {}),
            "exposure": snapshot_json.get("exposure", {}),
            "pipeline": snapshot_json.get("pipeline", {}),
            "detectors": snapshot_json.get("detectors", []),
        }

    return {
        "found": True,
        "scope": row_scope,
        "portfolio_id": pid,
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
        # Snapshot (key fields for "show facts" toggle)
        "snapshot": snapshot_subset,
        # Links
        "links": _build_links(pid, row_scope),
    }


@router.get("/latest")
def get_latest_digest(
    portfolio_id: int = Query(None),
    scope: str = Query(None, description="PORTFOLIO or GLOBAL"),
):
    """
    Latest daily digest. If scope=GLOBAL, returns system-wide digest.
    If portfolio_id is specified, returns that portfolio's digest.
    If neither, returns the most recent digest across all scopes.
    """
    # Determine effective scope
    if scope and scope.upper() == "GLOBAL":
        # Global scope: PORTFOLIO_ID is null
        sql = """
        select
            s.SCOPE,
            s.PORTFOLIO_ID,
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
        from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
        left join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
            on  n.SCOPE         = s.SCOPE
            and n.PORTFOLIO_ID is null
            and s.PORTFOLIO_ID is null
            and n.AS_OF_TS      = s.AS_OF_TS
            and n.RUN_ID        = s.RUN_ID
        where s.SCOPE = 'GLOBAL'
          and s.PORTFOLIO_ID is null
        order by s.CREATED_AT desc
        limit 1
        """
        params = ()
    elif portfolio_id is not None:
        # Portfolio scope
        sql = """
        select
            s.SCOPE,
            s.PORTFOLIO_ID,
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
        from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
        left join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
            on  n.SCOPE         = s.SCOPE
            and n.PORTFOLIO_ID  = s.PORTFOLIO_ID
            and n.AS_OF_TS      = s.AS_OF_TS
            and n.RUN_ID        = s.RUN_ID
        where s.SCOPE = 'PORTFOLIO'
          and s.PORTFOLIO_ID = %s
        order by s.CREATED_AT desc
        limit 1
        """
        params = (portfolio_id,)
    else:
        # Default: most recent across all scopes
        sql = """
        select
            s.SCOPE,
            s.PORTFOLIO_ID,
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
        from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
        left join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
            on  coalesce(n.SCOPE, 'PORTFOLIO')   = coalesce(s.SCOPE, 'PORTFOLIO')
            and coalesce(n.PORTFOLIO_ID, -1)      = coalesce(s.PORTFOLIO_ID, -1)
            and n.AS_OF_TS      = s.AS_OF_TS
            and n.RUN_ID        = s.RUN_ID
        order by s.CREATED_AT desc
        limit 1
        """
        params = ()

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return {
                "found": False,
                "message": "No digest exists yet. Run the daily pipeline to generate one.",
            }

        columns = [d[0].lower() for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))

        effective_scope = (data.get("scope") or "PORTFOLIO").upper()
        return _format_response(data, effective_scope)
    finally:
        conn.close()


@router.get("")
def get_digest(
    portfolio_id: int = Query(None),
    as_of_ts: str = Query(None),
    scope: str = Query(None, description="PORTFOLIO or GLOBAL"),
):
    """
    Historical digest lookup. Filters by scope, portfolio_id, and/or as_of_ts.
    """
    conditions = ["1=1"]
    params = []

    effective_scope = (scope or "PORTFOLIO").upper() if scope else None

    if effective_scope == "GLOBAL":
        conditions.append("s.SCOPE = 'GLOBAL'")
        conditions.append("s.PORTFOLIO_ID is null")
    elif portfolio_id is not None:
        conditions.append("s.SCOPE = 'PORTFOLIO'")
        conditions.append("s.PORTFOLIO_ID = %s")
        params.append(portfolio_id)
    elif effective_scope == "PORTFOLIO":
        conditions.append("s.SCOPE = 'PORTFOLIO'")

    if as_of_ts is not None:
        conditions.append("s.AS_OF_TS::date = %s::date")
        params.append(as_of_ts)

    where_clause = " and ".join(conditions)

    sql = f"""
    select
        s.SCOPE,
        s.PORTFOLIO_ID,
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
    from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
    left join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
        on  coalesce(n.SCOPE, 'PORTFOLIO')   = coalesce(s.SCOPE, 'PORTFOLIO')
        and coalesce(n.PORTFOLIO_ID, -1)      = coalesce(s.PORTFOLIO_ID, -1)
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
            raise HTTPException(
                status_code=404,
                detail="No digest found for the given filters.",
            )

        columns = [d[0].lower() for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))

        row_scope = (data.get("scope") or "PORTFOLIO").upper()
        return _format_response(data, row_scope)
    finally:
        conn.close()

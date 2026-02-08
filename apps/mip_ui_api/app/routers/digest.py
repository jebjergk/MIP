"""
Digest endpoints: GET /digest/latest, GET /digest
Daily Intelligence Digest — narrative + deterministic snapshot.
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


def _build_links(portfolio_id: int) -> dict:
    """Build drill-down links for digest page."""
    base = {
        "signals": "/signals",
        "training": "/training",
        "brief": "/brief",
        "market_timeline": "/market-timeline",
        "suggestions": "/suggestions",
        "runs": "/runs",
    }
    if portfolio_id:
        base["portfolio"] = f"/portfolios/{portfolio_id}"
    return base


@router.get("/latest")
def get_latest_digest(portfolio_id: int = Query(None)):
    """
    Latest daily digest for a portfolio (or the most recent across all portfolios).
    Returns narrative JSON + text, snapshot JSON (key fields), and drill-down links.
    """
    sql = """
    select
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
        on  n.PORTFOLIO_ID = s.PORTFOLIO_ID
        and n.AS_OF_TS     = s.AS_OF_TS
        and n.RUN_ID       = s.RUN_ID
    where (%s is null or s.PORTFOLIO_ID = %s)
    order by s.CREATED_AT desc
    limit 1
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (portfolio_id, portfolio_id))
        row = cur.fetchone()
        if not row:
            return {
                "found": False,
                "message": "No digest exists yet. Run the daily pipeline to generate one.",
            }

        columns = [d[0].lower() for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))

        snapshot_json = _parse_json(data.get("snapshot_json"))
        narrative_json = _parse_json(data.get("narrative_json"))
        narrative_text = data.get("narrative_text") or ""
        model_info = data.get("model_info")
        is_ai_narrative = model_info is not None and model_info != "DETERMINISTIC_FALLBACK"

        pid = data.get("portfolio_id")

        return {
            "found": True,
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
            "snapshot": {
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
            },
            # Links
            "links": _build_links(pid),
        }
    finally:
        conn.close()


@router.get("")
def get_digest(
    portfolio_id: int = Query(None),
    as_of_ts: str = Query(None),
):
    """
    Historical digest lookup. Filters by portfolio_id and/or as_of_ts.
    Returns the matching digest or 404.
    """
    conditions = ["1=1"]
    params = []

    if portfolio_id is not None:
        conditions.append("s.PORTFOLIO_ID = %s")
        params.append(portfolio_id)
    if as_of_ts is not None:
        conditions.append("s.AS_OF_TS::date = %s::date")
        params.append(as_of_ts)

    where_clause = " and ".join(conditions)

    sql = f"""
    select
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
        on  n.PORTFOLIO_ID = s.PORTFOLIO_ID
        and n.AS_OF_TS     = s.AS_OF_TS
        and n.RUN_ID       = s.RUN_ID
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

        snapshot_json = _parse_json(data.get("snapshot_json"))
        narrative_json = _parse_json(data.get("narrative_json"))
        narrative_text = data.get("narrative_text") or ""
        model_info = data.get("model_info")
        is_ai_narrative = model_info is not None and model_info != "DETERMINISTIC_FALLBACK"

        pid = data.get("portfolio_id")

        return {
            "found": True,
            "portfolio_id": pid,
            "as_of_ts": data.get("as_of_ts"),
            "run_id": data.get("run_id"),
            "snapshot_created_at": data.get("snapshot_created_at"),
            "narrative_created_at": data.get("narrative_created_at"),
            "source_facts_hash": data.get("source_facts_hash"),
            "narrative": narrative_json,
            "narrative_text": narrative_text,
            "model_info": model_info,
            "agent_name": data.get("agent_name"),
            "is_ai_narrative": is_ai_narrative,
            "snapshot": {
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
            },
            "links": _build_links(pid),
        }
    finally:
        conn.close()

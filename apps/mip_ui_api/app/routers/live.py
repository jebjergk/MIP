"""
GET /live/metrics — lightweight live metrics for header and Suggestions.
Read-only. Returns api_ok, snowflake_ok, updated_at, last_run, last_brief, outcomes.
"""
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Query

from app.config import get_snowflake_config
from app.db import get_connection, fetch_all, serialize_row, SnowflakeAuthError

router = APIRouter(prefix="/live", tags=["live"])


def _summary_hint_from_status_and_details(status: str | None, details: dict | None) -> str | None:
    """Derive a short summary hint for the run (same as runs router)."""
    if not status:
        return None
    s = (status or "").upper()
    if s == "SKIPPED_NO_NEW_BARS":
        return "No new bars"
    if s == "SKIP_RATE_LIMIT":
        return "Rate limit"
    if s == "SUCCESS_WITH_SKIPS":
        return "Success with skips"
    if s == "FAIL":
        return "Failed"
    if s == "SUCCESS":
        return None
    return None


@router.get("/metrics")
def get_live_metrics(portfolio_id: int = Query(1, description="Portfolio ID for latest brief")):
    """
    Single cheap request for UI to poll every 30–60s.
    Returns: api_ok, snowflake_ok, updated_at, last_run, last_brief, outcomes.
    last_brief uses found: false when no brief exists for portfolio_id.
    outcomes.since_last_run = count of outcomes with CALCULATED_AT > last_run.completed_at (or null/0 when no run).
    """
    updated_at = datetime.now(timezone.utc).isoformat()
    api_ok = True
    snowflake_ok = False
    last_run = None
    last_brief = {"found": False}
    outcomes = {"total": 0, "last_calculated_at": None, "since_last_run": None}

    try:
        conn = get_connection()
        snowflake_ok = True
    except SnowflakeAuthError:
        pass
    except Exception:
        pass

    if not snowflake_ok:
        return {
            "api_ok": api_ok,
            "snowflake_ok": snowflake_ok,
            "updated_at": updated_at,
            "last_run": last_run,
            "last_brief": last_brief,
            "outcomes": outcomes,
        }

    try:
        cur = conn.cursor()

        # --- Last run: same logic as /runs (MIP_AUDIT_LOG, PIPELINE, SP_RUN_DAILY_PIPELINE), most recent by completion
        runs_sql = """
        select EVENT_TS, RUN_ID, STATUS, DETAILS
        from MIP.APP.MIP_AUDIT_LOG
        where EVENT_TYPE = 'PIPELINE' and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
        order by EVENT_TS desc
        limit 200
        """
        cur.execute(runs_sql)
        run_rows = fetch_all(cur)
        runs_by_id = {}
        for r in run_rows:
            run_id = r.get("RUN_ID")
            if not run_id:
                continue
            ts = r.get("EVENT_TS")
            status = r.get("STATUS") or ""
            details = r.get("DETAILS")
            if isinstance(details, str):
                try:
                    details = json.loads(details) if details else {}
                except Exception:
                    details = {}
            if run_id not in runs_by_id:
                runs_by_id[run_id] = {"started_at": ts, "completed_at": ts, "status": status, "details": details}
            else:
                run = runs_by_id[run_id]
                if ts:
                    if run["started_at"] is None or (ts < run["started_at"]):
                        run["started_at"] = ts
                    if run["completed_at"] is None or (ts > run["completed_at"]):
                        run["completed_at"] = ts
                        run["status"] = status
                        run["details"] = details
        run_list = [
            {
                "run_id": rid,
                "started_at": r["started_at"].isoformat() if hasattr(r["started_at"], "isoformat") else r["started_at"],
                "completed_at": r["completed_at"].isoformat() if hasattr(r["completed_at"], "isoformat") else r["completed_at"],
                "status": r["status"] if r["status"] != "START" else "RUNNING",
                "summary_hint": _summary_hint_from_status_and_details(r["status"], r.get("details")),
            }
            for rid, r in runs_by_id.items()
        ]
        run_list.sort(key=lambda x: (x["completed_at"] or x["started_at"] or ""), reverse=True)
        if run_list:
            last_run = serialize_row(run_list[0])

        # --- Last brief: same as /briefs/latest
        brief_sql = """
        select
          mb.PORTFOLIO_ID as portfolio_id,
          coalesce(
            try_cast(mb.BRIEF:as_of_ts::varchar as timestamp_ntz),
            try_cast(get_path(mb.BRIEF, 'attribution.as_of_ts')::varchar as timestamp_ntz),
            mb.AS_OF_TS
          ) as as_of_ts,
          coalesce(
            get_path(mb.BRIEF, 'attribution.pipeline_run_id')::varchar,
            mb.PIPELINE_RUN_ID
          ) as pipeline_run_id,
          mb.AGENT_NAME as agent_name
        from MIP.AGENT_OUT.MORNING_BRIEF mb
        where mb.PORTFOLIO_ID = %s and coalesce(mb.AGENT_NAME, '') = 'MORNING_BRIEF'
        order by mb.AS_OF_TS desc
        limit 1
        """
        cur.execute(brief_sql, (portfolio_id,))
        brief_row = cur.fetchone()
        if brief_row:
            cols = [d[0] for d in cur.description]
            last_brief = serialize_row(dict(zip(cols, brief_row)))
            last_brief["found"] = True
        else:
            last_brief = {"found": False}

        # --- Outcomes: total, max(CALCULATED_AT), and since_last_run
        outcomes_sql = """
        select count(*) as total, max(CALCULATED_AT) as last_calculated_at
        from MIP.APP.RECOMMENDATION_OUTCOMES
        """
        cur.execute(outcomes_sql)
        out_row = cur.fetchone()
        if out_row:
            outcomes["total"] = int(out_row[0]) if out_row[0] is not None else 0
            lca = out_row[1]
            outcomes["last_calculated_at"] = lca.isoformat() if hasattr(lca, "isoformat") else (str(lca) if lca else None)

        last_run_completed_at = None
        if last_run and last_run.get("completed_at"):
            last_run_completed_at = last_run["completed_at"]

        if last_run_completed_at is not None:
            # Count outcomes where CALCULATED_AT > last_run.completed_at
            # last_run_completed_at is already ISO string from serialize_row
            since_sql = """
            select count(*) as cnt from MIP.APP.RECOMMENDATION_OUTCOMES
            where CALCULATED_AT > %s
            """
            cur.execute(since_sql, (last_run_completed_at,))
            since_row = cur.fetchone()
            outcomes["since_last_run"] = int(since_row[0]) if since_row and since_row[0] is not None else 0
        else:
            outcomes["since_last_run"] = 0

        conn.close()
    except Exception:
        snowflake_ok = False
        try:
            conn.close()
        except Exception:
            pass

    return {
        "api_ok": api_ok,
        "snowflake_ok": snowflake_ok,
        "updated_at": updated_at,
        "last_run": last_run,
        "last_brief": last_brief,
        "outcomes": outcomes,
    }

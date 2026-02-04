import json
from fastapi import APIRouter, HTTPException

from app.audit_interpreter import interpret_timeline
from app.db import get_connection, fetch_all, serialize_row, serialize_rows

router = APIRouter(prefix="/runs", tags=["runs"])


def _summary_hint_from_status_and_details(status: str | None, details: dict | None) -> str | None:
    """Derive a short summary hint for the runs list (e.g. 'No new bars')."""
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


@router.get("")
def list_runs(limit: int = 50):
    """Recent pipeline runs from MIP_AUDIT_LOG. Returns run_id, started_at, completed_at, status, summary_hint."""
    sql = """
    select
        EVENT_TS,
        RUN_ID,
        STATUS,
        ROWS_AFFECTED,
        DETAILS
    from MIP.APP.MIP_AUDIT_LOG
    where EVENT_TYPE = 'PIPELINE'
      and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
    order by EVENT_TS desc
    limit %s
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (limit * 2,))  # fetch extra so we get START + END per run
        rows = fetch_all(cur)
    finally:
        conn.close()

    # Group by RUN_ID: one row per run with started_at = min(ts), completed_at = max(ts), status from completion row
    runs_by_id: dict = {}
    for r in rows:
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

    # Build list: take completion status (prefer non-START), summary_hint
    out = []
    for run_id, run in runs_by_id.items():
        started_at = run["started_at"]
        completed_at = run["completed_at"]
        status = run["status"] if run["status"] != "START" else "RUNNING"
        summary_hint = _summary_hint_from_status_and_details(status, run.get("details"))
        out.append({
            "run_id": run_id,
            "started_at": started_at.isoformat() if hasattr(started_at, "isoformat") else started_at,
            "completed_at": completed_at.isoformat() if hasattr(completed_at, "isoformat") else completed_at,
            "status": status,
            "summary_hint": summary_hint,
        })
    out.sort(key=lambda x: (x["completed_at"] or x["started_at"] or ""), reverse=True)
    out = out[:limit]
    return [serialize_row(r) for r in out]


@router.get("/{run_id}")
def get_run(run_id: str):
    """All audit events for the run (ordered by EVENT_TS) + interpreted narrative (interpreted_narrative, sections, etc.)."""
    sql = """
    select
        EVENT_TS,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        ERROR_MESSAGE,
        DETAILS
    from MIP.APP.MIP_AUDIT_LOG
    where RUN_ID = %s
       or PARENT_RUN_ID = %s
    order by EVENT_TS
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (run_id, run_id))
        rows = fetch_all(cur)
        if not rows:
            raise HTTPException(status_code=404, detail="Run not found")
        serialized = serialize_rows(rows)
        interpreted = interpret_timeline(rows)
        interpreted["timeline"] = serialized
        return interpreted
    finally:
        conn.close()

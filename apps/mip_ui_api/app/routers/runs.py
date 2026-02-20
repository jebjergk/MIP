import json
from datetime import datetime, date
from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from app.audit_interpreter import interpret_timeline
from app.db import get_connection, fetch_all, serialize_row, serialize_rows

router = APIRouter(prefix="/runs", tags=["runs"])


# ---------------------------------------------------------------------------
# Intraday run helpers
# ---------------------------------------------------------------------------

INTRADAY_RUNS_SQL = """
SELECT
    RUN_ID,
    INTERVAL_MINUTES,
    STARTED_AT,
    COMPLETED_AT,
    STATUS,
    BARS_INGESTED,
    SIGNALS_GENERATED,
    OUTCOMES_EVALUATED,
    SYMBOLS_PROCESSED,
    DAILY_CONTEXT_USED,
    COMPUTE_SECONDS,
    DETAILS
FROM MIP.APP.INTRADAY_PIPELINE_RUN_LOG
{where}
ORDER BY STARTED_AT DESC
LIMIT %s
"""


def _intraday_summary_hint(status: str | None) -> str | None:
    if not status:
        return None
    s = (status or "").upper()
    if s == "SKIPPED_DISABLED":
        return "Disabled"
    if s == "PARTIAL":
        return "Partial success"
    if s == "FAIL":
        return "Failed"
    if s == "SUCCESS":
        return None
    return None


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


def _generate_debug_sql(run_id: str) -> dict:
    """Generate debug SQL queries for a given run_id."""
    return {
        "all_events": f"""-- All audit events for this run
SELECT EVENT_TS, EVENT_TYPE, EVENT_NAME, STATUS, ROWS_AFFECTED, 
       ERROR_MESSAGE, ERROR_SQLSTATE, ERROR_QUERY_ID, DURATION_MS, DETAILS
FROM MIP.APP.MIP_AUDIT_LOG
WHERE RUN_ID = '{run_id}' OR PARENT_RUN_ID = '{run_id}'
ORDER BY EVENT_TS;""",
        "failed_steps": f"""-- Failed steps with error details
SELECT EVENT_TS, EVENT_NAME, STATUS, ERROR_MESSAGE, ERROR_SQLSTATE, 
       ERROR_QUERY_ID, ERROR_CONTEXT, DURATION_MS
FROM MIP.APP.MIP_AUDIT_LOG
WHERE (RUN_ID = '{run_id}' OR PARENT_RUN_ID = '{run_id}')
  AND STATUS IN ('FAIL', 'ERROR')
ORDER BY EVENT_TS;""",
        "step_timeline": f"""-- Step timeline with durations
SELECT EVENT_TS, EVENT_NAME, STATUS, DURATION_MS,
       DETAILS:step_name::string as STEP_NAME,
       DETAILS:started_at::timestamp as STARTED_AT,
       DETAILS:completed_at::timestamp as COMPLETED_AT
FROM MIP.APP.MIP_AUDIT_LOG
WHERE PARENT_RUN_ID = '{run_id}'
  AND EVENT_TYPE IN ('PIPELINE_STEP', 'REPLAY')
ORDER BY EVENT_TS;""",
        "query_history": f"""-- Query history for failed query (if ERROR_QUERY_ID captured)
SELECT QUERY_ID, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE, 
       START_TIME, END_TIME, EXECUTION_STATUS
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_ID IN (
    SELECT ERROR_QUERY_ID 
    FROM MIP.APP.MIP_AUDIT_LOG 
    WHERE (RUN_ID = '{run_id}' OR PARENT_RUN_ID = '{run_id}')
      AND ERROR_QUERY_ID IS NOT NULL
);"""
    }


@router.get("")
def list_runs(
    limit: int = 50,
    status: Optional[str] = Query(None, description="Filter by status (FAIL, SUCCESS, SUCCESS_WITH_SKIPS, etc.)"),
    portfolio_id: Optional[int] = Query(None, description="Filter by portfolio_id (searches DETAILS JSON)"),
    from_ts: Optional[str] = Query(None, description="Filter runs started after this timestamp (ISO format)"),
    to_ts: Optional[str] = Query(None, description="Filter runs started before this timestamp (ISO format)")
):
    """Recent pipeline runs from MIP_AUDIT_LOG. Returns run_id, started_at, completed_at, status, summary_hint, has_errors, error_count."""
    # Build dynamic WHERE clause
    where_clauses = [
        "EVENT_TYPE = 'PIPELINE'",
        "EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'"
    ]
    params = []
    
    if from_ts:
        where_clauses.append("EVENT_TS >= %s")
        params.append(from_ts)
    if to_ts:
        where_clauses.append("EVENT_TS <= %s")
        params.append(to_ts)
    
    where_sql = " AND ".join(where_clauses)
    
    sql = f"""
    SELECT
        EVENT_TS,
        RUN_ID,
        STATUS,
        ROWS_AFFECTED,
        DETAILS,
        ERROR_MESSAGE,
        ERROR_SQLSTATE,
        ERROR_QUERY_ID
    FROM MIP.APP.MIP_AUDIT_LOG
    WHERE {where_sql}
    ORDER BY EVENT_TS DESC
    LIMIT %s
    """
    params.append(limit * 2)  # fetch extra so we get START + END per run
    
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        rows = fetch_all(cur)
        
        # If portfolio_id filter, get run_ids with matching portfolio
        portfolio_run_ids = None
        if portfolio_id is not None:
            portfolio_sql = """
            SELECT DISTINCT PARENT_RUN_ID
            FROM MIP.APP.MIP_AUDIT_LOG
            WHERE EVENT_TYPE = 'PIPELINE_STEP'
              AND DETAILS:portfolio_id = %s
            """
            cur.execute(portfolio_sql, (portfolio_id,))
            portfolio_rows = fetch_all(cur)
            portfolio_run_ids = {r["PARENT_RUN_ID"] for r in portfolio_rows if r.get("PARENT_RUN_ID")}
        
        # Get error counts per run
        run_ids = list({r.get("RUN_ID") for r in rows if r.get("RUN_ID")})
        error_counts = {}
        if run_ids:
            placeholders = ", ".join(["%s"] * len(run_ids))
            error_sql = f"""
            SELECT PARENT_RUN_ID, COUNT(*) as ERROR_COUNT
            FROM MIP.APP.MIP_AUDIT_LOG
            WHERE PARENT_RUN_ID IN ({placeholders})
              AND STATUS IN ('FAIL', 'ERROR')
            GROUP BY PARENT_RUN_ID
            """
            cur.execute(error_sql, tuple(run_ids))
            for row in fetch_all(cur):
                error_counts[row["PARENT_RUN_ID"]] = row["ERROR_COUNT"]
    finally:
        conn.close()

    # Group by RUN_ID: one row per run with started_at = min(ts), completed_at = max(ts), status from completion row
    runs_by_id: dict = {}
    for r in rows:
        run_id = r.get("RUN_ID")
        if not run_id:
            continue
        # Filter by portfolio_id if specified
        if portfolio_run_ids is not None and run_id not in portfolio_run_ids:
            continue
        ts = r.get("EVENT_TS")
        row_status = r.get("STATUS") or ""
        details = r.get("DETAILS")
        if isinstance(details, str):
            try:
                details = json.loads(details) if details else {}
            except Exception:
                details = {}
        error_message = r.get("ERROR_MESSAGE")
        error_sqlstate = r.get("ERROR_SQLSTATE")
        error_query_id = r.get("ERROR_QUERY_ID")
        
        if run_id not in runs_by_id:
            runs_by_id[run_id] = {
                "started_at": ts, 
                "completed_at": ts, 
                "status": row_status, 
                "details": details,
                "error_message": error_message,
                "error_sqlstate": error_sqlstate,
                "error_query_id": error_query_id
            }
        else:
            run = runs_by_id[run_id]
            if ts:
                if run["started_at"] is None or (ts < run["started_at"]):
                    run["started_at"] = ts
                if run["completed_at"] is None or (ts > run["completed_at"]):
                    run["completed_at"] = ts
                    run["status"] = row_status
                    run["details"] = details
                    if error_message:
                        run["error_message"] = error_message
                    if error_sqlstate:
                        run["error_sqlstate"] = error_sqlstate
                    if error_query_id:
                        run["error_query_id"] = error_query_id

    # Build list: take completion status (prefer non-START), summary_hint
    out = []
    for run_id, run in runs_by_id.items():
        started_at = run["started_at"]
        completed_at = run["completed_at"]
        run_status = run["status"] if run["status"] != "START" else "RUNNING"
        
        # Filter by status if specified
        if status and run_status.upper() != status.upper():
            continue
            
        summary_hint = _summary_hint_from_status_and_details(run_status, run.get("details"))
        error_count = error_counts.get(run_id, 0)
        
        out.append({
            "run_id": run_id,
            "started_at": started_at.isoformat() if hasattr(started_at, "isoformat") else started_at,
            "completed_at": completed_at.isoformat() if hasattr(completed_at, "isoformat") else completed_at,
            "status": run_status,
            "summary_hint": summary_hint,
            "has_errors": run_status in ("FAIL", "ERROR") or error_count > 0,
            "error_count": error_count,
            "error_message": run.get("error_message"),
            "error_sqlstate": run.get("error_sqlstate"),
            "error_query_id": run.get("error_query_id"),
        })
    out.sort(key=lambda x: (x["completed_at"] or x["started_at"] or ""), reverse=True)
    out = out[:limit]
    return [serialize_row(r) for r in out]


# ---------------------------------------------------------------------------
# Intraday pipeline runs (from INTRADAY_PIPELINE_RUN_LOG)
# IMPORTANT: these must be defined BEFORE /{run_id} to avoid route shadowing.
# ---------------------------------------------------------------------------

@router.get("/intraday")
def list_intraday_runs(
    limit: int = 50,
    status: Optional[str] = Query(None, description="Filter by status"),
    from_ts: Optional[str] = Query(None, description="Filter runs after this timestamp"),
    to_ts: Optional[str] = Query(None, description="Filter runs before this timestamp"),
):
    """List intraday pipeline runs from INTRADAY_PIPELINE_RUN_LOG."""
    where_clauses = []
    params = []

    if status:
        where_clauses.append("STATUS = %s")
        params.append(status.upper())
    if from_ts:
        where_clauses.append("STARTED_AT >= %s")
        params.append(from_ts)
    if to_ts:
        where_clauses.append("STARTED_AT <= %s")
        params.append(to_ts)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    params.append(limit)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(INTRADAY_RUNS_SQL.format(where=where), tuple(params))
        rows = fetch_all(cur)
    finally:
        conn.close()

    out = []
    for r in rows:
        details = r.get("DETAILS")
        if isinstance(details, str):
            try:
                details = json.loads(details) if details else {}
            except Exception:
                details = {}

        run_status = r.get("STATUS") or ""
        started_at = r.get("STARTED_AT")
        completed_at = r.get("COMPLETED_AT")
        is_error = run_status.upper() in ("FAIL", "ERROR")

        out.append({
            "run_id": r.get("RUN_ID"),
            "started_at": started_at.isoformat() if hasattr(started_at, "isoformat") else started_at,
            "completed_at": completed_at.isoformat() if hasattr(completed_at, "isoformat") else completed_at,
            "status": run_status,
            "summary_hint": _intraday_summary_hint(run_status),
            "has_errors": is_error,
            "error_count": 1 if is_error else 0,
            "interval_minutes": r.get("INTERVAL_MINUTES"),
            "bars_ingested": r.get("BARS_INGESTED"),
            "signals_generated": r.get("SIGNALS_GENERATED"),
            "outcomes_evaluated": r.get("OUTCOMES_EVALUATED"),
            "symbols_processed": r.get("SYMBOLS_PROCESSED"),
            "compute_seconds": r.get("COMPUTE_SECONDS"),
        })
    return [serialize_row(r) for r in out]


@router.get("/intraday/{run_id}")
def get_intraday_run(run_id: str):
    """Detail for a single intraday pipeline run."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT *
            FROM MIP.APP.INTRADAY_PIPELINE_RUN_LOG
            WHERE RUN_ID = %s
        """, (run_id,))
        rows = fetch_all(cur)
        if not rows:
            raise HTTPException(status_code=404, detail="Intraday run not found")

        r = rows[0]
        details = r.get("DETAILS")
        if isinstance(details, str):
            try:
                details = json.loads(details) if details else {}
            except Exception:
                details = {}

        started_at = r.get("STARTED_AT")
        completed_at = r.get("COMPLETED_AT")
        duration_ms = None
        if started_at and completed_at and hasattr(started_at, "timestamp") and hasattr(completed_at, "timestamp"):
            duration_ms = int((completed_at.timestamp() - started_at.timestamp()) * 1000)

        is_error = (r.get("STATUS") or "").upper() in ("FAIL", "ERROR")
        error_msg = None
        if is_error and isinstance(details, dict):
            error_msg = details.get("error") or details.get("error_message")

        return serialize_row({
            "run_id": r.get("RUN_ID"),
            "interval_minutes": r.get("INTERVAL_MINUTES"),
            "started_at": started_at.isoformat() if hasattr(started_at, "isoformat") else started_at,
            "completed_at": completed_at.isoformat() if hasattr(completed_at, "isoformat") else completed_at,
            "status": r.get("STATUS"),
            "bars_ingested": r.get("BARS_INGESTED"),
            "signals_generated": r.get("SIGNALS_GENERATED"),
            "outcomes_evaluated": r.get("OUTCOMES_EVALUATED"),
            "symbols_processed": r.get("SYMBOLS_PROCESSED"),
            "daily_context_used": r.get("DAILY_CONTEXT_USED"),
            "compute_seconds": r.get("COMPUTE_SECONDS"),
            "details": details,
            "total_duration_ms": duration_ms,
            "has_errors": is_error,
            "error_count": 1 if is_error else 0,
            "error_message": error_msg,
        })
    finally:
        conn.close()


@router.get("/{run_id}")
def get_run(run_id: str):
    """All audit events for the run (ordered by EVENT_TS) + interpreted narrative, error details, step timeline, and debug SQL."""
    sql = """
    SELECT
        EVENT_TS,
        RUN_ID,
        PARENT_RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        ERROR_MESSAGE,
        ERROR_SQLSTATE,
        ERROR_QUERY_ID,
        ERROR_CONTEXT,
        DURATION_MS,
        DETAILS
    FROM MIP.APP.MIP_AUDIT_LOG
    WHERE RUN_ID = %s
       OR PARENT_RUN_ID = %s
    ORDER BY EVENT_TS
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
        
        # Build step timeline with enhanced details
        steps = []
        for row in rows:
            if row.get("EVENT_TYPE") not in ("PIPELINE_STEP", "REPLAY"):
                continue
            details = row.get("DETAILS")
            if isinstance(details, str):
                try:
                    details = json.loads(details) if details else {}
                except Exception:
                    details = {}
            
            step_name = details.get("step_name") if isinstance(details, dict) else None
            started_at = details.get("started_at") if isinstance(details, dict) else None
            completed_at = details.get("completed_at") if isinstance(details, dict) else None
            
            step = {
                "event_ts": row.get("EVENT_TS").isoformat() if hasattr(row.get("EVENT_TS"), "isoformat") else row.get("EVENT_TS"),
                "run_id": row.get("RUN_ID"),
                "event_name": row.get("EVENT_NAME"),
                "step_name": step_name or row.get("EVENT_NAME"),
                "status": row.get("STATUS"),
                "rows_affected": row.get("ROWS_AFFECTED"),
                "duration_ms": row.get("DURATION_MS"),
                "started_at": started_at,
                "completed_at": completed_at,
                "error_message": row.get("ERROR_MESSAGE"),
                "error_sqlstate": row.get("ERROR_SQLSTATE"),
                "error_query_id": row.get("ERROR_QUERY_ID"),
                "error_context": row.get("ERROR_CONTEXT"),
                "scope": details.get("scope") if isinstance(details, dict) else None,
                "scope_key": details.get("scope_key") if isinstance(details, dict) else None,
                "portfolio_id": details.get("portfolio_id") if isinstance(details, dict) else None,
            }
            steps.append(step)
        
        interpreted["steps"] = steps
        
        # Extract all errors across the run
        errors = []
        for row in rows:
            if row.get("STATUS") in ("FAIL", "ERROR") or row.get("ERROR_MESSAGE"):
                error_context = row.get("ERROR_CONTEXT")
                if isinstance(error_context, str):
                    try:
                        error_context = json.loads(error_context) if error_context else None
                    except Exception:
                        pass
                
                errors.append({
                    "event_ts": row.get("EVENT_TS").isoformat() if hasattr(row.get("EVENT_TS"), "isoformat") else row.get("EVENT_TS"),
                    "event_name": row.get("EVENT_NAME"),
                    "status": row.get("STATUS"),
                    "error_message": row.get("ERROR_MESSAGE"),
                    "error_sqlstate": row.get("ERROR_SQLSTATE"),
                    "error_query_id": row.get("ERROR_QUERY_ID"),
                    "error_context": error_context,
                    "duration_ms": row.get("DURATION_MS"),
                })
        
        interpreted["errors"] = errors
        interpreted["has_errors"] = len(errors) > 0
        interpreted["error_count"] = len(errors)
        
        # Add first failed step info for quick access
        failed_step = None
        for step in steps:
            if step.get("status") in ("FAIL", "ERROR"):
                failed_step = step
                break
        interpreted["failed_step"] = failed_step
        
        # Generate debug SQL for this run
        interpreted["debug_sql"] = _generate_debug_sql(run_id)
        
        # Compute total run duration
        if rows:
            first_ts = rows[0].get("EVENT_TS")
            last_ts = rows[-1].get("EVENT_TS")
            if first_ts and last_ts and hasattr(first_ts, "timestamp") and hasattr(last_ts, "timestamp"):
                total_duration_ms = int((last_ts.timestamp() - first_ts.timestamp()) * 1000)
                interpreted["total_duration_ms"] = total_duration_ms
        
        return interpreted
    finally:
        conn.close()

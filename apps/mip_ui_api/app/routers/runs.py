from fastapi import APIRouter, HTTPException

from app.audit_interpreter import interpret_timeline
from app.db import get_connection, fetch_all, serialize_rows

router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("")
def list_runs(limit: int = 50):
    """Recent pipeline runs from MIP_AUDIT_LOG."""
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
        cur.execute(sql, (limit,))
        rows = fetch_all(cur)
        return serialize_rows(rows)
    finally:
        conn.close()


@router.get("/{run_id}")
def get_run(run_id: str):
    """Timeline (audit rows for run) + interpreted summary (summary_cards, narrative_bullets)."""
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
        # phases + sections are already in interpreted (structured "what happened and why")
        return interpreted
    finally:
        conn.close()

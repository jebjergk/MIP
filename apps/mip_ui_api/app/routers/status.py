"""
System status / health endpoint. Read-only.
Returns API health, Snowflake reachability, env info, and latest pipeline run info.
Never exposes secrets (passwords, keys, passphrases).
"""
from datetime import datetime, timezone

from fastapi import APIRouter

from app.config import get_snowflake_config
from app.db import get_connection, SnowflakeAuthError, serialize_row

router = APIRouter(tags=["status"])


def _get_latest_pipeline_run(conn):
    """
    Get the latest successful pipeline run info for freshness checks.
    
    Uses the audit log as the source of truth (same as digest staleness check).
    This ensures consistency across the UI - the same run ID is considered "latest"
    everywhere (digests, portfolios, status badge).
    
    Note: We use audit log, NOT PORTFOLIO.LAST_SIMULATION_RUN_ID, because that field
    is set by a different procedure (SP_RUN_PORTFOLIO_SIMULATION) with a different run ID.
    """
    try:
        cur = conn.cursor()
        # Get latest successful pipeline run from audit log
        # This matches the staleness check in digest/briefs.py
        cur.execute("""
            select 
                RUN_ID as run_id,
                EVENT_TS as run_ts
            from MIP.APP.MIP_AUDIT_LOG
            where EVENT_TYPE = 'PIPELINE'
              and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
              and STATUS in ('SUCCESS', 'SUCCESS_WITH_SKIPS')
            order by EVENT_TS desc
            limit 1
        """)
        row = cur.fetchone()
        if row and cur.description:
            columns = [d[0].lower() for d in cur.description]
            data = serialize_row(dict(zip(columns, row)))
            return {
                "latest_success_run_id": data.get("run_id"),
                "latest_success_ts": data.get("run_ts"),
            }
    except Exception:
        pass
    return {
        "latest_success_run_id": None,
        "latest_success_ts": None,
    }


def _is_pipeline_running(conn) -> bool:
    """
    Check if a pipeline is currently running.
    
    A pipeline is running if there is a START event in the last 2 hours
    that has not yet been followed by a SUCCESS/FAIL/SUCCESS_WITH_SKIPS event.
    Mirrors SP_CHECK_PIPELINE_SAFE_FOR_EDIT logic.
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            select count(*) as active_count
            from MIP.APP.MIP_AUDIT_LOG start_evt
            where start_evt.EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
              and start_evt.STATUS = 'START'
              and start_evt.EVENT_TS > dateadd(hour, -2, current_timestamp())
              and not exists (
                  select 1
                  from MIP.APP.MIP_AUDIT_LOG end_evt
                  where end_evt.EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
                    and end_evt.RUN_ID = start_evt.RUN_ID
                    and end_evt.STATUS in ('SUCCESS', 'SUCCESS_WITH_SKIPS', 'FAIL')
              )
        """)
        row = cur.fetchone()
        return row[0] > 0 if row else False
    except Exception:
        return False


@router.get("/status")
def get_status():
    """
    Health/status: api_ok, snowflake_ok, auth_method, warehouse/database/schema, 
    latest_success_run_id, latest_success_ts, timestamp.
    Used by the UI to show a header banner (green/yellow/red) and freshness badges.
    """
    cfg = get_snowflake_config()
    auth_method = cfg.get("auth_method") or "password"
    warehouse = cfg.get("warehouse")
    database = cfg.get("database")
    schema = cfg.get("schema")

    snowflake_ok = False
    snowflake_message = None
    latest_run_info = {"latest_success_run_id": None, "latest_success_ts": None}
    pipeline_running = False
    
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            snowflake_ok = True
            # Fetch latest pipeline run info
            latest_run_info = _get_latest_pipeline_run(conn)
            pipeline_running = _is_pipeline_running(conn)
        finally:
            conn.close()
    except SnowflakeAuthError as e:
        snowflake_message = str(e)
    except Exception:
        snowflake_message = "Connection failed"

    return {
        "api_ok": True,
        "snowflake_ok": snowflake_ok,
        "auth_method": auth_method,
        "warehouse": warehouse if warehouse else None,
        "database": database if database else None,
        "schema": schema if schema else None,
        "snowflake_message": snowflake_message,
        "latest_success_run_id": latest_run_info.get("latest_success_run_id"),
        "latest_success_ts": latest_run_info.get("latest_success_ts"),
        "pipeline_running": pipeline_running,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

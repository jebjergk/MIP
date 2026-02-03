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
    """Get the latest successful pipeline run info for freshness checks."""
    try:
        cur = conn.cursor()
        # Get latest run from any active portfolio
        cur.execute("""
            select
                LAST_SIMULATION_RUN_ID as run_id,
                LAST_SIMULATED_AT as run_ts
            from MIP.APP.PORTFOLIO
            where STATUS = 'ACTIVE'
              and LAST_SIMULATION_RUN_ID is not null
            order by LAST_SIMULATED_AT desc
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
    
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            snowflake_ok = True
            # Fetch latest pipeline run info
            latest_run_info = _get_latest_pipeline_run(conn)
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

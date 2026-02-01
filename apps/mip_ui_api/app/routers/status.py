"""
System status / health endpoint. Read-only.
Returns API health, Snowflake reachability, and env info (warehouse/database/schema).
"""
from datetime import datetime, timezone

from fastapi import APIRouter

from app.config import get_snowflake_config
from app.db import get_connection

router = APIRouter(tags=["status"])


@router.get("/status")
def get_status():
    """
    Health/status: api_ok, snowflake_ok, warehouse/database/schema from env, timestamp.
    Used by the UI to show a header banner (green/yellow/red).
    """
    cfg = get_snowflake_config()
    warehouse = cfg.get("warehouse")
    database = cfg.get("database")
    schema = cfg.get("schema")
    # None from os.getenv becomes None; keep as None for JSON
    env_info = {
        "warehouse": warehouse if warehouse else None,
        "database": database if database else None,
        "schema": schema if schema else None,
    }

    snowflake_ok = False
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            snowflake_ok = True
        finally:
            conn.close()
    except Exception:
        snowflake_ok = False

    return {
        "api_ok": True,
        "snowflake_ok": snowflake_ok,
        "warehouse": env_info["warehouse"],
        "database": env_info["database"],
        "schema": env_info["schema"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

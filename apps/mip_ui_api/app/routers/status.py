"""
System status / health endpoint. Read-only.
Returns API health, Snowflake reachability, and env info (warehouse/database/schema).
Never exposes secrets (passwords, keys, passphrases).
"""
from datetime import datetime, timezone

from fastapi import APIRouter

from app.config import get_snowflake_config
from app.db import get_connection, SnowflakeAuthError

router = APIRouter(tags=["status"])


@router.get("/status")
def get_status():
    """
    Health/status: api_ok, snowflake_ok, auth_method, warehouse/database/schema, timestamp.
    Used by the UI to show a header banner (green/yellow/red).
    """
    cfg = get_snowflake_config()
    auth_method = cfg.get("auth_method") or "password"
    warehouse = cfg.get("warehouse")
    database = cfg.get("database")
    schema = cfg.get("schema")

    snowflake_ok = False
    snowflake_message = None
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            snowflake_ok = True
        finally:
            conn.close()
    except SnowflakeAuthError as e:
        snowflake_message = str(e)
    except Exception as e:
        snowflake_message = "Connection failed"

    return {
        "api_ok": True,
        "snowflake_ok": snowflake_ok,
        "auth_method": auth_method,
        "warehouse": warehouse if warehouse else None,
        "database": database if database else None,
        "schema": schema if schema else None,
        "snowflake_message": snowflake_message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

import snowflake.connector

from app.config import get_snowflake_config


def get_connection():
    """Read-only Snowflake connection from env. No writes from this API."""
    cfg = get_snowflake_config()
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=cfg["password"],
        role=cfg["role"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
    )


def fetch_all(cursor):
    columns = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def serialize_row(row):
    """Convert row dict for JSON: handle dates, decimals, etc."""
    if row is None:
        return None
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "__float__") and not isinstance(v, (int, bool)):
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                out[k] = str(v) if v is not None else None
        else:
            out[k] = v
    return out


def serialize_rows(rows):
    return [serialize_row(r) for r in rows] if rows else []

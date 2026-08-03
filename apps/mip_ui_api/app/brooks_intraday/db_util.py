from __future__ import annotations

from app.db import fetch_all, get_connection


def query_rows(sql: str, params=()) -> list[dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return [{str(k).lower(): v for k, v in row.items()} for row in rows]
    finally:
        conn.close()

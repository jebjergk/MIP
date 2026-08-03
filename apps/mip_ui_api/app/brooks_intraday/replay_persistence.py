from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any

from app.db import get_connection

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def persist_replay_cursor(run_id: str, cursor: dict[str, Any]) -> None:
    cursor = {**cursor, "updated_at_utc": _utc_now().isoformat(timespec="seconds")}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE INTO MIP.APP.BROOKS_INTRADAY_REPLAY_STATE t
            USING (
                SELECT %s AS RUN_ID, PARSE_JSON(%s) AS REPLAY_CURSOR_JSON
            ) s
            ON t.RUN_ID = s.RUN_ID
            WHEN MATCHED THEN UPDATE SET
                REPLAY_CURSOR_JSON = s.REPLAY_CURSOR_JSON,
                UPDATED_AT_UTC = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (RUN_ID, REPLAY_CURSOR_JSON, UPDATED_AT_UTC)
            VALUES (s.RUN_ID, s.REPLAY_CURSOR_JSON, CURRENT_TIMESTAMP())
            """,
            (run_id, json.dumps(cursor, default=str)),
        )
        conn.commit()
    except Exception as exc:
        logger.warning("replay cursor table persist skipped run_id=%s err=%s", run_id, exc)
    finally:
        conn.close()


def load_replay_cursor_table(run_id: str) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT REPLAY_CURSOR_JSON FROM MIP.APP.BROOKS_INTRADAY_REPLAY_STATE WHERE RUN_ID = %s",
            (run_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        val = row[0]
        if isinstance(val, str):
            return json.loads(val)
        return val
    except Exception:
        return None
    finally:
        conn.close()

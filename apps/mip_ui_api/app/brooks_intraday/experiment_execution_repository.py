"""Snowflake persistence for Phase 9A experiment orchestration."""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from app.db import get_connection

_FIELD_MAP = {
    "overall_status": "OVERALL_STATUS",
    "current_week_start": "CURRENT_WEEK_START",
    "validation_run_id": "VALIDATION_RUN_ID",
    "current_stage": "CURRENT_STAGE",
    "requested_action": "REQUESTED_ACTION",
    "run_all_remaining": "RUN_ALL_REMAINING",
    "pause_requested": "PAUSE_REQUESTED",
    "last_success_action": "LAST_SUCCESS_ACTION",
    "next_automatic_action": "NEXT_AUTOMATIC_ACTION",
    "lease_owner": "LEASE_OWNER",
    "lease_until": "LEASE_UNTIL",
    "cohort_json": "COHORT_JSON",
    "progress_json": "PROGRESS_JSON",
    "attempt_ids_json": "ATTEMPT_IDS_JSON",
    "tws_state_json": "TWS_STATE_JSON",
    "latest_error_json": "LATEST_ERROR_JSON",
}

_JSON_FIELDS = frozenset(
    {"cohort_json", "progress_json", "attempt_ids_json", "tws_state_json", "latest_error_json"}
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json_dumps(val: Any) -> str | None:
    if val is None:
        return None
    return json.dumps(val, default=str)


def _parse_json(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return val
    return val


def get_active_execution() -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM MIP.APP.BROOKS_EXPERIMENT_EXECUTION
            ORDER BY CREATED_AT DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0].lower() for d in cur.description]
        rec = dict(zip(cols, row))
        for k in _JSON_FIELDS:
            rec[k] = _parse_json(rec.get(k))
        return rec
    finally:
        conn.close()


def insert_execution(
    *,
    freeze_id: str,
    cohort: list[str],
    overall_status: str,
    progress: dict | None = None,
) -> str:
    eid = str(uuid.uuid4())
    conn = get_connection()
    try:
        cur = conn.cursor()
        # Snowflake rejects PARSE_JSON(...) inside INSERT ... VALUES; use SELECT.
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_EXPERIMENT_EXECUTION (
                EXECUTION_ID, FREEZE_ID, COHORT_JSON, OVERALL_STATUS, PROGRESS_JSON
            )
            SELECT %s, %s, PARSE_JSON(%s), %s, PARSE_JSON(%s)
            """,
            (eid, freeze_id, _json_dumps(cohort), overall_status, _json_dumps(progress or {})),
        )
        conn.commit()
        return eid
    finally:
        conn.close()


def update_execution(execution_id: str, **fields: Any) -> None:
    if not fields:
        return
    sets: list[str] = []
    params: list[Any] = []
    for key, val in fields.items():
        col = _FIELD_MAP.get(key, key.upper())
        if key in _JSON_FIELDS:
            sets.append(f"{col} = PARSE_JSON(%s)")
            params.append(_json_dumps(val))
        else:
            sets.append(f"{col} = %s")
            params.append(val)
    sets.append("UPDATED_AT = CURRENT_TIMESTAMP()")
    params.append(execution_id)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE MIP.APP.BROOKS_EXPERIMENT_EXECUTION SET {', '.join(sets)} WHERE EXECUTION_ID = %s",
            tuple(params),
        )
        conn.commit()
    finally:
        conn.close()


def try_claim_lease(execution_id: str, owner: str, lease_sec: int) -> bool:
    until = _utc_now() + timedelta(seconds=lease_sec)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_EXPERIMENT_EXECUTION
            SET LEASE_OWNER = %s,
                LEASE_UNTIL = %s,
                HEARTBEAT_AT = CURRENT_TIMESTAMP(),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE EXECUTION_ID = %s
              AND (LEASE_UNTIL IS NULL OR LEASE_UNTIL < CURRENT_TIMESTAMP() OR LEASE_OWNER = %s)
            """,
            (owner, until, execution_id, owner),
        )
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def renew_lease(execution_id: str, owner: str, lease_sec: int) -> None:
    until = _utc_now() + timedelta(seconds=lease_sec)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_EXPERIMENT_EXECUTION
            SET LEASE_UNTIL = %s, HEARTBEAT_AT = CURRENT_TIMESTAMP()
            WHERE EXECUTION_ID = %s AND LEASE_OWNER = %s
            """,
            (until, execution_id, owner),
        )
        conn.commit()
    finally:
        conn.close()


def release_lease(execution_id: str, owner: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_EXPERIMENT_EXECUTION
            SET LEASE_OWNER = NULL, LEASE_UNTIL = NULL
            WHERE EXECUTION_ID = %s AND LEASE_OWNER = %s
            """,
            (execution_id, owner),
        )
        conn.commit()
    finally:
        conn.close()


def recover_stale_running_executions(stale_sec: int) -> int:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_EXPERIMENT_EXECUTION
            SET OVERALL_STATUS = 'PAUSED',
                LEASE_OWNER = NULL,
                LEASE_UNTIL = NULL,
                LAST_SUCCESS_ACTION = COALESCE(LAST_SUCCESS_ACTION, 'Prior run interrupted — safe to resume'),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE OVERALL_STATUS = 'RUNNING'
              AND (
                    HEARTBEAT_AT IS NULL
                    OR HEARTBEAT_AT < DATEADD(second, -%s, CURRENT_TIMESTAMP())
                  )
            """,
            (stale_sec,),
        )
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()


def upsert_stage(
    execution_id: str,
    week_start: date,
    stage_key: str,
    *,
    stage_status: str,
    attempt_id: str | None = None,
    row_count: int | None = None,
    content_hash: str | None = None,
    retry_count: int | None = None,
    error_json: dict | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT STAGE_KEY FROM MIP.APP.BROOKS_EXPERIMENT_STAGE
            WHERE EXECUTION_ID = %s AND WEEK_START = %s AND STAGE_KEY = %s
            """,
            (execution_id, week_start, stage_key),
        )
        exists = cur.fetchone()
        err_s = _json_dumps(error_json) if error_json else None
        if exists:
            err_clause = "ERROR_JSON = NULL" if err_s is None else "ERROR_JSON = PARSE_JSON(%s)"
            err_params: tuple[Any, ...] = () if err_s is None else (err_s,)
            cur.execute(
                f"""
                UPDATE MIP.APP.BROOKS_EXPERIMENT_STAGE SET
                    STAGE_STATUS = %s,
                    ATTEMPT_ID = COALESCE(%s, ATTEMPT_ID),
                    ROW_COUNT = COALESCE(%s, ROW_COUNT),
                    CONTENT_HASH = COALESCE(%s, CONTENT_HASH),
                    RETRY_COUNT = COALESCE(%s, RETRY_COUNT),
                    {err_clause},
                    STARTED_AT = COALESCE(%s, STARTED_AT),
                    FINISHED_AT = COALESCE(%s, FINISHED_AT),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE EXECUTION_ID = %s AND WEEK_START = %s AND STAGE_KEY = %s
                """,
                (
                    stage_status,
                    attempt_id,
                    row_count,
                    content_hash,
                    retry_count,
                    *err_params,
                    started_at,
                    finished_at,
                    execution_id,
                    week_start,
                    stage_key,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO MIP.APP.BROOKS_EXPERIMENT_STAGE (
                    EXECUTION_ID, WEEK_START, STAGE_KEY, STAGE_STATUS,
                    ATTEMPT_ID, ROW_COUNT, CONTENT_HASH, RETRY_COUNT, ERROR_JSON,
                    STARTED_AT, FINISHED_AT
                )
                SELECT %s, %s, %s, %s, %s, %s, %s, %s,
                       IFF(%s IS NULL, NULL, PARSE_JSON(%s)),
                       %s, %s
                """,
                (
                    execution_id,
                    week_start,
                    stage_key,
                    stage_status,
                    attempt_id,
                    row_count,
                    content_hash,
                    retry_count or 0,
                    err_s,
                    err_s,
                    started_at or _utc_now(),
                    finished_at,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def list_stages(execution_id: str, week_start: date | None = None) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if week_start:
            cur.execute(
                """
                SELECT * FROM MIP.APP.BROOKS_EXPERIMENT_STAGE
                WHERE EXECUTION_ID = %s AND WEEK_START = %s
                ORDER BY STAGE_KEY
                """,
                (execution_id, week_start),
            )
        else:
            cur.execute(
                """
                SELECT * FROM MIP.APP.BROOKS_EXPERIMENT_STAGE
                WHERE EXECUTION_ID = %s
                ORDER BY WEEK_START, STAGE_KEY
                """,
                (execution_id,),
            )
        cols = [d[0].lower() for d in cur.description]
        out = []
        for raw in cur.fetchall():
            rec = dict(zip(cols, raw))
            rec["error_json"] = _parse_json(rec.get("error_json"))
            out.append(rec)
        return out
    finally:
        conn.close()


def append_event(
    execution_id: str,
    message: str,
    *,
    severity: str = "INFO",
    stage: str | None = None,
    week_start: date | None = None,
    symbol: str | None = None,
    trading_date: date | None = None,
    detail: dict | None = None,
    recoverable: bool | None = None,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_EXPERIMENT_EVENT (
                EXECUTION_ID, SEVERITY, STAGE, WEEK_START, SYMBOL, TRADING_DATE,
                MESSAGE, DETAIL_JSON, RECOVERABLE
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s
            """,
            (
                execution_id,
                severity,
                stage,
                week_start,
                symbol,
                trading_date,
                message[:2000],
                _json_dumps(detail or {}),
                recoverable,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_events(execution_id: str, *, limit: int = 80) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT EVENT_ID, EVENT_TS, SEVERITY, STAGE, WEEK_START, SYMBOL, TRADING_DATE,
                   MESSAGE, DETAIL_JSON, RECOVERABLE
            FROM MIP.APP.BROOKS_EXPERIMENT_EVENT
            WHERE EXECUTION_ID = %s
            ORDER BY EVENT_ID DESC
            LIMIT %s
            """,
            (execution_id, limit),
        )
        cols = [d[0].lower() for d in cur.description]
        out = []
        for raw in cur.fetchall():
            rec = dict(zip(cols, raw))
            rec["detail_json"] = _parse_json(rec.get("detail_json"))
            out.append(rec)
        return list(reversed(out))
    finally:
        conn.close()

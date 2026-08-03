from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from app.db import get_connection

from .objective_ruleset_v01 import DEFAULT_PARAMETERS, RULESET_VERSION


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def create_replay_attempt(
    *,
    run_id: str,
    ruleset_version: str = RULESET_VERSION,
    starting_cursor: dict[str, Any] | None = None,
    notes: str | None = None,
    parameters: dict[str, Any] | None = None,
) -> str:
    attempt_id = str(uuid.uuid4())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_INTRADAY_REPLAY_ATTEMPT (
                REPLAY_ATTEMPT_ID, RUN_ID, RULESET_VERSION, STATUS,
                STARTING_CURSOR_JSON, RULESET_PARAMETERS_JSON, NOTES, CREATED_AT_UTC
            )
            SELECT %s, %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s, %s
            """,
            (
                attempt_id,
                run_id,
                ruleset_version,
                "IN_PROGRESS",
                _json_or_empty(starting_cursor),
                _json_or_empty(parameters or DEFAULT_PARAMETERS),
                notes,
                _utc_now(),
            ),
        )
        conn.commit()
        return attempt_id
    finally:
        conn.close()


def _json_or_empty(obj: dict | None) -> str:
    import json

    return json.dumps(obj or {})


def abandon_replay_attempt(*, attempt_id: str, notes: str | None = None) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_INTRADAY_REPLAY_ATTEMPT
            SET STATUS = %s,
                NOTES = COALESCE(%s, NOTES),
                COMPLETED_AT_UTC = %s
            WHERE REPLAY_ATTEMPT_ID = %s
            """,
            ("ABANDONED", notes, _utc_now(), attempt_id),
        )
        conn.commit()
    finally:
        conn.close()


def complete_replay_attempt(
    *,
    attempt_id: str,
    observation_count: int,
    sequence_hash: str | None,
    status: str = "COMPLETED",
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_INTRADAY_REPLAY_ATTEMPT
            SET STATUS = %s,
                OBSERVATION_COUNT = %s,
                OBSERVATION_SEQUENCE_HASH = %s,
                COMPLETED_AT_UTC = %s
            WHERE REPLAY_ATTEMPT_ID = %s
            """,
            (status, observation_count, sequence_hash, _utc_now(), attempt_id),
        )
        conn.commit()
    finally:
        conn.close()


def list_replay_attempts(run_id: str) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT REPLAY_ATTEMPT_ID, RUN_ID, RULESET_VERSION, STATUS, REVIEW_STATUS,
                   OBSERVATION_COUNT, OBSERVATION_SEQUENCE_HASH, NOTES, REVIEW_NOTES,
                   CREATED_AT_UTC, COMPLETED_AT_UTC
            FROM MIP.APP.BROOKS_INTRADAY_REPLAY_ATTEMPT
            WHERE RUN_ID = %s
            ORDER BY CREATED_AT_UTC
            """,
            (run_id,),
        )
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def mark_v01_attempts_superseded(*, run_id: str, except_attempt_id: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_INTRADAY_REPLAY_ATTEMPT
            SET REVIEW_STATUS = 'SUPERSEDED_INCOMPLETE',
                REVIEW_NOTES = 'Superseded by designated Phase 4 review baseline attempt.'
            WHERE RUN_ID = %s
              AND RULESET_VERSION = 'BROOKS_OBJECTIVE_RULESET_V0_1'
              AND REPLAY_ATTEMPT_ID <> %s
              AND (REVIEW_STATUS IS NULL OR REVIEW_STATUS <> 'DESIGNATED_REVIEW_BASELINE')
            """,
            (run_id, except_attempt_id),
        )
        conn.commit()
    finally:
        conn.close()


def designate_review_baseline(*, run_id: str, attempt_id: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_INTRADAY_REPLAY_ATTEMPT
            SET REVIEW_STATUS = 'DESIGNATED_REVIEW_BASELINE',
                REVIEW_NOTES = 'Official Phase 4 manual review baseline (BROOKS_OBJECTIVE_RULESET_V0_1).'
            WHERE REPLAY_ATTEMPT_ID = %s AND RUN_ID = %s
            """,
            (attempt_id, run_id),
        )
        conn.commit()
    finally:
        conn.close()
    mark_v01_attempts_superseded(run_id=run_id, except_attempt_id=attempt_id)


def get_review_baseline_attempt_id(run_id: str) -> str | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT REPLAY_ATTEMPT_ID
            FROM MIP.APP.BROOKS_INTRADAY_REPLAY_ATTEMPT
            WHERE RUN_ID = %s AND REVIEW_STATUS = 'DESIGNATED_REVIEW_BASELINE'
            ORDER BY COMPLETED_AT_UTC DESC NULLS LAST
            LIMIT 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None
    finally:
        conn.close()

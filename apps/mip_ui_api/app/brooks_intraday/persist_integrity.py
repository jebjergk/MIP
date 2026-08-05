"""Phase C integrity checks and reliable attempt failure marking."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Iterable

from app.db import get_connection

logger = logging.getLogger(__name__)

LEGACY_UNSCOPED_SIM_ATTEMPT = "LEGACY_UNSCOPED"


def normalize_bar_ts(ts: Any) -> str:
    if ts is None:
        return ""
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    s = str(ts).replace("T", " ")
    return s[:19]


def schedule_key_set(schedule: Iterable[Any], symbols: list[str], *, normalize_utc) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for step in schedule:
        ts = normalize_bar_ts(normalize_utc(step.bar_timestamp_utc) or step.bar_timestamp_utc)
        for sym in symbols:
            keys.add((str(sym).upper(), ts))
    return keys


def assert_schedule_keys_match(
    *,
    expected: set[tuple[str, str]],
    persisted: set[tuple[str, str]],
    label: str,
) -> None:
    missing = expected - persisted
    extra = persisted - expected
    if missing or extra:
        raise ValueError(
            f"{label} schedule-key mismatch: missing={len(missing)} extra={len(extra)} "
            f"expected={len(expected)} persisted={len(persisted)}"
        )


def load_observation_keys(run_id: str, *, replay_attempt_id: str) -> set[tuple[str, str]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SYMBOL, BAR_TS
            FROM MIP.APP.BROOKS_INTRADAY_BAR_OBSERVATION
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            """,
            (run_id, replay_attempt_id),
        )
        return {(str(r[0]).upper(), normalize_bar_ts(r[1])) for r in cur.fetchall()}
    finally:
        conn.close()


def load_context_keys(run_id: str, *, context_attempt_id: str) -> set[tuple[str, str]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SYMBOL, BAR_TS
            FROM MIP.APP.BROOKS_INTRADAY_CONTEXT_OBSERVATION
            WHERE RUN_ID = %s AND CONTEXT_ATTEMPT_ID = %s
            """,
            (run_id, context_attempt_id),
        )
        return {(str(r[0]).upper(), normalize_bar_ts(r[1])) for r in cur.fetchall()}
    finally:
        conn.close()


def load_bar_link_keys(run_id: str, *, replay_attempt_id: str) -> set[tuple[str, str]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SYMBOL, BAR_TS
            FROM MIP.APP.BROOKS_INTRADAY_BAR_PATTERN_LINK
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            """,
            (run_id, replay_attempt_id),
        )
        return {(str(r[0]).upper(), normalize_bar_ts(r[1])) for r in cur.fetchall()}
    finally:
        conn.close()


def _fail_update(sql: str, params: tuple[Any, ...]) -> None:
    """Always use a fresh connection so failure status commits even if writer is broken."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def fail_replay_attempt(*, attempt_id: str, notes: str) -> None:
    from .replay_attempt_repository import _utc_now

    _fail_update(
        """
        UPDATE MIP.APP.BROOKS_INTRADAY_REPLAY_ATTEMPT
        SET STATUS = %s,
            NOTES = LEFT(COALESCE(NOTES, '') || ' | ' || %s, 2000),
            COMPLETED_AT_UTC = %s
        WHERE REPLAY_ATTEMPT_ID = %s
        """,
        ("FAILED", notes, _utc_now(), attempt_id),
    )


def fail_context_attempt(*, context_attempt_id: str, notes: str) -> None:
    _fail_update(
        """
        UPDATE MIP.APP.BROOKS_INTRADAY_CONTEXT_ATTEMPT
        SET STATUS = %s,
            NOTES = LEFT(COALESCE(NOTES, '') || ' | ' || %s, 2000),
            COMPLETED_AT_UTC = CURRENT_TIMESTAMP()
        WHERE CONTEXT_ATTEMPT_ID = %s
        """,
        ("FAILED", notes, context_attempt_id),
    )


def fail_simulation_attempt(*, simulation_attempt_id: str, notes: str) -> None:
    _fail_update(
        """
        UPDATE MIP.APP.BROOKS_INTRADAY_SIMULATION_ATTEMPT
        SET STATUS = %s,
            NOTES = LEFT(COALESCE(NOTES, '') || ' | ' || %s, 2000),
            COMPLETED_AT_UTC = CURRENT_TIMESTAMP()
        WHERE SIMULATION_ATTEMPT_ID = %s
        """,
        ("FAILED", notes, simulation_attempt_id),
    )


def chunked(seq: list[Any], size: int) -> list[list[Any]]:
    if size <= 0:
        return [seq]
    return [seq[i : i + size] for i in range(0, len(seq), size)]

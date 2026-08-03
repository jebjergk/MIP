"""Persist Phase 6 context attempts and per-bar context observations."""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import date, datetime
from typing import Any

from app.db import get_connection

from .context_engine import ContextResult
from .context_ruleset_v01 import RULESET_VERSION as RULESET_V01
from .context_ruleset_v02 import DEFAULT_PARAMETERS as V02_PARAMS
from .context_ruleset_v02 import RULESET_VERSION as RULESET_V02

logger = logging.getLogger(__name__)


def create_context_attempt(
    *,
    run_id: str,
    objective_attempt_id: str,
    pattern_attempt_id: str,
    parameters: dict[str, Any] | None = None,
    notes: str | None = None,
    ruleset_version: str | None = None,
) -> str:
    rs = ruleset_version or RULESET_V01
    attempt_id = str(uuid.uuid4())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_INTRADAY_CONTEXT_ATTEMPT (
                CONTEXT_ATTEMPT_ID, RUN_ID, OBJECTIVE_ATTEMPT_ID, PATTERN_ATTEMPT_ID,
                CONTEXT_RULESET_VERSION, STATUS, RULESET_PARAMETERS_JSON, NOTES, CREATED_AT_UTC
            )
            SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, CURRENT_TIMESTAMP()
            """,
            (
                attempt_id,
                run_id,
                objective_attempt_id,
                pattern_attempt_id,
                rs,
                "IN_PROGRESS",
                json.dumps(parameters or (V02_PARAMS if rs == RULESET_V02 else {})),
                notes,
            ),
        )
        conn.commit()
        return attempt_id
    finally:
        conn.close()


def complete_context_attempt(
    *,
    context_attempt_id: str,
    row_count: int,
    sequence_hash: str | None,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_INTRADAY_CONTEXT_ATTEMPT
            SET STATUS = %s, ROW_COUNT = %s, SEQUENCE_HASH = %s, COMPLETED_AT_UTC = CURRENT_TIMESTAMP()
            WHERE CONTEXT_ATTEMPT_ID = %s
            """,
            ("COMPLETED", row_count, sequence_hash, context_attempt_id),
        )
        conn.commit()
    finally:
        conn.close()


def insert_context_observation(
    *,
    run_id: str,
    context_attempt_id: str,
    symbol: str,
    trading_date: date,
    bar_ts_utc: datetime,
    sequence_num: int,
    objective_attempt_id: str,
    pattern_attempt_id: str,
    dossier_id: str | None,
    result: ContextResult,
    conn: Any | None = None,
    commit: bool = True,
    ruleset_version: str | None = None,
) -> None:
    rs = ruleset_version or RULESET_V01
    own = conn is None
    if own:
        conn = get_connection()
    payload = {
        "layers_json": result.layers,
        "thesis_effect": result.thesis_effect,
        "state_before": result.state_before,
        "state_after": result.state_after,
        "selected_action": result.selected_action,
        "candidate_actions_json": result.candidate_actions,
        "blocked_candidates_json": result.blocked_candidates,
        "blockers_json": result.blockers,
        "supporting_evidence_json": result.supporting_evidence,
        "opposing_evidence_json": result.opposing_evidence,
        "active_levels_json": result.active_levels,
        "daily_thesis_invalidation": result.daily_thesis_invalidation,
        "intraday_setup_invalidation": result.intraday_setup_invalidation,
        "reclaim_stage": result.reclaim_stage,
        "support_status": result.support_status,
        "room_class": result.room_class,
        "context_classifications_json": result.context_classifications,
        "marker_flags_json": result.marker_flags,
        "rule_ids_json": result.rule_ids,
        "explanation": result.explanation,
    }
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE INTO MIP.APP.BROOKS_INTRADAY_CONTEXT_OBSERVATION t
            USING (
                SELECT %s AS CONTEXT_ATTEMPT_ID, %s AS SYMBOL, %s AS BAR_TS
            ) s
            ON t.CONTEXT_ATTEMPT_ID = s.CONTEXT_ATTEMPT_ID
               AND t.SYMBOL = s.SYMBOL AND t.BAR_TS = s.BAR_TS
            WHEN MATCHED THEN UPDATE SET
                SEQUENCE_NUM = %s,
                TRADING_DATE = %s,
                THESIS_EFFECT = %s,
                STATE_BEFORE = %s,
                STATE_AFTER = %s,
                SELECTED_ACTION = %s,
                PAYLOAD_JSON = PARSE_JSON(%s),
                EXPLANATION = %s
            WHEN NOT MATCHED THEN INSERT (
                CONTEXT_OBSERVATION_ID, RUN_ID, CONTEXT_ATTEMPT_ID, SYMBOL, TRADING_DATE, BAR_TS,
                SEQUENCE_NUM, OBJECTIVE_ATTEMPT_ID, PATTERN_ATTEMPT_ID, DOSSIER_ID,
                CONTEXT_RULESET_VERSION, THESIS_EFFECT, STATE_BEFORE, STATE_AFTER,
                SELECTED_ACTION, PAYLOAD_JSON, EXPLANATION
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, PARSE_JSON(%s), %s
            )
            """,
            (
                context_attempt_id,
                symbol.upper(),
                bar_ts_utc,
                sequence_num,
                trading_date,
                result.thesis_effect,
                result.state_before,
                result.state_after,
                result.selected_action,
                json.dumps(payload),
                result.explanation,
                str(uuid.uuid4()),
                run_id,
                context_attempt_id,
                symbol.upper(),
                trading_date,
                bar_ts_utc,
                sequence_num,
                objective_attempt_id,
                pattern_attempt_id,
                dossier_id,
                rs,
                result.thesis_effect,
                result.state_before,
                result.state_after,
                result.selected_action,
                json.dumps(payload),
                result.explanation,
            ),
        )
        if commit and own:
            conn.commit()
    finally:
        if own:
            conn.close()


def load_context_observations(
    run_id: str,
    *,
    context_attempt_id: str,
    symbol: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        clauses = ["RUN_ID = %s", "CONTEXT_ATTEMPT_ID = %s"]
        params: list[Any] = [run_id, context_attempt_id]
        if symbol:
            clauses.append("SYMBOL = %s")
            params.append(symbol.upper())
        params.append(limit)
        cur.execute(
            f"""
            SELECT CONTEXT_OBSERVATION_ID, RUN_ID, CONTEXT_ATTEMPT_ID, SYMBOL, TRADING_DATE, BAR_TS,
                   SEQUENCE_NUM, THESIS_EFFECT, STATE_BEFORE, STATE_AFTER, SELECTED_ACTION,
                   PAYLOAD_JSON, EXPLANATION
            FROM MIP.APP.BROOKS_INTRADAY_CONTEXT_OBSERVATION
            WHERE {' AND '.join(clauses)}
            ORDER BY BAR_TS, SYMBOL
            LIMIT %s
            """,
            tuple(params),
        )
        cols = [d[0].lower() for d in cur.description]
        out = []
        for raw in cur.fetchall():
            rec = dict(zip(cols, raw))
            pj = rec.get("payload_json")
            if isinstance(pj, str):
                rec["payload_json"] = json.loads(pj)
            out.append(rec)
        return out
    finally:
        conn.close()


def count_context_rows(run_id: str, *, context_attempt_id: str) -> int:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM MIP.APP.BROOKS_INTRADAY_CONTEXT_OBSERVATION
            WHERE RUN_ID = %s AND CONTEXT_ATTEMPT_ID = %s
            """,
            (run_id, context_attempt_id),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def context_sequence_hash(run_id: str, *, context_attempt_id: str) -> str:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT BAR_TS, SYMBOL, THESIS_EFFECT, STATE_AFTER, SELECTED_ACTION
            FROM MIP.APP.BROOKS_INTRADAY_CONTEXT_OBSERVATION
            WHERE RUN_ID = %s AND CONTEXT_ATTEMPT_ID = %s
            ORDER BY BAR_TS, SYMBOL
            """,
            (run_id, context_attempt_id),
        )
        parts = ["|".join(str(x) for x in row) for row in cur.fetchall()]
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()
    finally:
        conn.close()


def load_pattern_snapshot_index(
    run_id: str,
    *,
    pattern_attempt_id: str,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Index pattern snapshots by (symbol, bar_ts) from bar pattern links."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SYMBOL, BAR_TS, PATTERN_SNAPSHOT_JSON
            FROM MIP.APP.BROOKS_INTRADAY_BAR_PATTERN_LINK
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            ORDER BY BAR_TS, SYMBOL
            """,
            (run_id, pattern_attempt_id),
        )
        index: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for sym, ts, snap in cur.fetchall():
            key = (str(sym).upper(), str(ts)[:19])
            if isinstance(snap, str):
                snap = json.loads(snap)
            index[key] = list(snap or [])
        return index
    finally:
        conn.close()

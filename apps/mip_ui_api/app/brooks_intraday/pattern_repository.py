from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime
from typing import Any

from app.db import get_connection

from .pattern_engine import PatternRecord
from .pattern_ruleset_v01 import RULESET_VERSION

logger = logging.getLogger(__name__)


def upsert_pattern_instance(
    *,
    run_id: str,
    replay_attempt_id: str,
    symbol: str,
    pat: PatternRecord,
    pattern_ruleset_version: str | None = None,
    conn: Any | None = None,
    commit: bool = True,
) -> None:
    prsv = pattern_ruleset_version or RULESET_VERSION
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE INTO MIP.APP.BROOKS_INTRADAY_PATTERN_INSTANCE t
            USING (
                SELECT %s AS PATTERN_INSTANCE_ID
            ) s
            ON t.PATTERN_INSTANCE_ID = s.PATTERN_INSTANCE_ID
            WHEN MATCHED THEN UPDATE SET
                LATEST_TS = %s,
                CURRENT_TS = %s,
                LIFECYCLE_STATUS = %s,
                CONFIDENCE = %s,
                RELEVANT_LEVELS_JSON = PARSE_JSON(%s),
                RELEVANT_PRICES_JSON = PARSE_JSON(%s),
                CONFIRMATION_TS = %s,
                FAILURE_TS = %s,
                EXPIRY_TS = %s,
                SOURCE_RULE_ID = %s,
                CONFIRMATION_RULE_ID = %s,
                FAILURE_RULE_ID = %s,
                EXPIRY_RULE_ID = %s,
                LIFECYCLE_HISTORY_JSON = PARSE_JSON(%s),
                EXPLANATION = %s,
                ACTION = %s,
                TERM_ID = %s,
                PATTERN_FAMILY = %s
            WHEN NOT MATCHED THEN INSERT (
                PATTERN_INSTANCE_ID, RUN_ID, REPLAY_ATTEMPT_ID, SYMBOL, TRADING_DATE,
                TERM_ID, PATTERN_FAMILY, DIRECTION, START_TS, LATEST_TS, CURRENT_TS,
                LIFECYCLE_STATUS, CONFIDENCE, RELEVANT_LEVELS_JSON, RELEVANT_PRICES_JSON,
                CONFIRMATION_TS, FAILURE_TS, EXPIRY_TS, SOURCE_RULE_ID,
                SUPPORTING_OBSERVATION_IDS, SUPPORTING_RULE_IDS,
                CONFIRMATION_RULE_ID, FAILURE_RULE_ID, EXPIRY_RULE_ID,
                LIFECYCLE_HISTORY_JSON, PATTERN_RULESET_VERSION, PARENT_PATTERN_INSTANCE_ID,
                EXPLANATION, ACTION
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, PARSE_JSON(%s), PARSE_JSON(%s),
                %s, %s, %s, %s,
                PARSE_JSON(%s), PARSE_JSON(%s),
                %s, %s, %s,
                PARSE_JSON(%s), %s, %s,
                %s, %s
            )
            """,
            (
                pat.pattern_instance_id,
                pat.current_ts,
                pat.current_ts,
                pat.lifecycle,
                pat.confidence,
                json.dumps(pat.relevant_prices),
                json.dumps(pat.relevant_prices),
                pat.confirmation_ts,
                pat.failure_ts,
                pat.expiry_ts,
                pat.supporting_rule_ids[0] if pat.supporting_rule_ids else None,
                pat.confirmation_rule_id,
                pat.failure_rule_id,
                pat.expiry_rule_id,
                json.dumps(pat.lifecycle_history),
                pat.explanation,
                pat.action,
                pat.pattern_family,
                pat.pattern_family,
                pat.pattern_instance_id,
                run_id,
                replay_attempt_id,
                symbol.upper(),
                pat.trading_date,
                pat.pattern_family,
                pat.pattern_family,
                pat.direction,
                pat.start_ts,
                pat.current_ts,
                pat.current_ts,
                pat.lifecycle,
                pat.confidence,
                json.dumps({}),
                json.dumps(pat.relevant_prices),
                pat.confirmation_ts,
                pat.failure_ts,
                pat.expiry_ts,
                pat.supporting_rule_ids[0] if pat.supporting_rule_ids else None,
                json.dumps(pat.supporting_observation_ids),
                json.dumps(pat.supporting_rule_ids),
                pat.confirmation_rule_id,
                pat.failure_rule_id,
                pat.expiry_rule_id,
                json.dumps(pat.lifecycle_history),
                prsv,
                pat.parent_pattern_instance_id,
                pat.explanation,
                pat.action,
            ),
        )
        if commit and own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def insert_bar_pattern_link(
    *,
    run_id: str,
    replay_attempt_id: str,
    symbol: str,
    trading_date: date,
    bar_ts_utc: datetime,
    active_pattern_ids: list[str],
    pattern_snapshot: list[dict[str, Any]],
    action: str,
    explanation: str,
    conn: Any | None = None,
    commit: bool = True,
) -> None:
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE INTO MIP.APP.BROOKS_INTRADAY_BAR_PATTERN_LINK t
            USING (
                SELECT %s AS RUN_ID, %s AS REPLAY_ATTEMPT_ID, %s AS SYMBOL, %s AS BAR_TS
            ) s
            ON t.RUN_ID = s.RUN_ID AND t.REPLAY_ATTEMPT_ID = s.REPLAY_ATTEMPT_ID
               AND t.SYMBOL = s.SYMBOL AND t.BAR_TS = s.BAR_TS
            WHEN MATCHED THEN UPDATE SET
                ACTIVE_PATTERN_IDS = PARSE_JSON(%s),
                PATTERN_SNAPSHOT_JSON = PARSE_JSON(%s),
                ACTION = %s,
                EXPLANATION = %s,
                TRADING_DATE = %s
            WHEN NOT MATCHED THEN INSERT (
                RUN_ID, REPLAY_ATTEMPT_ID, SYMBOL, TRADING_DATE, BAR_TS,
                ACTIVE_PATTERN_IDS, PATTERN_SNAPSHOT_JSON, ACTION, EXPLANATION
            ) VALUES (
                %s, %s, %s, %s, %s,
                PARSE_JSON(%s), PARSE_JSON(%s), %s, %s
            )
            """,
            (
                run_id,
                replay_attempt_id,
                symbol.upper(),
                bar_ts_utc,
                json.dumps(active_pattern_ids),
                json.dumps(pattern_snapshot),
                action,
                explanation,
                trading_date,
                run_id,
                replay_attempt_id,
                symbol.upper(),
                trading_date,
                bar_ts_utc,
                json.dumps(active_pattern_ids),
                json.dumps(pattern_snapshot),
                action,
                explanation,
            ),
        )
        if commit and own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def delete_pattern_data_for_attempt(*, run_id: str, replay_attempt_id: str) -> None:
    """Remove partial pattern rows for an abandoned replay attempt (does not touch other attempts)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM MIP.APP.BROOKS_INTRADAY_BAR_PATTERN_LINK
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            """,
            (run_id, replay_attempt_id),
        )
        cur.execute(
            """
            DELETE FROM MIP.APP.BROOKS_INTRADAY_PATTERN_INSTANCE
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            """,
            (run_id, replay_attempt_id),
        )
        conn.commit()
    finally:
        conn.close()


def count_pattern_bar_links(run_id: str, *, replay_attempt_id: str) -> int:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM MIP.APP.BROOKS_INTRADAY_BAR_PATTERN_LINK
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            """,
            (run_id, replay_attempt_id),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def count_completed_pattern_slices(run_id: str, symbol_count: int, *, replay_attempt_id: str) -> int:
    if symbol_count <= 0:
        return 0
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(DISTINCT BAR_TS) FROM MIP.APP.BROOKS_INTRADAY_BAR_PATTERN_LINK
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            """,
            (run_id, replay_attempt_id),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def slice_pattern_link_count(run_id: str, bar_ts_utc: datetime, *, replay_attempt_id: str) -> int:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM MIP.APP.BROOKS_INTRADAY_BAR_PATTERN_LINK
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s AND BAR_TS = %s
            """,
            (run_id, replay_attempt_id, bar_ts_utc),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def load_patterns(
    run_id: str,
    *,
    replay_attempt_id: str,
    symbol: str | None = None,
    pattern_filter: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        clauses = ["RUN_ID = %s", "REPLAY_ATTEMPT_ID = %s"]
        params: list[Any] = [run_id, replay_attempt_id]
        if symbol:
            clauses.append("SYMBOL = %s")
            params.append(symbol.upper())
        if pattern_filter:
            clauses.append("PATTERN_FAMILY ILIKE %s")
            params.append(f"%{pattern_filter.upper()}%")
        params.append(limit)
        cur.execute(
            f"""
            SELECT PATTERN_INSTANCE_ID, RUN_ID, REPLAY_ATTEMPT_ID, SYMBOL, TRADING_DATE,
                   TERM_ID, PATTERN_FAMILY, DIRECTION, START_TS, LATEST_TS, CURRENT_TS,
                   LIFECYCLE_STATUS, CONFIDENCE, RELEVANT_PRICES_JSON, RELEVANT_LEVELS_JSON,
                   CONFIRMATION_TS, FAILURE_TS, EXPIRY_TS,
                   SUPPORTING_OBSERVATION_IDS, SUPPORTING_RULE_IDS,
                   CONFIRMATION_RULE_ID, FAILURE_RULE_ID, EXPIRY_RULE_ID,
                   LIFECYCLE_HISTORY_JSON, PATTERN_RULESET_VERSION, PARENT_PATTERN_INSTANCE_ID,
                   EXPLANATION, ACTION
            FROM MIP.APP.BROOKS_INTRADAY_PATTERN_INSTANCE
            WHERE {' AND '.join(clauses)}
            ORDER BY START_TS, SYMBOL
            LIMIT %s
            """,
            tuple(params),
        )
        cols = [d[0].lower() for d in cur.description]
        json_cols = {
            "relevant_prices_json",
            "relevant_levels_json",
            "supporting_observation_ids",
            "supporting_rule_ids",
            "lifecycle_history_json",
        }
        out = []
        for raw in cur.fetchall():
            rec = dict(zip(cols, raw))
            for k in json_cols:
                v = rec.get(k)
                if isinstance(v, str):
                    try:
                        rec[k] = json.loads(v)
                    except json.JSONDecodeError:
                        pass
            out.append(rec)
        return out
    finally:
        conn.close()


def load_bar_pattern_links(
    run_id: str,
    *,
    replay_attempt_id: str,
    symbol: str,
    limit: int = 500,
) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT BAR_TS, ACTIVE_PATTERN_IDS, PATTERN_SNAPSHOT_JSON, ACTION, EXPLANATION
            FROM MIP.APP.BROOKS_INTRADAY_BAR_PATTERN_LINK
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s AND SYMBOL = %s
            ORDER BY BAR_TS
            LIMIT %s
            """,
            (run_id, replay_attempt_id, symbol.upper(), limit),
        )
        cols = [d[0].lower() for d in cur.description]
        out = []
        for raw in cur.fetchall():
            rec = dict(zip(cols, raw))
            for k in ("active_pattern_ids", "pattern_snapshot_json"):
                v = rec.get(k)
                if isinstance(v, str):
                    try:
                        rec[k] = json.loads(v)
                    except json.JSONDecodeError:
                        pass
            out.append(rec)
        return out
    finally:
        conn.close()


def pattern_sequence_hash(run_id: str, *, replay_attempt_id: str) -> str:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TRADING_DATE, START_TS, SYMBOL, PATTERN_FAMILY, LIFECYCLE_STATUS,
                   RELEVANT_PRICES_JSON, LIFECYCLE_HISTORY_JSON
            FROM MIP.APP.BROOKS_INTRADAY_PATTERN_INSTANCE
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            ORDER BY START_TS, SYMBOL, PATTERN_INSTANCE_ID
            """,
            (run_id, replay_attempt_id),
        )
        parts = ["|".join(str(x) for x in row) for row in cur.fetchall()]
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()
    finally:
        conn.close()


def count_pattern_instances(run_id: str, *, replay_attempt_id: str) -> int:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM MIP.APP.BROOKS_INTRADAY_PATTERN_INSTANCE
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            """,
            (run_id, replay_attempt_id),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()

"""Persist and load Brooks INTRADAY Adviser V1.0 artifacts."""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from typing import Any

from app.db import get_connection

from .adviser_baseline_v01 import canonical_run_pin


def _uuid() -> str:
    return str(uuid.uuid4())


def create_adviser_attempt(
    *,
    run_id: str,
    symbol: str,
    trading_date: date,
    corpus_version: str,
    query_tag: str,
    config: dict[str, Any],
) -> str:
    aid = _uuid()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_INTRADAY_ADVISER_ATTEMPT (
                ADVISER_ATTEMPT_ID, RUN_ID, SYMBOL, TRADING_DATE, STATUS,
                CORPUS_VERSION, QUERY_TAG, CONFIG_JSON
            )
            SELECT %s, %s, %s, %s, 'RUNNING', %s, %s, PARSE_JSON(%s)
            """,
            (aid, run_id, symbol.upper(), trading_date, corpus_version, query_tag, json.dumps(config)),
        )
        conn.commit()
    finally:
        conn.close()
    return aid


def complete_adviser_attempt(aid: str, *, cost_summary: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_INTRADAY_ADVISER_ATTEMPT
            SET STATUS = 'COMPLETED', COMPLETED_AT = CURRENT_TIMESTAMP(),
                COST_SUMMARY_JSON = PARSE_JSON(%s)
            WHERE ADVISER_ATTEMPT_ID = %s
            """,
            (json.dumps(cost_summary), aid),
        )
        conn.commit()
    finally:
        conn.close()


def insert_adviser_call(row: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_INTRADAY_ADVISER_CALL (
                CALL_ID, ADVISER_ATTEMPT_ID, CALL_NUMBER, BAR_TS_NY, BAR_TS_ET,
                WAKE_REASON, POSITION_STATE, RETRIEVAL_QUERY, RAG_CARD_IDS,
                RETRIEVED_CONCEPTS, CURRENT_THESIS, BROOKS_MARKET_STATE, ACTION,
                WATCH_CONDITIONS, CONFIRMATION_CONDITIONS, INVALIDATION_CONDITIONS,
                WATCH_PREDICATES, INVALIDATION_PREDICATES, CONFIRMATION_PREDICATES,
                DAILY_INTRADAY_CONTEXT,
                BROOKS_REASONING_SUMMARY, OBSERVATION_PACKET, LLM_MODEL,
                LLM_INPUT_TOKENS, LLM_OUTPUT_TOKENS, RAG_LATENCY_MS, LLM_LATENCY_MS,
                TOTAL_LATENCY_MS
            ) 
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s),
                %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
                PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
                %s, PARSE_JSON(%s), %s, %s, %s, %s, %s, %s
            """,
            (
                row["call_id"],
                row["adviser_attempt_id"],
                row["call_number"],
                row["bar_ts_ny"],
                row.get("bar_ts_et"),
                row.get("wake_reason"),
                row.get("position_state"),
                row.get("retrieval_query"),
                json.dumps(row.get("rag_card_ids") or []),
                json.dumps(row.get("retrieved_concepts") or []),
                row.get("current_thesis"),
                row.get("brooks_market_state"),
                row.get("action"),
                json.dumps(row.get("watch_conditions") or []),
                json.dumps(row.get("confirmation_conditions") or []),
                json.dumps(row.get("invalidation_conditions") or []),
                json.dumps(row.get("watch_predicates") or []),
                json.dumps(row.get("invalidation_predicates") or []),
                json.dumps(row.get("confirmation_predicates") or []),
                json.dumps(row.get("daily_intraday_context") or {}),
                row.get("brooks_reasoning_summary"),
                json.dumps(row.get("observation_packet") or {}),
                row.get("llm_model"),
                row.get("llm_input_tokens"),
                row.get("llm_output_tokens"),
                row.get("rag_latency_ms"),
                row.get("llm_latency_ms"),
                row.get("total_latency_ms"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def archive_adviser_attempts(
    adviser_attempt_ids: list[str],
    *,
    archive_reason: str = "validation_round_archived",
    archive_round: int = 1,
) -> int:
    """Mark attempts ARCHIVED (rows retained; hidden from V1 UI + re-run guard)."""
    ids = [str(x).strip() for x in adviser_attempt_ids if str(x).strip()]
    if not ids:
        return 0
    conn = get_connection()
    try:
        cur = conn.cursor()
        updated = 0
        for aid in ids:
            cur.execute(
                """
                UPDATE MIP.APP.BROOKS_INTRADAY_ADVISER_ATTEMPT
                SET STATUS = 'ARCHIVED',
                    CONFIG_JSON = OBJECT_INSERT(
                        COALESCE(CONFIG_JSON, OBJECT_CONSTRUCT()),
                        'validation_archive',
                        OBJECT_CONSTRUCT(
                            'round', %s,
                            'reason', %s,
                            'archived_at', TO_VARCHAR(CURRENT_TIMESTAMP())
                        ),
                        TRUE
                    )
                WHERE ADVISER_ATTEMPT_ID = %s
                  AND STATUS IN ('COMPLETED', 'RUNNING', 'FAILED')
                """,
                (archive_round, archive_reason, aid),
            )
            updated += int(cur.rowcount or 0)
        conn.commit()
        return updated
    finally:
        conn.close()


def upsert_adviser_bar(row: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE INTO MIP.APP.BROOKS_INTRADAY_ADVISER_BAR t
            USING (SELECT %s AID, %s BAR_TS) s
            ON t.ADVISER_ATTEMPT_ID = s.AID AND t.BAR_TS_NY = s.BAR_TS
            WHEN MATCHED THEN UPDATE SET
                BAR_TS_ET = %s, ADVISER_CALLED = %s, CALL_ID = %s,
                THESIS_SNAPSHOT = %s, ACTION_SNAPSHOT = %s, WATCH_SNAPSHOT = %s,
                BAR_NOTE = %s, SIM_POSITION_QTY = %s, SIM_POSITION_AVG = %s
            WHEN NOT MATCHED THEN INSERT (
                ADVISER_ATTEMPT_ID, BAR_TS_NY, BAR_TS_ET, ADVISER_CALLED, CALL_ID,
                THESIS_SNAPSHOT, ACTION_SNAPSHOT, WATCH_SNAPSHOT, BAR_NOTE,
                SIM_POSITION_QTY, SIM_POSITION_AVG
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["adviser_attempt_id"],
                row["bar_ts_ny"],
                row.get("bar_ts_et"),
                row.get("adviser_called", False),
                row.get("call_id"),
                row.get("thesis_snapshot"),
                row.get("action_snapshot"),
                row.get("watch_snapshot"),
                row.get("bar_note"),
                row.get("sim_position_qty", 0),
                row.get("sim_position_avg"),
                row["adviser_attempt_id"],
                row["bar_ts_ny"],
                row.get("bar_ts_et"),
                row.get("adviser_called", False),
                row.get("call_id"),
                row.get("thesis_snapshot"),
                row.get("action_snapshot"),
                row.get("watch_snapshot"),
                row.get("bar_note"),
                row.get("sim_position_qty", 0),
                row.get("sim_position_avg"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def load_adviser_bars(adviser_attempt_id: str) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT BAR_TS_NY, BAR_TS_ET, ADVISER_CALLED, CALL_ID, THESIS_SNAPSHOT,
                   ACTION_SNAPSHOT, WATCH_SNAPSHOT, BAR_NOTE, SIM_POSITION_QTY, SIM_POSITION_AVG
            FROM MIP.APP.BROOKS_INTRADAY_ADVISER_BAR
            WHERE ADVISER_ATTEMPT_ID = %s
            ORDER BY BAR_TS_NY
            """,
            (adviser_attempt_id,),
        )
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


def load_adviser_calls(adviser_attempt_id: str) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT CALL_NUMBER, BAR_TS_ET, WAKE_REASON, RETRIEVAL_QUERY, RAG_CARD_IDS,
                   RETRIEVED_CONCEPTS, CURRENT_THESIS, BROOKS_MARKET_STATE, ACTION,
                   WATCH_CONDITIONS, CONFIRMATION_CONDITIONS, INVALIDATION_CONDITIONS,
                   BROOKS_REASONING_SUMMARY, TOTAL_LATENCY_MS
            FROM MIP.APP.BROOKS_INTRADAY_ADVISER_CALL
            WHERE ADVISER_ATTEMPT_ID = %s
            ORDER BY CALL_NUMBER
            """,
            (adviser_attempt_id,),
        )
        cols = [d[0].lower() for d in cur.description]
        rows = []
        for raw in cur.fetchall():
            rec = dict(zip(cols, raw))
            for k in ("rag_card_ids", "retrieved_concepts", "watch_conditions", "confirmation_conditions", "invalidation_conditions"):
                v = rec.get(k)
                if isinstance(v, str):
                    rec[k] = json.loads(v)
            rows.append(rec)
        return rows
    finally:
        conn.close()


def insert_sim_trade(row: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_INTRADAY_ADVISER_SIM_TRADE (
                TRADE_ID, ADVISER_ATTEMPT_ID, SYMBOL, ENTRY_TS_NY, EXIT_TS_NY,
                ENTRY_PRICE, EXIT_PRICE, QUANTITY, STOP_PRICE, EXIT_REASON, REALIZED_PNL
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                row["trade_id"],
                row["adviser_attempt_id"],
                row["symbol"],
                row["entry_ts_ny"],
                row.get("exit_ts_ny"),
                row.get("entry_price"),
                row.get("exit_price"),
                row.get("quantity"),
                row.get("stop_price"),
                row.get("exit_reason"),
                row.get("realized_pnl"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def pin_run_adviser_attempt(run_id: str, adviser_attempt_id: str, simulation_attempt_id: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT CONFIG_JSON FROM MIP.APP.BROOKS_INTRADAY_RUN WHERE RUN_ID = %s",
            (run_id,),
        )
        row = cur.fetchone()
        cfg = {}
        if row and row[0]:
            cfg = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        cfg["adviser_foundation"] = canonical_run_pin(
            adviser_attempt_id=adviser_attempt_id,
            simulation_attempt_id=simulation_attempt_id,
        )
        cur.execute(
            "UPDATE MIP.APP.BROOKS_INTRADAY_RUN SET CONFIG_JSON = (SELECT PARSE_JSON(%s)) WHERE RUN_ID = %s",
            (json.dumps(cfg), run_id),
        )
        conn.commit()
    finally:
        conn.close()

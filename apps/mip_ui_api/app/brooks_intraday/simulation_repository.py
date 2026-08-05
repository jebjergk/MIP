"""Persist Phase 7 simulation attempts, trades, and blocked signals."""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from typing import Any

from app.db import get_connection

from .simulation_ruleset_v01 import RULESET_VERSION

logger = logging.getLogger(__name__)


def create_simulation_attempt(
    *,
    run_id: str,
    context_attempt_id: str,
    context_ruleset_version: str,
    starting_cash: float,
    notes: str | None = None,
) -> str:
    attempt_id = str(uuid.uuid4())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_INTRADAY_SIMULATION_ATTEMPT (
                SIMULATION_ATTEMPT_ID, RUN_ID, CONTEXT_ATTEMPT_ID, CONTEXT_RULESET_VERSION,
                SIMULATION_RULESET_VERSION, STATUS, STARTING_CASH, NOTES, CREATED_AT_UTC
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()
            """,
            (
                attempt_id,
                run_id,
                context_attempt_id,
                context_ruleset_version,
                RULESET_VERSION,
                "IN_PROGRESS",
                starting_cash,
                notes,
            ),
        )
        conn.commit()
        return attempt_id
    finally:
        conn.close()


def complete_simulation_attempt(
    *,
    simulation_attempt_id: str,
    trade_count: int,
    blocked_count: int,
    ending_cash: float,
    realized_pnl: float,
    sequence_hash: str | None,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_INTRADAY_SIMULATION_ATTEMPT
            SET STATUS = %s, TRADE_COUNT = %s, BLOCKED_SIGNAL_COUNT = %s,
                ENDING_CASH = %s, REALIZED_PNL = %s, SEQUENCE_HASH = %s,
                COMPLETED_AT_UTC = CURRENT_TIMESTAMP()
            WHERE SIMULATION_ATTEMPT_ID = %s
            """,
            ("COMPLETED", trade_count, blocked_count, ending_cash, realized_pnl, sequence_hash, simulation_attempt_id),
        )
        conn.commit()
    finally:
        conn.close()


def clear_run_simulation_artifacts(run_id: str) -> None:
    """Deprecated. Phase C forbids run-wide DELETE; raises to prevent accidental use."""
    raise RuntimeError(
        "clear_run_simulation_artifacts is disabled: use SIMULATION_ATTEMPT_ID isolation "
        f"(refusing run-wide DELETE for run_id={run_id})"
    )


def insert_sim_trade(
    *,
    run_id: str,
    trade: dict[str, Any],
    simulation_attempt_id: str | None = None,
    conn: Any | None = None,
    commit: bool = True,
) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    attempt_id = simulation_attempt_id or trade.get("simulation_attempt_id")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_INTRADAY_SIM_TRADE (
                TRADE_ID, RUN_ID, SIMULATION_ATTEMPT_ID, SYMBOL, DIRECTION, QUANTITY,
                SIGNAL_TS, ENTRY_TS, ENTRY_PRICE,
                EXIT_DECISION_TS, EXIT_TS, EXIT_PRICE, EXIT_REASON,
                INITIAL_CASH, REMAINING_CASH, REALIZED_PNL, RULE_VERSION
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                trade["trade_id"],
                run_id,
                attempt_id,
                trade["symbol"],
                trade.get("direction", "LONG"),
                trade["quantity"],
                trade.get("signal_ts"),
                trade.get("entry_ts"),
                trade.get("entry_price"),
                trade.get("exit_decision_ts"),
                trade.get("exit_ts"),
                trade.get("exit_price"),
                trade.get("exit_reason"),
                trade.get("initial_cash"),
                trade.get("remaining_cash"),
                trade.get("realized_pnl"),
                RULESET_VERSION,
            ),
        )
        if commit and own:
            conn.commit()
    finally:
        if own:
            conn.close()


def insert_blocked_signal(
    *,
    run_id: str,
    signal: dict[str, Any],
    active_position_symbol: str | None,
    simulation_attempt_id: str | None = None,
    conn: Any | None = None,
    commit: bool = True,
) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    attempt_id = simulation_attempt_id or signal.get("simulation_attempt_id")
    if not attempt_id:
        raise ValueError("simulation_attempt_id is required for blocked signal inserts")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_INTRADAY_BLOCKED_SIGNAL (
                RUN_ID, SIMULATION_ATTEMPT_ID, SYMBOL, SIGNAL_TS, CANDIDATE_ACTION, BLOCK_REASON,
                ACTIVE_POSITION_SYMBOL, TIE_BREAK_JSON
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s, PARSE_JSON(%s))
            """,
            (
                run_id,
                attempt_id,
                signal["symbol"],
                signal["signal_ts"],
                signal["candidate_action"],
                signal["block_reason"],
                active_position_symbol,
                json.dumps(signal.get("tie_break_json") or {}),
            ),
        )
        if commit and own:
            conn.commit()
    finally:
        if own:
            conn.close()


def insert_sim_trades_batch(
    *,
    run_id: str,
    simulation_attempt_id: str,
    trades: list[dict[str, Any]],
    conn: Any | None = None,
    commit: bool = True,
    chunk_size: int = 100,
) -> int:
    if not trades:
        return 0
    own = conn is None
    if own:
        conn = get_connection()
    written = 0
    try:
        for i in range(0, len(trades), chunk_size):
            for trade in trades[i : i + chunk_size]:
                insert_sim_trade(
                    run_id=run_id,
                    trade=trade,
                    simulation_attempt_id=simulation_attempt_id,
                    conn=conn,
                    commit=False,
                )
            written += len(trades[i : i + chunk_size])
            if commit:
                conn.commit()
        return written
    finally:
        if own:
            conn.close()


def insert_blocked_signals_batch(
    *,
    run_id: str,
    simulation_attempt_id: str,
    signals: list[dict[str, Any]],
    conn: Any | None = None,
    commit: bool = True,
    chunk_size: int = 200,
) -> int:
    if not signals:
        return 0
    own = conn is None
    if own:
        conn = get_connection()
    written = 0
    try:
        for i in range(0, len(signals), chunk_size):
            for sig in signals[i : i + chunk_size]:
                insert_blocked_signal(
                    run_id=run_id,
                    signal=sig,
                    active_position_symbol=sig.get("active_position_symbol"),
                    simulation_attempt_id=simulation_attempt_id,
                    conn=conn,
                    commit=False,
                )
            written += len(signals[i : i + chunk_size])
            if commit:
                conn.commit()
        return written
    finally:
        if own:
            conn.close()


def load_sim_trades(
    run_id: str,
    *,
    simulation_attempt_id: str | None = None,
    allow_legacy_fallback: bool = True,
) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if simulation_attempt_id:
            cur.execute(
                """
                SELECT TRADE_ID, RUN_ID, SIMULATION_ATTEMPT_ID, SYMBOL, DIRECTION, QUANTITY,
                       SIGNAL_TS, ENTRY_TS, ENTRY_PRICE,
                       EXIT_DECISION_TS, EXIT_TS, EXIT_PRICE, EXIT_REASON,
                       INITIAL_CASH, REMAINING_CASH, REALIZED_PNL, RULE_VERSION
                FROM MIP.APP.BROOKS_INTRADAY_SIM_TRADE
                WHERE RUN_ID = %s AND SIMULATION_ATTEMPT_ID = %s
                ORDER BY COALESCE(ENTRY_TS, SIGNAL_TS)
                """,
                (run_id, simulation_attempt_id),
            )
            cols = [d[0].lower() for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            if rows or not allow_legacy_fallback:
                return rows
            cur.execute(
                """
                SELECT TRADE_ID, RUN_ID, SIMULATION_ATTEMPT_ID, SYMBOL, DIRECTION, QUANTITY,
                       SIGNAL_TS, ENTRY_TS, ENTRY_PRICE,
                       EXIT_DECISION_TS, EXIT_TS, EXIT_PRICE, EXIT_REASON,
                       INITIAL_CASH, REMAINING_CASH, REALIZED_PNL, RULE_VERSION
                FROM MIP.APP.BROOKS_INTRADAY_SIM_TRADE
                WHERE RUN_ID = %s AND SIMULATION_ATTEMPT_ID IS NULL
                ORDER BY COALESCE(ENTRY_TS, SIGNAL_TS)
                """,
                (run_id,),
            )
            cols = [d[0].lower() for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT TRADE_ID, RUN_ID, SIMULATION_ATTEMPT_ID, SYMBOL, DIRECTION, QUANTITY,
                   SIGNAL_TS, ENTRY_TS, ENTRY_PRICE,
                   EXIT_DECISION_TS, EXIT_TS, EXIT_PRICE, EXIT_REASON,
                   INITIAL_CASH, REMAINING_CASH, REALIZED_PNL, RULE_VERSION
            FROM MIP.APP.BROOKS_INTRADAY_SIM_TRADE
            WHERE RUN_ID = %s AND SIMULATION_ATTEMPT_ID IS NULL
            ORDER BY COALESCE(ENTRY_TS, SIGNAL_TS)
            """,
            (run_id,),
        )
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def load_simulation_attempt(simulation_attempt_id: str | None) -> dict[str, Any] | None:
    if not simulation_attempt_id:
        return None
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SIMULATION_ATTEMPT_ID, RUN_ID, CONTEXT_ATTEMPT_ID, CONTEXT_RULESET_VERSION,
                   SIMULATION_RULESET_VERSION, STATUS, STARTING_CASH, ENDING_CASH, REALIZED_PNL,
                   TRADE_COUNT, BLOCKED_SIGNAL_COUNT, SEQUENCE_HASH
            FROM MIP.APP.BROOKS_INTRADAY_SIMULATION_ATTEMPT
            WHERE SIMULATION_ATTEMPT_ID = %s
            """,
            (simulation_attempt_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0].lower() for d in cur.description]
        return dict(zip(cols, row))
    finally:
        conn.close()


def load_blocked_signals(
    run_id: str,
    *,
    simulation_attempt_id: str | None = None,
    allow_legacy_fallback: bool = True,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    from .persist_integrity import LEGACY_UNSCOPED_SIM_ATTEMPT

    conn = get_connection()
    try:
        cur = conn.cursor()

        def _parse(rows_raw: list) -> list[dict[str, Any]]:
            cols = [d[0].lower() for d in cur.description]
            out = []
            for raw in rows_raw:
                rec = dict(zip(cols, raw))
                tj = rec.get("tie_break_json")
                if isinstance(tj, str):
                    rec["tie_break_json"] = json.loads(tj)
                out.append(rec)
            return out

        if simulation_attempt_id:
            cur.execute(
                """
                SELECT RUN_ID, SIMULATION_ATTEMPT_ID, SYMBOL, SIGNAL_TS, CANDIDATE_ACTION, BLOCK_REASON,
                       ACTIVE_POSITION_SYMBOL, TIE_BREAK_JSON
                FROM MIP.APP.BROOKS_INTRADAY_BLOCKED_SIGNAL
                WHERE RUN_ID = %s AND SIMULATION_ATTEMPT_ID = %s
                ORDER BY SIGNAL_TS, SYMBOL
                LIMIT %s
                """,
                (run_id, simulation_attempt_id, limit),
            )
            rows = _parse(cur.fetchall())
            if rows or not allow_legacy_fallback:
                return rows
            cur.execute(
                """
                SELECT RUN_ID, SIMULATION_ATTEMPT_ID, SYMBOL, SIGNAL_TS, CANDIDATE_ACTION, BLOCK_REASON,
                       ACTIVE_POSITION_SYMBOL, TIE_BREAK_JSON
                FROM MIP.APP.BROOKS_INTRADAY_BLOCKED_SIGNAL
                WHERE RUN_ID = %s AND SIMULATION_ATTEMPT_ID = %s
                ORDER BY SIGNAL_TS, SYMBOL
                LIMIT %s
                """,
                (run_id, LEGACY_UNSCOPED_SIM_ATTEMPT, limit),
            )
            return _parse(cur.fetchall())
        cur.execute(
            """
            SELECT RUN_ID, SIMULATION_ATTEMPT_ID, SYMBOL, SIGNAL_TS, CANDIDATE_ACTION, BLOCK_REASON,
                   ACTIVE_POSITION_SYMBOL, TIE_BREAK_JSON
            FROM MIP.APP.BROOKS_INTRADAY_BLOCKED_SIGNAL
            WHERE RUN_ID = %s AND SIMULATION_ATTEMPT_ID = %s
            ORDER BY SIGNAL_TS, SYMBOL
            LIMIT %s
            """,
            (run_id, LEGACY_UNSCOPED_SIM_ATTEMPT, limit),
        )
        return _parse(cur.fetchall())
    finally:
        conn.close()


def verify_context_attempt(*, run_id: str, context_attempt_id: str, required_ruleset: str) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT CONTEXT_ATTEMPT_ID, RUN_ID, CONTEXT_RULESET_VERSION, STATUS, ROW_COUNT
            FROM MIP.APP.BROOKS_INTRADAY_CONTEXT_ATTEMPT
            WHERE CONTEXT_ATTEMPT_ID = %s
            """,
            (context_attempt_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Context attempt not found: {context_attempt_id}")
        cols = [d[0].lower() for d in cur.description]
        rec = dict(zip(cols, row))
        if rec["run_id"] != run_id:
            raise ValueError("Context attempt run_id mismatch")
        if rec["context_ruleset_version"] != required_ruleset:
            raise ValueError(
                f"Refusing simulation gating on {rec['context_ruleset_version']}; required {required_ruleset}"
            )
        if rec["status"] != "COMPLETED":
            raise ValueError(f"Context attempt not COMPLETED: {rec['status']}")
        return rec
    finally:
        conn.close()


def simulation_sequence_hash(trades: list[dict[str, Any]], blocked: list[dict[str, Any]]) -> str:
    parts = []
    for t in trades:
        parts.append(
            "|".join(
                str(t.get(k))
                for k in ("trade_id", "symbol", "entry_ts", "exit_ts", "realized_pnl", "exit_reason")
            )
        )
    for b in blocked:
        parts.append("|".join(str(b.get(k)) for k in ("symbol", "signal_ts", "block_reason", "candidate_action")))
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()

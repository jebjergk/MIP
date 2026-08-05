from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime
from typing import Any

from app.db import get_connection

from .objective_ruleset_v01 import legacy_attempt_id

logger = logging.getLogger(__name__)


def slice_observation_count(run_id: str, bar_ts_utc: datetime, *, replay_attempt_id: str) -> int:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM MIP.APP.BROOKS_INTRADAY_BAR_OBSERVATION
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s AND BAR_TS = %s
            """,
            (run_id, replay_attempt_id, bar_ts_utc),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def count_observations(run_id: str, *, replay_attempt_id: str | None = None) -> int:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if replay_attempt_id:
            cur.execute(
                """
                SELECT COUNT(*) FROM MIP.APP.BROOKS_INTRADAY_BAR_OBSERVATION
                WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
                """,
                (run_id, replay_attempt_id),
            )
        else:
            cur.execute(
                "SELECT COUNT(*) FROM MIP.APP.BROOKS_INTRADAY_BAR_OBSERVATION WHERE RUN_ID = %s",
                (run_id,),
            )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def count_completed_steps_from_observations(
    run_id: str,
    symbol_count: int,
    *,
    replay_attempt_id: str,
) -> int:
    if symbol_count <= 0:
        return 0
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(DISTINCT BAR_TS) AS SLICES
            FROM MIP.APP.BROOKS_INTRADAY_BAR_OBSERVATION
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            """,
            (run_id, replay_attempt_id),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def load_baseline_observation_index(
    run_id: str,
    replay_attempt_id: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Load all Phase 4 baseline observations for in-memory pattern replay."""
    rows = load_observations(run_id, replay_attempt_id=replay_attempt_id, limit=10000)
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for rec in rows:
        sym = str(rec.get("symbol", "")).upper()
        ts = rec.get("bar_ts")
        key = (sym, str(ts)[:19])
        index[key] = rec
    return index


def load_observation_at_bar(
    run_id: str,
    *,
    replay_attempt_id: str,
    symbol: str,
    bar_ts_utc: datetime,
) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT RUN_ID, REPLAY_ATTEMPT_ID, SYMBOL, TRADING_DATE, BAR_TS, SEQUENCE_NUM,
                   OHLCV_JSON, OBJECTIVE_FACTS_JSON, DERIVED_METRICS_JSON, BROOKS_OBS_JSON,
                   RULE_EVALUATIONS_JSON, RULESET_VERSION, VISIBLE_HISTORY_COUNT,
                   PATTERN_STATE_JSON, CONTEXT_JSON, STATE_BEFORE, STATE_AFTER,
                   ACTION, BLOCKERS_JSON, EXPLANATION, RULE_IDS, DATA_QUALITY_STATUS
            FROM MIP.APP.BROOKS_INTRADAY_BAR_OBSERVATION
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s AND SYMBOL = %s AND BAR_TS = %s
            LIMIT 1
            """,
            (run_id, replay_attempt_id, symbol.upper(), bar_ts_utc),
        )
        rows = _parse_rows(cur)
        return rows[0] if rows else None
    finally:
        conn.close()


def load_observations(
    run_id: str,
    symbol: str | None = None,
    *,
    replay_attempt_id: str | None = None,
    limit: int = 500,
    review_filter: str | None = None,
) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cols_sql = """
            RUN_ID, REPLAY_ATTEMPT_ID, SYMBOL, TRADING_DATE, BAR_TS, SEQUENCE_NUM,
            OHLCV_JSON, OBJECTIVE_FACTS_JSON, DERIVED_METRICS_JSON, BROOKS_OBS_JSON,
            RULE_EVALUATIONS_JSON, RULESET_VERSION, VISIBLE_HISTORY_COUNT,
            PATTERN_STATE_JSON, CONTEXT_JSON, STATE_BEFORE, STATE_AFTER,
            ACTION, BLOCKERS_JSON, EXPLANATION, RULE_IDS, DATA_QUALITY_STATUS
        """
        params: list[Any] = [run_id]
        clauses = ["RUN_ID = %s"]
        if replay_attempt_id:
            clauses.append("REPLAY_ATTEMPT_ID = %s")
            params.append(replay_attempt_id)
        if symbol:
            clauses.append("SYMBOL = %s")
            params.append(symbol.upper())
        params.append(limit)
        cur.execute(
            f"""
            SELECT {cols_sql}
            FROM MIP.APP.BROOKS_INTRADAY_BAR_OBSERVATION
            WHERE {' AND '.join(clauses)}
            ORDER BY BAR_TS, SYMBOL
            LIMIT %s
            """,
            tuple(params),
        )
        rows = _parse_rows(cur)
        if review_filter:
            rows = [r for r in rows if _matches_review_filter(r, review_filter)]
        return rows
    finally:
        conn.close()


def _parse_rows(cur) -> list[dict[str, Any]]:
    cols = [d[0].lower() for d in cur.description]
    json_cols = (
        "ohlcv_json",
        "objective_facts_json",
        "derived_metrics_json",
        "brooks_obs_json",
        "rule_evaluations_json",
        "pattern_state_json",
        "context_json",
        "blockers_json",
        "rule_ids",
    )
    out = []
    for raw in cur.fetchall():
        rec = dict(zip(cols, raw))
        for key in json_cols:
            val = rec.get(key)
            if isinstance(val, str):
                try:
                    rec[key] = json.loads(val)
                except json.JSONDecodeError:
                    pass
        out.append(rec)
    return out


def _matches_review_filter(rec: dict[str, Any], flt: str) -> bool:
    terms = rec.get("brooks_obs_json") or []
    term_names = {t.get("term") for t in terms if isinstance(t, dict)}
    key = flt.lower()
    if key == "inside_bar":
        return "INSIDE_BAR" in term_names
    if key == "outside_bar":
        return "OUTSIDE_BAR" in term_names
    if key == "doji":
        return "DOJI" in term_names
    if key == "large_bar":
        return bool(term_names & {"BIG_BULL_BAR", "BIG_BEAR_BAR"})
    if key == "possible_breakout":
        return "POSSIBLE_BREAKOUT_BAR" in term_names
    if key == "possible_follow_through":
        return "POSSIBLE_FOLLOW_THROUGH_BAR" in term_names
    if key == "hh_hl":
        return "HIGHER_HIGH" in term_names and "HIGHER_LOW" in term_names
    if key == "lh_ll":
        return "LOWER_HIGH" in term_names and "LOWER_LOW" in term_names
    return True


def review_summary(run_id: str, *, replay_attempt_id: str) -> dict[str, Any]:
    rows = load_observations(run_id, replay_attempt_id=replay_attempt_id, limit=2000)
    counts: dict[str, int] = {}
    by_symbol: dict[str, dict[str, int]] = {}
    for rec in rows:
        sym = rec.get("symbol", "?")
        by_symbol.setdefault(sym, {})
        for t in rec.get("brooks_obs_json") or []:
            if not isinstance(t, dict):
                continue
            name = t.get("term", "?")
            counts[name] = counts.get(name, 0) + 1
            by_symbol[sym][name] = by_symbol[sym].get(name, 0) + 1
    return {"run_id": run_id, "replay_attempt_id": replay_attempt_id, "term_counts": counts, "by_symbol": by_symbol}


def insert_objective_observation(
    *,
    run_id: str,
    replay_attempt_id: str,
    symbol: str,
    trading_date: date,
    bar_ts_utc: datetime,
    sequence_num: int,
    ohlcv: dict[str, Any],
    payload: dict[str, Any],
    conn: Any | None = None,
    commit: bool = True,
) -> bool:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE INTO MIP.APP.BROOKS_INTRADAY_BAR_OBSERVATION t
            USING (
                SELECT %s AS RUN_ID, %s AS REPLAY_ATTEMPT_ID, %s AS SYMBOL,
                       %s AS TRADING_DATE, %s AS BAR_TS, %s AS SEQUENCE_NUM
            ) s
            ON t.RUN_ID = s.RUN_ID AND t.REPLAY_ATTEMPT_ID = s.REPLAY_ATTEMPT_ID
               AND t.SYMBOL = s.SYMBOL AND t.BAR_TS = s.BAR_TS
            WHEN MATCHED THEN UPDATE SET
                OHLCV_JSON = PARSE_JSON(%s),
                OBJECTIVE_FACTS_JSON = PARSE_JSON(%s),
                DERIVED_METRICS_JSON = PARSE_JSON(%s),
                BROOKS_OBS_JSON = PARSE_JSON(%s),
                RULE_EVALUATIONS_JSON = PARSE_JSON(%s),
                RULESET_VERSION = %s,
                VISIBLE_HISTORY_COUNT = %s,
                PATTERN_STATE_JSON = PARSE_JSON(%s),
                CONTEXT_JSON = PARSE_JSON(%s),
                STATE_BEFORE = %s, STATE_AFTER = %s, ACTION = %s,
                BLOCKERS_JSON = PARSE_JSON(%s), EXPLANATION = %s,
                RULE_IDS = PARSE_JSON(%s), DATA_QUALITY_STATUS = %s
            WHEN NOT MATCHED THEN INSERT (
                RUN_ID, REPLAY_ATTEMPT_ID, SYMBOL, TRADING_DATE, BAR_TS, SEQUENCE_NUM,
                OHLCV_JSON, OBJECTIVE_FACTS_JSON, DERIVED_METRICS_JSON, BROOKS_OBS_JSON,
                RULE_EVALUATIONS_JSON, RULESET_VERSION, VISIBLE_HISTORY_COUNT,
                PATTERN_STATE_JSON, CONTEXT_JSON, STATE_BEFORE, STATE_AFTER,
                ACTION, BLOCKERS_JSON, EXPLANATION, RULE_IDS, DATA_QUALITY_STATUS
            ) VALUES (
                s.RUN_ID, s.REPLAY_ATTEMPT_ID, s.SYMBOL, s.TRADING_DATE, s.BAR_TS, s.SEQUENCE_NUM,
                PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
                PARSE_JSON(%s), %s, %s, PARSE_JSON(%s), PARSE_JSON(%s),
                %s, %s, %s, PARSE_JSON(%s), %s, PARSE_JSON(%s), %s
            )
            """,
            _merge_params(
                run_id,
                replay_attempt_id,
                symbol,
                trading_date,
                bar_ts_utc,
                sequence_num,
                ohlcv,
                payload,
            ),
        )
        if commit and own:
            conn.commit()
        return True
    finally:
        if own:
            conn.close()


def _merge_params(
    run_id: str,
    replay_attempt_id: str,
    symbol: str,
    trading_date: date,
    bar_ts_utc: datetime,
    sequence_num: int,
    ohlcv: dict[str, Any],
    payload: dict[str, Any],
) -> tuple:
    vals = (
        json.dumps(ohlcv),
        json.dumps(payload["objective_facts_json"]),
        json.dumps(payload["derived_metrics_json"]),
        json.dumps(payload["brooks_obs_json"]),
        json.dumps(payload["rule_evaluations_json"]),
        payload["ruleset_version"],
        payload["visible_history_count"],
        json.dumps(payload["pattern_state_json"]),
        json.dumps(payload["context_json"]),
        payload["state_before"],
        payload["state_after"],
        payload["action"],
        json.dumps(payload["blockers_json"]),
        payload["explanation"],
        json.dumps(payload["rule_ids"]),
        payload["data_quality_status"],
    )
    head = (run_id, replay_attempt_id, symbol.upper(), trading_date, bar_ts_utc, sequence_num)
    return head + vals + vals


def insert_objective_observations_batch(
    rows: list[dict[str, Any]],
    *,
    conn: Any | None = None,
    commit: bool = True,
    chunk_size: int = 500,
) -> int:
    """Bulk INSERT (no MERGE). Used when BROOKS_PERSIST_MODE=bulk."""
    from .persist_batch import execute_insert_select_from_values

    if not rows:
        return 0
    own = conn is None
    if own:
        conn = get_connection()
    written = 0
    table = """MIP.APP.BROOKS_INTRADAY_BAR_OBSERVATION (
            RUN_ID, REPLAY_ATTEMPT_ID, SYMBOL, TRADING_DATE, BAR_TS, SEQUENCE_NUM,
            OHLCV_JSON, OBJECTIVE_FACTS_JSON, DERIVED_METRICS_JSON, BROOKS_OBS_JSON,
            RULE_EVALUATIONS_JSON, RULESET_VERSION, VISIBLE_HISTORY_COUNT,
            PATTERN_STATE_JSON, CONTEXT_JSON, STATE_BEFORE, STATE_AFTER,
            ACTION, BLOCKERS_JSON, EXPLANATION, RULE_IDS, DATA_QUALITY_STATUS
        )"""
    select_list = """
            column1, column2, column3, column4, column5, column6,
            PARSE_JSON(column7), PARSE_JSON(column8), PARSE_JSON(column9), PARSE_JSON(column10),
            PARSE_JSON(column11), column12, column13,
            PARSE_JSON(column14), PARSE_JSON(column15), column16, column17,
            column18, PARSE_JSON(column19), column20, PARSE_JSON(column21), column22
    """
    try:
        cur = conn.cursor()
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            params = []
            for r in chunk:
                payload = r["payload"]
                ohlcv = r["ohlcv"]
                params.append(
                    (
                        r["run_id"],
                        r["replay_attempt_id"],
                        str(r["symbol"]).upper(),
                        r["trading_date"],
                        r["bar_ts_utc"],
                        r["sequence_num"],
                        json.dumps(ohlcv),
                        json.dumps(payload["objective_facts_json"]),
                        json.dumps(payload["derived_metrics_json"]),
                        json.dumps(payload["brooks_obs_json"]),
                        json.dumps(payload["rule_evaluations_json"]),
                        payload["ruleset_version"],
                        payload["visible_history_count"],
                        json.dumps(payload["pattern_state_json"]),
                        json.dumps(payload["context_json"]),
                        payload["state_before"],
                        payload["state_after"],
                        payload["action"],
                        json.dumps(payload["blockers_json"]),
                        payload["explanation"],
                        json.dumps(payload["rule_ids"]),
                        payload["data_quality_status"],
                    )
                )
            execute_insert_select_from_values(
                cur,
                table_and_columns=table,
                select_list_sql=select_list,
                row_params=params,
            )
            written += len(chunk)
            if commit:
                conn.commit()
        return written
    finally:
        if own:
            conn.close()


def observation_sequence_hash(run_id: str, *, replay_attempt_id: str) -> str:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TRADING_DATE, BAR_TS, SYMBOL,
                   OBJECTIVE_FACTS_JSON:ohlcv:open::FLOAT,
                   OBJECTIVE_FACTS_JSON:ohlcv:high::FLOAT,
                   OBJECTIVE_FACTS_JSON:ohlcv:low::FLOAT,
                   OBJECTIVE_FACTS_JSON:ohlcv:close::FLOAT,
                   OBJECTIVE_FACTS_JSON:ohlcv:volume::FLOAT
            FROM MIP.APP.BROOKS_INTRADAY_BAR_OBSERVATION
            WHERE RUN_ID = %s AND REPLAY_ATTEMPT_ID = %s
            ORDER BY BAR_TS, SYMBOL
            """,
            (run_id, replay_attempt_id),
        )
        parts = ["|".join(str(x) for x in row) for row in cur.fetchall()]
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()
    finally:
        conn.close()


def insert_placeholder_observation(
    *,
    run_id: str,
    symbol: str,
    trading_date: date,
    bar_ts_utc: datetime,
    sequence_num: int,
    ohlcv: dict[str, Any],
    replay_attempt_id: str | None = None,
) -> bool:
    """Legacy Phase 3 transport placeholder (deprecated)."""
    from .objective_ruleset_v01 import legacy_attempt_id

    attempt = replay_attempt_id or legacy_attempt_id(run_id)
    objective = {"ohlcv": ohlcv, "facts": []}
    context = {"phase": "REPLAY_TRANSPORT_ONLY"}
    payload = {
        "objective_facts_json": objective,
        "derived_metrics_json": {},
        "brooks_obs_json": [],
        "rule_evaluations_json": [],
        "pattern_state_json": [],
        "context_json": context,
        "state_before": "DOSSIER_READY",
        "state_after": "OBSERVING",
        "action": "OBSERVE",
        "blockers_json": ["BROOKS_RULE_ENGINE_NOT_IMPLEMENTED"],
        "explanation": "Historical bar revealed. Brooks observation logic is not active in Phase 3.",
        "rule_ids": [],
        "data_quality_status": "COMPLETE",
        "visible_history_count": 0,
        "ruleset_version": "BROOKS_REPLAY_TRANSPORT_V0_1",
    }
    return insert_objective_observation(
        run_id=run_id,
        replay_attempt_id=attempt,
        symbol=symbol,
        trading_date=trading_date,
        bar_ts_utc=bar_ts_utc,
        sequence_num=sequence_num,
        ohlcv=ohlcv,
        payload=payload,
    )

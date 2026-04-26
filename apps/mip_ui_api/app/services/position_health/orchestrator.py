"""
Daily Position Health V1 - shadow orchestrator.

Entry point: run_shadow_health_review(as_of_date)

Stages:
  0. Resolve config (feature flag, agent name, concurrency, credentials).
  1. Build per-position payloads from canonical real verdicts (payload_builder).
  2. Call POSITION_HEALTH_REVIEW_AGENT in parallel for each position
     (bounded concurrency).
  3. MERGE each ShadowReviewResult into DAILY_POSITION_SHADOW_REVIEW.
     Failures persist with SHADOW_RUN_STATUS in (PARSE_ERROR, API_ERROR).
  4. CALL SP_UPDATE_SHADOW_POSITION_LIFECYCLE(:as_of_date) so the bake-off
     lifecycle reflects today's verdicts (open new, simulate exits at close).
  5. Return OrchestrationResult.

The orchestrator is reusable: it is invoked from a FastAPI router (manual
trigger) and from the daily job after the structural pipeline.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from app.config import get_snowflake_config
from app.db import fetch_all, get_connection

from .health_review_client import (
    DEFAULT_AGENT_NAME,
    DEFAULT_AGENT_TIMEOUT_SEC,
    review_one_position,
)
from .payload_builder import (
    DEFAULT_PATH_LOOKBACK_BARS,
    build_position_payloads,
)
from .types import (
    OrchestrationResult,
    PositionPayload,
    ShadowReviewResult,
    ShadowRunStatus,
)

logger = logging.getLogger(__name__)

DEFAULT_MAX_PARALLEL = 4


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _read_config_flag(key: str, default: str) -> str:
    """Read APP_CONFIG.CONFIG_VALUE; fall back to default on missing/error."""
    sql = "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %(k)s"
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, {"k": key})
            row = cur.fetchone()
            if row and row[0] is not None:
                return str(row[0])
        finally:
            cur.close()
    except Exception as e:
        logger.warning("position_health.config: failed to read %s (%s)", key, e)
    finally:
        conn.close()
    return default


def _get_snowflake_creds() -> Dict[str, str]:
    cfg = get_snowflake_config()
    return {
        "account": cfg.get("account") or "",
        "user": cfg.get("user") or "",
        "private_key_path": cfg.get("private_key_path") or "",
    }


def _is_truthy(value: Optional[str]) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

_MERGE_SQL = """
    MERGE INTO MIP.APP.DAILY_POSITION_SHADOW_REVIEW tgt
    USING (
        SELECT
            %(as_of_date)s              AS AS_OF_DATE,
            %(position_episode_key)s    AS POSITION_EPISODE_KEY,
            %(portfolio_id)s            AS PORTFOLIO_ID,
            %(episode_id)s              AS EPISODE_ID,
            %(symbol)s                  AS SYMBOL,
            %(side)s                    AS SIDE,
            %(entry_date)s              AS ENTRY_DATE,
            %(shadow_verdict)s          AS SHADOW_VERDICT,
            %(shadow_thesis_status)s    AS SHADOW_THESIS_STATUS,
            %(shadow_action_bias)s      AS SHADOW_ACTION_BIAS,
            %(shadow_severity)s         AS SHADOW_SEVERITY,
            %(primary_reason_code)s     AS PRIMARY_REASON_CODE,
            %(primary_reason_text)s     AS PRIMARY_REASON_TEXT,
            %(observation_summary)s     AS OBSERVATION_SUMMARY,
            %(verdict_summary)s         AS VERDICT_SUMMARY,
            %(why_summary)s             AS WHY_SUMMARY,
            %(rationale_text)s          AS RATIONALE_TEXT,
            PARSE_JSON(%(rationale_json)s) AS RATIONALE_JSON,
            %(agrees_with_real)s        AS AGREES_WITH_REAL,
            %(disagreement_class)s      AS DISAGREEMENT_CLASS,
            %(shadow_run_status)s       AS SHADOW_RUN_STATUS,
            %(shadow_run_ts)s           AS SHADOW_RUN_TS,
            %(shadow_run_elapsed_ms)s   AS SHADOW_RUN_ELAPSED_MS,
            %(shadow_run_error)s        AS SHADOW_RUN_ERROR,
            %(agent_name)s              AS AGENT_NAME,
            %(model_version)s           AS MODEL_VERSION,
            %(prompt_version)s          AS PROMPT_VERSION
    ) src
    ON tgt.AS_OF_DATE = src.AS_OF_DATE
       AND tgt.POSITION_EPISODE_KEY = src.POSITION_EPISODE_KEY
    WHEN MATCHED THEN UPDATE SET
        PORTFOLIO_ID = src.PORTFOLIO_ID,
        EPISODE_ID = src.EPISODE_ID,
        SYMBOL = src.SYMBOL,
        SIDE = src.SIDE,
        ENTRY_DATE = src.ENTRY_DATE,
        SHADOW_VERDICT = src.SHADOW_VERDICT,
        SHADOW_THESIS_STATUS = src.SHADOW_THESIS_STATUS,
        SHADOW_ACTION_BIAS = src.SHADOW_ACTION_BIAS,
        SHADOW_SEVERITY = src.SHADOW_SEVERITY,
        PRIMARY_REASON_CODE = src.PRIMARY_REASON_CODE,
        PRIMARY_REASON_TEXT = src.PRIMARY_REASON_TEXT,
        OBSERVATION_SUMMARY = src.OBSERVATION_SUMMARY,
        VERDICT_SUMMARY = src.VERDICT_SUMMARY,
        WHY_SUMMARY = src.WHY_SUMMARY,
        RATIONALE_TEXT = src.RATIONALE_TEXT,
        RATIONALE_JSON = src.RATIONALE_JSON,
        AGREES_WITH_REAL = src.AGREES_WITH_REAL,
        DISAGREEMENT_CLASS = src.DISAGREEMENT_CLASS,
        SHADOW_RUN_STATUS = src.SHADOW_RUN_STATUS,
        SHADOW_RUN_TS = src.SHADOW_RUN_TS,
        SHADOW_RUN_ELAPSED_MS = src.SHADOW_RUN_ELAPSED_MS,
        SHADOW_RUN_ERROR = src.SHADOW_RUN_ERROR,
        AGENT_NAME = src.AGENT_NAME,
        MODEL_VERSION = src.MODEL_VERSION,
        PROMPT_VERSION = src.PROMPT_VERSION,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        AS_OF_DATE, POSITION_EPISODE_KEY, PORTFOLIO_ID, EPISODE_ID, SYMBOL, SIDE, ENTRY_DATE,
        SHADOW_VERDICT, SHADOW_THESIS_STATUS, SHADOW_ACTION_BIAS, SHADOW_SEVERITY,
        PRIMARY_REASON_CODE, PRIMARY_REASON_TEXT,
        OBSERVATION_SUMMARY, VERDICT_SUMMARY, WHY_SUMMARY, RATIONALE_TEXT, RATIONALE_JSON,
        AGREES_WITH_REAL, DISAGREEMENT_CLASS,
        SHADOW_RUN_STATUS, SHADOW_RUN_TS, SHADOW_RUN_ELAPSED_MS, SHADOW_RUN_ERROR,
        AGENT_NAME, MODEL_VERSION, PROMPT_VERSION
    ) VALUES (
        src.AS_OF_DATE, src.POSITION_EPISODE_KEY, src.PORTFOLIO_ID, src.EPISODE_ID,
        src.SYMBOL, src.SIDE, src.ENTRY_DATE,
        src.SHADOW_VERDICT, src.SHADOW_THESIS_STATUS, src.SHADOW_ACTION_BIAS, src.SHADOW_SEVERITY,
        src.PRIMARY_REASON_CODE, src.PRIMARY_REASON_TEXT,
        src.OBSERVATION_SUMMARY, src.VERDICT_SUMMARY, src.WHY_SUMMARY,
        src.RATIONALE_TEXT, src.RATIONALE_JSON,
        src.AGREES_WITH_REAL, src.DISAGREEMENT_CLASS,
        src.SHADOW_RUN_STATUS, src.SHADOW_RUN_TS, src.SHADOW_RUN_ELAPSED_MS, src.SHADOW_RUN_ERROR,
        src.AGENT_NAME, src.MODEL_VERSION, src.PROMPT_VERSION
    )
"""


def _persist_one_review(
    payload: PositionPayload,
    result: ShadowReviewResult,
    agent_name: str,
    model_version: str,
    prompt_version: str,
) -> None:
    rationale_json = (
        json.dumps(result.rationale_json, default=str)
        if result.rationale_json is not None
        else None
    )
    params = {
        "as_of_date": payload.as_of_date,
        "position_episode_key": payload.position_episode_key,
        "portfolio_id": payload.portfolio_id,
        "episode_id": payload.episode_id,
        "symbol": payload.symbol,
        "side": payload.side,
        "entry_date": payload.entry_date,
        "shadow_verdict": result.shadow_verdict,
        "shadow_thesis_status": result.shadow_thesis_status,
        "shadow_action_bias": result.shadow_action_bias,
        "shadow_severity": result.shadow_severity,
        "primary_reason_code": result.primary_reason_code,
        "primary_reason_text": result.primary_reason_text,
        "observation_summary": result.observation_summary,
        "verdict_summary": result.verdict_summary,
        "why_summary": result.why_summary,
        "rationale_text": result.rationale_text,
        "rationale_json": rationale_json,
        "agrees_with_real": result.agrees_with_real,
        "disagreement_class": result.disagreement_class,
        "shadow_run_status": result.shadow_run_status.value,
        "shadow_run_ts": result.shadow_run_ts,
        "shadow_run_elapsed_ms": result.shadow_run_elapsed_ms,
        "shadow_run_error": result.shadow_run_error,
        "agent_name": agent_name,
        "model_version": model_version,
        "prompt_version": prompt_version,
    }
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(_MERGE_SQL, params)
        finally:
            cur.close()
    finally:
        conn.close()


def _call_lifecycle_sp(as_of_date: date) -> Optional[Dict[str, Any]]:
    """Call SP_UPDATE_SHADOW_POSITION_LIFECYCLE and return its VARIANT summary."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "CALL MIP.APP.SP_UPDATE_SHADOW_POSITION_LIFECYCLE(%(d)s)",
                {"d": as_of_date},
            )
            row = cur.fetchone()
            if not row:
                return None
            raw = row[0]
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return {"raw": raw}
            if isinstance(raw, dict):
                return raw
            return {"raw": str(raw)}
        finally:
            cur.close()
    except Exception as e:
        logger.warning("position_health.lifecycle: SP call failed: %s", e)
        return {"error": str(e)}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def run_shadow_health_review(
    as_of_date: date,
    max_parallel: Optional[int] = None,
    force: bool = False,
) -> OrchestrationResult:
    """
    Run shadow health review for every open position with a row in
    DAILY_POSITION_VERDICT for as_of_date.

    Args:
        as_of_date: business date being evaluated.
        max_parallel: cap on concurrent agent calls (default 4).
        force: when True, runs even if POSITION_HEALTH_SHADOW_ENABLED is false.
    """
    started_at = datetime.utcnow()
    out = OrchestrationResult(
        as_of_date=as_of_date,
        started_at=started_at,
        status="PENDING",
    )

    enabled = _read_config_flag("POSITION_HEALTH_SHADOW_ENABLED", "true")
    if not _is_truthy(enabled) and not force:
        out.completed_at = datetime.utcnow()
        out.status = "SKIPPED"
        out.error = "POSITION_HEALTH_SHADOW_ENABLED is false"
        logger.info("position_health.orchestrator: skipped (flag off) for %s", as_of_date)
        return out

    agent_name = _read_config_flag("POSITION_HEALTH_AGENT_NAME", DEFAULT_AGENT_NAME)
    lookback_str = _read_config_flag(
        "POSITION_HEALTH_PATH_LOOKBACK_DAYS", str(DEFAULT_PATH_LOOKBACK_BARS)
    )
    try:
        lookback = max(5, int(lookback_str))
    except ValueError:
        lookback = DEFAULT_PATH_LOOKBACK_BARS

    creds = _get_snowflake_creds()
    if not creds["private_key_path"]:
        out.completed_at = datetime.utcnow()
        out.status = "FAIL"
        out.error = "Snowflake keypair not configured (SNOWFLAKE_PRIVATE_KEY_PATH missing)"
        logger.error("position_health.orchestrator: %s", out.error)
        return out

    try:
        payloads = build_position_payloads(as_of_date=as_of_date, lookback_bars=lookback)
    except Exception as e:
        out.completed_at = datetime.utcnow()
        out.status = "FAIL"
        out.error = f"payload_builder failed: {e}"
        logger.exception("position_health.orchestrator: payload build failed")
        return out

    out.positions_total = len(payloads)
    if not payloads:
        out.completed_at = datetime.utcnow()
        out.status = "SUCCESS"
        out.lifecycle_summary = _call_lifecycle_sp(as_of_date)
        logger.info(
            "position_health.orchestrator: no open positions for %s; lifecycle still ran",
            as_of_date,
        )
        return out

    cap = max(1, int(max_parallel or DEFAULT_MAX_PARALLEL))
    semaphore = asyncio.Semaphore(cap)
    results: List[ShadowReviewResult] = []

    async def _bounded(p: PositionPayload) -> ShadowReviewResult:
        async with semaphore:
            return await review_one_position(
                payload=p,
                account=creds["account"],
                user=creds["user"],
                private_key_path=creds["private_key_path"],
                agent_name=agent_name,
                timeout=DEFAULT_AGENT_TIMEOUT_SEC,
            )

    out.reviews_attempted = len(payloads)
    coros = [_bounded(p) for p in payloads]
    results = await asyncio.gather(*coros, return_exceptions=False)

    # Persist each review (run sync DB I/O off the event loop).
    persistence_errors = 0
    for payload, result in zip(payloads, results):
        try:
            await asyncio.to_thread(
                _persist_one_review,
                payload,
                result,
                agent_name,
                "claude-4-sonnet",
                "1.0.0",
            )
        except Exception as e:
            persistence_errors += 1
            logger.exception(
                "position_health.persist: failed for key=%s symbol=%s err=%s",
                payload.position_episode_key, payload.symbol, e,
            )

        if result.shadow_run_status == ShadowRunStatus.SUCCESS:
            out.reviews_success += 1
        elif result.shadow_run_status == ShadowRunStatus.PARSE_ERROR:
            out.reviews_parse_error += 1
        elif result.shadow_run_status == ShadowRunStatus.API_ERROR:
            out.reviews_api_error += 1
        elif result.shadow_run_status == ShadowRunStatus.SKIPPED:
            out.reviews_skipped += 1

    # Update bake-off lifecycle (open new shadow positions; simulate exits).
    out.lifecycle_summary = await asyncio.to_thread(_call_lifecycle_sp, as_of_date)

    out.completed_at = datetime.utcnow()
    if persistence_errors > 0 and out.reviews_success == 0:
        out.status = "FAIL"
        out.error = f"{persistence_errors} persistence errors, no successes"
    else:
        out.status = "SUCCESS"
    logger.info(
        "position_health.orchestrator: as_of=%s total=%d ok=%d parse_err=%d api_err=%d skipped=%d",
        as_of_date,
        out.positions_total,
        out.reviews_success,
        out.reviews_parse_error,
        out.reviews_api_error,
        out.reviews_skipped,
    )
    return out


# ---------------------------------------------------------------------------
# Read helpers (UI / API)
# ---------------------------------------------------------------------------

def _query_view(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or {})
            return fetch_all(cur)
        finally:
            cur.close()
    finally:
        conn.close()


def get_latest_comparison_rows(portfolio_id: Optional[int] = None) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM MIP.MART.V_POSITION_HEALTH_COMPARISON_LATEST"
    params: Dict[str, Any] = {}
    if portfolio_id is not None:
        sql += " WHERE PORTFOLIO_ID = %(p)s"
        params["p"] = int(portfolio_id)
    sql += " ORDER BY AS_OF_DATE DESC, PORTFOLIO_ID, SYMBOL"
    return _query_view(sql, params)


def get_history_for_position(position_episode_key: str) -> Dict[str, List[Dict[str, Any]]]:
    real_sql = (
        "SELECT * FROM MIP.MART.V_DAILY_POSITION_VERDICT_HISTORY "
        "WHERE POSITION_EPISODE_KEY = %(k)s ORDER BY AS_OF_DATE ASC"
    )
    shadow_sql = (
        "SELECT * FROM MIP.MART.V_DAILY_POSITION_SHADOW_REVIEW_HISTORY "
        "WHERE POSITION_EPISODE_KEY = %(k)s ORDER BY AS_OF_DATE ASC"
    )
    return {
        "real": _query_view(real_sql, {"k": position_episode_key}),
        "shadow": _query_view(shadow_sql, {"k": position_episode_key}),
    }


def get_lifecycle_rows(portfolio_id: Optional[int] = None) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM MIP.MART.V_SHADOW_POSITION_LIFECYCLE_STATUS"
    params: Dict[str, Any] = {}
    if portfolio_id is not None:
        sql += " WHERE PORTFOLIO_ID = %(p)s"
        params["p"] = int(portfolio_id)
    sql += " ORDER BY SHADOW_STATUS, SYMBOL"
    return _query_view(sql, params)

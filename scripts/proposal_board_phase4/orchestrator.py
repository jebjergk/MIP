"""
Phase 4 Cortex Agentic Proposal Board — multi-stage orchestrator.

Stages:
  0  Snapshot dossiers from V_PROPOSAL_BOARD_SYMBOL_DOSSIER into
     PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT and stage the per-dossier
     evidence pack into PROPOSAL_BOARD_DOSSIER_PACK_CACHE.

  1  Run 5 specialist agents per dossier in parallel via persistent
     CREATE AGENT objects (DATA_AGENT_RUN equivalent through REST).
     Persist each initial position to PROPOSAL_BOARD_AGENT_OUTCOME_V2.

  2  Detect conflicts in Python from persisted positions.

  3  Challenge turn (objectless AGENT_RUN per challenger) — persist to
     PROPOSAL_BOARD_INTERACTION_V2 with TOPIC, DISAGREEMENT_TYPE.

  4  Revision turn (objectless AGENT_RUN for the challenged role) —
     persist response to PROPOSAL_BOARD_INTERACTION_V2 and update the
     challenged role's row in PROPOSAL_BOARD_AGENT_OUTCOME_V2 with the
     revised stance. Loop back to Stage 2 if MAX_ROUNDS not exhausted
     and revision changed any verdicts.

  5  Chair agent — runs only if all 5 specialists are durable. Reads
     persisted positions + interactions and authors the final decision.
     Persist to PROPOSAL_BOARD_THESIS_VERDICT and
     PROPOSAL_BOARD_FINAL_SLATE_V2.

  6  Apply publication policy and insert published rows into
     STRUCTURAL_TRADE_PROPOSALS, gated by FINAL_SLATE_V2.PUBLICATION_STATUS.
     FX/short live-flag gating applied here.

Hard ordering invariants:
  * Chair runs only if `SELECT COUNT(*) = 5` for the dossier in
    AGENT_OUTCOME_V2.
  * FINAL_SLATE_V2 only after THESIS_VERDICT row exists.
  * STRUCTURAL_TRADE_PROPOSALS only for FINAL_SLATE_V2 rows with
    PUBLICATION_STATUS='PENDING'.

Failure policy:
  * Any specialist with malformed JSON or non-allowlisted enum -> that
    specialist's row is NOT written; the dossier is marked INVALID and
    excluded from chair / final slate. The whole run is marked FAILED
    if any dossier has any specialist in an INVALID state by Stage 5.
  * No deterministic fallback. No old selector. No candidate-review board.

Entry point: orchestrate_phase4_board(...)
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import date as _date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from .conflict_detection import ConflictEntry, detect_conflicts, detect_stance_drift
from .cortex_client import (
    extract_agent_text,
    run_agent_object,
    run_agent_objectless,
)
from .review_eligibility import EligibilityDecision, evaluate_dossier_eligibility

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

_AGENT_OBJECT_NAMES = {
    "MARKET_STRUCTURE":    "PHASE4_MARKET_STRUCTURE_AGENT",
    "LEVEL_PRICE_ACTION":  "PHASE4_LEVEL_PRICE_ACTION_AGENT",
    "THESIS":              "PHASE4_THESIS_AGENT",
    "HISTORICAL_EVIDENCE": "PHASE4_HISTORICAL_EVIDENCE_AGENT",
    "RISK_EXECUTION":      "PHASE4_RISK_EXECUTION_AGENT",
}
_CHAIR_AGENT_NAME = "PHASE4_CHAIR_PORTFOLIO_PM_AGENT"
_REQUIRED_ROLES = list(_AGENT_OBJECT_NAMES.keys())

_OBJECTLESS_MODEL = "claude-sonnet-4-6"

_PROMPT_VERSION = "phase4_agentic_board_v1_cortex_agents_multi_round"
_POLICY_VERSION = "phase4_agentic_board_v1"
_MODEL_CONFIG_MODE = "symbol_dossier_cortex_agentic_board_multi_round"

_CACHE_TTL_HOURS = 24
_DEFAULT_MAX_ROUNDS = 2
_DEFAULT_MAX_PROPOSALS = 8

# Cost-control defaults — empirically validated from QUERY_HISTORY (May 2026).
# ~7 Cortex agent sessions per candidate (5 specialists + chair + ~1 revision avg).
# ~10 GET_PHASE4_DOSSIER_SLICE calls per session (measured avg 9.7).
_DEFAULT_MAX_CANDIDATES: Optional[int] = None   # None = uncapped legacy mode
_DEFAULT_DAILY_CALL_BUDGET: int = 80            # agent sessions; use allow_budget_override to exceed
_EMPIRICAL_SESSIONS_PER_CANDIDATE: int = 7
_EMPIRICAL_SLICES_PER_SESSION: int = 10

_AGENT_TIMEOUT_SEC = 240.0
_OBJECTLESS_TIMEOUT_SEC = 180.0


# ---------------------------------------------------------------------------
# Allowed enums (mirrors agent system prompts)
# ---------------------------------------------------------------------------

_ALLOWED_VERDICTS = {
    "MARKET_STRUCTURE": {
        "TREND_UP", "TREND_DOWN", "RANGE", "BREAKOUT_ATTEMPT",
        "FAILED_BREAKOUT", "RESISTANCE_REJECTION", "SUPPORT_BOUNCE",
        "EXHAUSTION", "REVERSAL_FORMING", "CHOP_NO_EDGE",
    },
    "LEVEL_PRICE_ACTION": {
        "LONG_LOCATION", "SHORT_LOCATION", "BOTH_SIDES",
        "WAIT_CONFIRMATION", "NO_EDGE",
    },
    "THESIS": {
        "LONG_THESIS", "SHORT_THESIS", "WATCH_LONG", "WATCH_SHORT",
        "NO_TRADE", "CONFLICTED",
    },
    "HISTORICAL_EVIDENCE": {
        "LONG_SUPPORTIVE", "SHORT_SUPPORTIVE",
        "MIXED_DIRECTIONAL", "WEAK_BOTH_SIDES",
    },
    "RISK_EXECUTION": {
        "ACTIONABLE", "RESEARCH_ONLY", "WAIT_CONFIRMATION",
        "NO_TRADE", "HARD_BLOCK",
    },
}
_ALLOWED_PRIMARY_REASON = {
    "MARKET_STRUCTURE": {
        "STRUCTURE_TREND_UP", "STRUCTURE_TREND_DOWN", "STRUCTURE_RANGE",
        "STRUCTURE_CHOP_NO_EDGE", "STRUCTURE_REVERSAL_FORMING",
        "STRUCTURE_FAILED_BREAKOUT",
    },
    "LEVEL_PRICE_ACTION": {
        "LEVEL_LONG_LOCATION", "LEVEL_SHORT_LOCATION",
        "LEVEL_WAIT_CONFIRMATION", "LEVEL_NO_EDGE",
    },
    "THESIS": {
        "THESIS_LONG", "THESIS_SHORT", "THESIS_WATCH_LONG",
        "THESIS_WATCH_SHORT", "THESIS_NO_TRADE", "THESIS_CONFLICTED",
    },
    "HISTORICAL_EVIDENCE": {
        "HISTORY_LONG_SUPPORTIVE", "HISTORY_SHORT_SUPPORTIVE",
        "HISTORY_MIXED_DIRECTIONAL", "HISTORY_WEAK_BOTH_SIDES",
    },
    "RISK_EXECUTION": {
        "RISK_ACTIONABLE", "RISK_RESEARCH_ONLY",
        "RISK_WAIT_CONFIRMATION", "RISK_NO_TRADE", "RISK_HARD_BLOCK",
        "SHORT_LIVE_DISABLED", "SHORT_RESEARCH_ONLY",
    },
}

_CHAIR_ALLOWED_FINAL_ACTIONS = {
    "PROPOSE_LONG", "PROPOSE_SHORT",
    "WATCH_LONG", "WATCH_SHORT",
    # Phase 4 taxonomy v2: thesis-health states for stewarding a contested
    # prior thesis without committing to a fresh opposite-direction trade.
    "WATCH_LONG_FAILURE", "WATCH_SHORT_FAILURE",
    "NO_TRADE", "REJECT", "WAIT_FOR_CONFIRMATION",
}
_CHAIR_ALLOWED_FINAL_DIRECTIONS = {"LONG", "SHORT", "NONE"}
_CHAIR_ALLOWED_PRIMARY_REASON = {
    "CHAIR_PROPOSE_LONG", "CHAIR_PROPOSE_SHORT",
    "CHAIR_WATCH_LONG", "CHAIR_WATCH_SHORT",
    "CHAIR_WATCH_LONG_FAILURE", "CHAIR_WATCH_SHORT_FAILURE",
    "CHAIR_NO_TRADE", "CHAIR_REJECT", "CHAIR_WAIT_FOR_CONFIRMATION",
    "SHORT_RESEARCH_ONLY", "FX_LIVE_DISABLED",
}
# Phase 4 taxonomy v2: required thesis_health labels.
_CHAIR_ALLOWED_THESIS_HEALTH = {
    "LONG_CONFIRMED",
    "LONG_DEGRADED_BUT_ALIVE",
    "LONG_REJECTED",
    "SHORT_CONFIRMED",
    "SHORT_DEGRADED_BUT_ALIVE",
    "SHORT_REJECTED",
    "NEUTRAL",
}
# final_actions that require thesis_label to start with AGENTIC_.
_AGENTIC_THESIS_LABEL_REQUIRED = {
    "PROPOSE_LONG", "PROPOSE_SHORT",
    "WATCH_LONG", "WATCH_SHORT",
    "WATCH_LONG_FAILURE", "WATCH_SHORT_FAILURE",
}
# final_actions that must carry a non-null prior_thesis_reference.
_REQUIRES_PRIOR_THESIS_REFERENCE = {
    "WATCH_LONG_FAILURE", "WATCH_SHORT_FAILURE",
}
# Phase 4 structural v2: Chair emits MSM summary JSON for UI consumers.
_CHAIR_STRUCTURE_SUMMARY_REQUIRED_ACTIONS = {
    "PROPOSE_LONG",
    "PROPOSE_SHORT",
    "WATCH_LONG_FAILURE",
    "WATCH_SHORT_FAILURE",
    "WAIT_FOR_CONFIRMATION",
}
_CHAIR_MARKET_STRUCTURE_READ_KEYS = (
    "primary_structure",
    "structure_health",
    "current_phase",
    "latest_structure_event",
    "bos_body_close_confirmed",
    "choch_detected",
    "structure_posture_hint",
)


# ---------------------------------------------------------------------------
# Snowflake connection helper
# ---------------------------------------------------------------------------

def _load_env() -> None:
    agent_env = _PROJECT_ROOT / ".env.agent"
    fallback_env = _PROJECT_ROOT / ".env"
    if agent_env.exists():
        load_dotenv(agent_env, override=True)
    elif fallback_env.exists():
        load_dotenv(fallback_env, override=True)


def _connect():
    """Open a Snowflake connection using .env.agent credentials."""
    import snowflake.connector
    _load_env()
    auth_method = (os.getenv("SNOWFLAKE_AUTH_METHOD") or "password").strip().lower()
    params: Dict[str, Any] = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }
    if auth_method == "keypair":
        key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        if not key_path:
            raise RuntimeError("SNOWFLAKE_AUTH_METHOD=keypair but SNOWFLAKE_PRIVATE_KEY_PATH not set")
        params["authenticator"] = "SNOWFLAKE_JWT"
        params["private_key_file"] = key_path
        passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        if passphrase:
            params["private_key_file_pwd"] = passphrase
    else:
        password = os.getenv("SNOWFLAKE_PASSWORD")
        if not password:
            raise RuntimeError("SNOWFLAKE_AUTH_METHOD=password but SNOWFLAKE_PASSWORD not set")
        params["password"] = password
    return snowflake.connector.connect(**params)


def _get_rest_creds() -> Tuple[str, str, str]:
    _load_env()
    return (
        os.getenv("SNOWFLAKE_ACCOUNT") or "",
        os.getenv("SNOWFLAKE_USER") or "",
        os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or "",
    )


# ---------------------------------------------------------------------------
# JSON parsing helpers
# ---------------------------------------------------------------------------

_FENCE_RE = re.compile(r"^[\s`]*(?:json)?[\s`]*", re.IGNORECASE)
_FENCE_END_RE = re.compile(r"[\s`]*$")


def _strip_fences(text: str) -> str:
    if not text:
        return text
    s = text.strip()
    s = _FENCE_RE.sub("", s, count=1)
    s = _FENCE_END_RE.sub("", s, count=1)
    return s.strip()


def _try_parse_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    s = _strip_fences(text)
    try:
        return json.loads(s)
    except Exception:
        # find first { ... last }
        first = s.find("{")
        last = s.rfind("}")
        if first >= 0 and last > first:
            try:
                return json.loads(s[first:last + 1])
            except Exception:
                return None
    return None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _truncate(s: Any, n: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n]


# ---------------------------------------------------------------------------
# Specialist position validation
# ---------------------------------------------------------------------------

@dataclass
class SpecialistPosition:
    role: str
    verdict: str
    primary_reason_code: str
    secondary_reason_code: Optional[str]
    confidence: Optional[float]
    long_score: Optional[float]
    short_score: Optional[float]
    no_trade_score: Optional[float]
    rationale: str
    structured_output: Dict[str, Any]
    invalid_reason: Optional[str] = None  # if non-None: position is INVALID


def _validate_specialist(role: str, raw: Optional[Dict[str, Any]]) -> SpecialistPosition:
    """Validate a parsed specialist response against allowlists."""
    if not isinstance(raw, dict):
        return SpecialistPosition(
            role=role, verdict="", primary_reason_code="",
            secondary_reason_code=None, confidence=None,
            long_score=None, short_score=None, no_trade_score=None,
            rationale="", structured_output={"raw": str(raw)},
            invalid_reason="MISSING_OR_NON_OBJECT_JSON",
        )

    verdict = str(raw.get("verdict") or "").strip().upper()
    primary = str(raw.get("primary_reason_code") or "").strip().upper()
    secondary_v = raw.get("secondary_reason_code")
    secondary = str(secondary_v).strip().upper() if secondary_v else None

    if verdict not in _ALLOWED_VERDICTS.get(role, set()):
        return SpecialistPosition(
            role=role, verdict=verdict, primary_reason_code=primary,
            secondary_reason_code=secondary, confidence=None,
            long_score=None, short_score=None, no_trade_score=None,
            rationale=str(raw.get("rationale") or ""),
            structured_output=raw,
            invalid_reason=f"VERDICT_NOT_IN_ALLOWLIST:{verdict}",
        )
    if primary not in _ALLOWED_PRIMARY_REASON.get(role, set()):
        return SpecialistPosition(
            role=role, verdict=verdict, primary_reason_code=primary,
            secondary_reason_code=secondary, confidence=None,
            long_score=None, short_score=None, no_trade_score=None,
            rationale=str(raw.get("rationale") or ""),
            structured_output=raw,
            invalid_reason=f"PRIMARY_REASON_NOT_IN_ALLOWLIST:{primary}",
        )

    return SpecialistPosition(
        role=role,
        verdict=verdict,
        primary_reason_code=primary,
        secondary_reason_code=secondary,
        confidence=_to_float(raw.get("confidence")),
        long_score=_to_float(raw.get("long_score")),
        short_score=_to_float(raw.get("short_score")),
        no_trade_score=_to_float(raw.get("no_trade_score")),
        rationale=str(raw.get("rationale") or ""),
        structured_output=raw,
    )


# ---------------------------------------------------------------------------
# Snowflake JSON persistence helpers
# ---------------------------------------------------------------------------

def _jdump(obj: Any) -> str:
    return json.dumps(obj, default=str)


# ---------------------------------------------------------------------------
# Stage 0.5 — direction-neutral review eligibility filter
# ---------------------------------------------------------------------------

def _persist_eligibility(
    cur,
    run_id: str,
    as_of: _date,
    portfolio_id: Optional[int],
    dossier_id: Optional[int],
    decision: EligibilityDecision,
) -> None:
    """Insert one PROPOSAL_BOARD_REVIEW_ELIGIBILITY row per symbol per run."""
    cur.execute(
        """
        INSERT INTO MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY (
            RUN_ID, AS_OF_DATE, PORTFOLIO_ID, DOSSIER_ID, SYMBOL, MARKET_TYPE,
            ELIGIBLE, PRIMARY_REASON_CODE, SECONDARY_REASON_CODE,
            SIGNAL_FLAGS_JSON, EVIDENCE_SUMMARY_JSON, NOTES
        )
        SELECT %(run_id)s, %(as_of)s, %(pid)s, %(did)s, %(sym)s, %(mkt)s,
               %(elig)s, %(primary)s, %(secondary)s,
               PARSE_JSON(%(flags)s), PARSE_JSON(%(summary)s), %(notes)s
        """,
        {
            "run_id": run_id,
            "as_of": as_of,
            "pid": portfolio_id,
            "did": dossier_id,
            "sym": (decision.symbol or "")[:20],
            "mkt": (decision.market_type or "")[:20],
            "elig": bool(decision.eligible),
            "primary": (decision.primary_reason_code or "AGENT_OUTPUT_INVALID")[:80],
            "secondary": (decision.secondary_reason_code[:80] if decision.secondary_reason_code else None),
            "flags": _jdump(decision.signal_flags or {}),
            "summary": _jdump(decision.evidence_summary or {}),
            "notes": _truncate(decision.notes, 2000),
        },
    )


# ---------------------------------------------------------------------------
# IBKR account-mode helper (short-publication safety gate)
# ---------------------------------------------------------------------------

def _get_ibkr_account_mode(cur, portfolio_id: Optional[int]) -> str:
    """Return 'PAPER', 'REAL', or 'UNKNOWN' for the configured portfolio.

    Falls back to 'UNKNOWN' if portfolio_id is NULL or the row is missing.
    Any value not in the allowlist also collapses to 'UNKNOWN' so downstream
    code can fail closed.
    """
    if portfolio_id is None:
        return "UNKNOWN"
    cur.execute(
        """
        SELECT UPPER(COALESCE(IBKR_ACCOUNT_MODE, 'UNKNOWN'))
          FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
         WHERE PORTFOLIO_ID = %(pid)s
        """,
        {"pid": int(portfolio_id)},
    )
    row = cur.fetchone()
    if not row:
        return "UNKNOWN"
    val = (row[0] or "UNKNOWN").strip().upper()
    if val not in {"PAPER", "REAL", "UNKNOWN"}:
        return "UNKNOWN"
    return val


# ---------------------------------------------------------------------------
# Stage 0 — snapshot dossiers + stage evidence pack cache
# ---------------------------------------------------------------------------

def _snapshot_dossiers(
    cur,
    run_id: str,
    as_of_date: _date,
    portfolio_id: Optional[int],
    symbols_filter: Optional[List[str]],
    market_types_filter: Optional[List[str]] = None,
) -> List[Tuple[int, str, str, Dict[str, Any]]]:
    """
    Insert dossier snapshot rows for this run. Returns
    [(dossier_id, symbol, market_type, dossier_payload_dict), ...].

    `market_types_filter`, when provided, restricts the snapshot to the
    listed MARKET_TYPE values (case-insensitive). Used to scope daily
    runs to STOCK only since MIP does not currently trade ETF or FX.
    """
    sql_filter = ""
    if symbols_filter:
        in_list = ",".join("'" + s.replace("'", "''").upper() + "'" for s in symbols_filter)
        sql_filter = f" AND UPPER(SYMBOL) IN ({in_list})"
    if market_types_filter:
        mt_list = ",".join(
            "'" + m.replace("'", "''").upper() + "'" for m in market_types_filter
        )
        sql_filter += f" AND UPPER(MARKET_TYPE) IN ({mt_list})"

    insert_sql = f"""
    INSERT INTO MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT (
        RUN_ID, DOSSIER_KEY, AS_OF_DATE, PORTFOLIO_ID,
        SYMBOL, MARKET_TYPE,
        CURRENT_PRICE, CURRENT_PRICE_SOURCE, CURRENT_PRICE_DATE,
        PRIMARY_EVIDENCE_SETUP_EVENT_ID,
        SHORT_RESEARCH_VISIBLE, SHORT_LIVE_ENABLED, FX_LIVE_ENABLED,
        DATA_QUALITY_FLAGS, BOARD_WARNING_FLAGS,
        DOSSIER_PAYLOAD_JSON, PAYLOAD_HASH
    )
    SELECT
        %(run_id)s,
        SYMBOL || '|' || MARKET_TYPE || '|' || %(as_of)s,
        %(as_of)s,
        %(portfolio_id)s,
        SYMBOL,
        MARKET_TYPE,
        TRY_TO_DOUBLE(DOSSIER_PAYLOAD_JSON:price:current_price::STRING),
        DOSSIER_PAYLOAD_JSON:price:price_source::STRING,
        TRY_TO_DATE(DOSSIER_PAYLOAD_JSON:price:price_date::STRING),
        TRY_TO_NUMBER(DOSSIER_PAYLOAD_JSON:primary_evidence_setup_event_id::STRING),
        DOSSIER_PAYLOAD_JSON:policy:short_research_visible::BOOLEAN,
        DOSSIER_PAYLOAD_JSON:policy:short_live_enabled::BOOLEAN,
        DOSSIER_PAYLOAD_JSON:policy:fx_live_enabled::BOOLEAN,
        DOSSIER_PAYLOAD_JSON:warnings:data_quality,
        DOSSIER_PAYLOAD_JSON:warnings:board,
        DOSSIER_PAYLOAD_JSON,
        SHA2(TO_VARCHAR(DOSSIER_PAYLOAD_JSON), 256)
    FROM MIP.MART.V_PROPOSAL_BOARD_SYMBOL_DOSSIER
    WHERE 1=1
    {sql_filter}
    """
    cur.execute(insert_sql, {"run_id": run_id, "as_of": as_of_date, "portfolio_id": portfolio_id})

    # Load back the dossiers we just inserted, including the payload.
    cur.execute("""
        SELECT DOSSIER_ID, SYMBOL, COALESCE(MARKET_TYPE,'EQUITY') AS MARKET_TYPE,
               DOSSIER_PAYLOAD_JSON
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT
        WHERE RUN_ID = %(run_id)s
        ORDER BY DOSSIER_ID
    """, {"run_id": run_id})

    rows = cur.fetchall()
    out: List[Tuple[int, str, str, Dict[str, Any]]] = []
    for r in rows:
        did, sym, mkt, payload = r[0], r[1], r[2], r[3]
        if isinstance(payload, str):
            payload = json.loads(payload)
        out.append((did, sym, mkt, payload))
    return out


def _stage_pack_cache(
    cur,
    run_id: str,
    rows: List[Tuple[int, str, str, Dict[str, Any]]],
) -> None:
    if not rows:
        return
    cur.execute(
        "DELETE FROM MIP.APP.PROPOSAL_BOARD_DOSSIER_PACK_CACHE "
        "WHERE EXPIRES_AT < CURRENT_TIMESTAMP()"
    )
    insert_sql = """
        INSERT INTO MIP.APP.PROPOSAL_BOARD_DOSSIER_PACK_CACHE
            (RUN_ID, DOSSIER_ID, SYMBOL, MARKET_TYPE, AS_OF_DATE,
             PACK_JSON, PACK_HASH, EXPIRES_AT)
        SELECT %(run_id)s, %(did)s, %(sym)s, %(mkt)s, CURRENT_DATE(),
               PARSE_JSON(%(pack_json)s), %(pack_hash)s,
               DATEADD(hour, %(ttl_hours)s, CURRENT_TIMESTAMP())
    """
    for did, sym, mkt, payload in rows:
        pack_json = _jdump(payload)
        pack_hash = hashlib.sha256(pack_json.encode("utf-8")).hexdigest()
        cur.execute(insert_sql, {
            "run_id": run_id,
            "did": did,
            "sym": sym,
            "mkt": mkt,
            "pack_json": pack_json,
            "pack_hash": pack_hash,
            "ttl_hours": _CACHE_TTL_HOURS,
        })


# ---------------------------------------------------------------------------
# Stage 1 — specialist agents (parallel per dossier)
# ---------------------------------------------------------------------------

def _user_message_for_specialist(role: str, run_id: str, dossier_id: int, symbol: str) -> str:
    return (
        f"Run the {role} specialist analysis for symbol={symbol}, "
        f"run_id={run_id}, dossier_id={dossier_id}. "
        "You MUST call the get_evidence_slice tool with role_name='" + role + "' "
        "for every slice you reason from before producing output. "
        "Your output MUST be the JSON object specified in your system prompt. "
        "Do not include prose, explanations, or markdown fences."
    )


async def _run_one_specialist(
    sem: asyncio.Semaphore,
    rest_creds: Tuple[str, str, str],
    role: str,
    run_id: str,
    dossier_id: int,
    symbol: str,
) -> Tuple[str, SpecialistPosition, Dict[str, Any]]:
    """Run one specialist agent and return (role, validated_position, raw_response)."""
    account, user, key_path = rest_creds
    agent_name = _AGENT_OBJECT_NAMES[role]
    user_msg = _user_message_for_specialist(role, run_id, dossier_id, symbol)

    async with sem:
        t0 = time.monotonic()
        try:
            response = await run_agent_object(
                account=account, user=user, private_key_path=key_path,
                agent_name=agent_name,
                messages=[{"role": "user", "content": user_msg}],
                timeout=_AGENT_TIMEOUT_SEC,
            )
        except Exception as e:
            logger.warning("phase4 specialist %s dossier=%s symbol=%s FAILED: %s",
                           role, dossier_id, symbol, e)
            response = {"error": str(e)}
        elapsed = time.monotonic() - t0

    text = extract_agent_text(response) if isinstance(response, dict) and "error" not in response else ""
    parsed = _try_parse_json(text) if text else None
    pos = _validate_specialist(role, parsed)
    if pos.invalid_reason and parsed is None and "error" in response:
        pos.invalid_reason = "AGENT_HTTP_ERROR:" + str(response.get("error"))[:200]

    logger.info(
        "phase4 specialist done role=%s dossier=%s symbol=%s "
        "verdict=%s primary=%s elapsed=%.2fs invalid=%s",
        role, dossier_id, symbol, pos.verdict, pos.primary_reason_code,
        elapsed, pos.invalid_reason,
    )
    return role, pos, response


def _persist_specialist(
    cur, run_id: str, dossier_id: int, pos: SpecialistPosition,
) -> None:
    cur.execute(
        """
        MERGE INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 t
        USING (
          SELECT %(run_id)s AS RUN_ID, %(did)s AS DOSSIER_ID,
                 %(role)s AS AGENT_NAME
        ) s
        ON t.RUN_ID = s.RUN_ID AND t.DOSSIER_ID = s.DOSSIER_ID
           AND t.AGENT_NAME = s.AGENT_NAME
        WHEN MATCHED THEN UPDATE SET
          VERDICT = %(verdict)s,
          PRIMARY_REASON_CODE = %(primary)s,
          SECONDARY_REASON_CODE = %(secondary)s,
          CONFIDENCE = %(conf)s,
          LONG_SCORE = %(long_s)s,
          SHORT_SCORE = %(short_s)s,
          NO_TRADE_SCORE = %(nt_s)s,
          RATIONALE_TEXT = %(rationale)s,
          STRUCTURED_OUTPUT_JSON = PARSE_JSON(%(structured)s)
        WHEN NOT MATCHED THEN INSERT (
          RUN_ID, DOSSIER_ID, AGENT_NAME, VERDICT,
          PRIMARY_REASON_CODE, SECONDARY_REASON_CODE,
          CONFIDENCE, LONG_SCORE, SHORT_SCORE, NO_TRADE_SCORE,
          RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
        ) VALUES (
          %(run_id)s, %(did)s, %(role)s, %(verdict)s,
          %(primary)s, %(secondary)s,
          %(conf)s, %(long_s)s, %(short_s)s, %(nt_s)s,
          %(rationale)s, PARSE_JSON(%(structured)s)
        )
        """,
        {
            "run_id": run_id,
            "did": dossier_id,
            "role": pos.role,
            "verdict": pos.verdict or "INVALID",
            "primary": pos.primary_reason_code or "AGENT_OUTPUT_INVALID",
            "secondary": pos.secondary_reason_code,
            "conf": pos.confidence,
            "long_s": pos.long_score,
            "short_s": pos.short_score,
            "nt_s": pos.no_trade_score,
            "rationale": _truncate(pos.rationale, 4000),
            "structured": _jdump(pos.structured_output or {"invalid_reason": pos.invalid_reason}),
        },
    )


def _persist_invalid_agent(
    cur, run_id: str, dossier_id: int, role: str, reason: str, raw: Any,
) -> None:
    cur.execute(
        """
        INSERT INTO MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR
            (RUN_ID, CANDIDATE_ID, AGENT_NAME, ERROR_TYPE,
             RAW_OUTPUT_JSON, ERROR_MESSAGE)
        SELECT %(run_id)s, %(did)s, %(role)s, 'AGENT_OUTPUT_INVALID',
               PARSE_JSON(%(raw)s), %(reason)s
        """,
        {
            "run_id": run_id,
            "did": dossier_id,
            "role": role,
            "raw": _jdump(raw if raw is not None else {}),
            "reason": _truncate(reason, 4000),
        },
    )


# ---------------------------------------------------------------------------
# Stages 3 + 4 — challenge / revision
# ---------------------------------------------------------------------------

def _challenge_user_message(
    conflict: ConflictEntry,
    target_role: str,
    target_position: SpecialistPosition,
    challenger_position: SpecialistPosition,
    run_id: str,
    dossier_id: int,
    symbol: str,
    evidence_context: Dict[str, Any],
) -> str:
    """
    Build the user message for the revision call.
    The objectless AGENT_RUN endpoint is invoked WITHOUT tools, so all
    necessary evidence is embedded directly here. The agent's task is to
    MAINTAIN or AMEND its position based on the challenge + the persisted
    target position + the relevant evidence slice payloads.
    """
    return (
        f"You are the {target_role} specialist. Another specialist on the board "
        f"(the {conflict.source_role} agent) has challenged your initial position "
        f"on symbol={symbol}, run_id={run_id}, dossier_id={dossier_id}.\n\n"
        f"Your initial verdict: {target_position.verdict} "
        f"(primary_reason={target_position.primary_reason_code}, "
        f"confidence={target_position.confidence}).\n"
        f"Your initial rationale: {target_position.rationale}\n\n"
        f"Your initial structured output:\n"
        f"{json.dumps(target_position.structured_output, default=str)[:4000]}\n\n"
        f"Challenger ({conflict.source_role}) verdict: {challenger_position.verdict} "
        f"(primary_reason={challenger_position.primary_reason_code}).\n"
        f"Challenger rationale: {challenger_position.rationale}\n\n"
        f"Challenger structured output:\n"
        f"{json.dumps(challenger_position.structured_output, default=str)[:4000]}\n\n"
        f"Challenge topic: {conflict.topic}\n"
        f"Disagreement type: {conflict.disagreement_type}\n"
        f"Challenge text: {conflict.challenge_text}\n\n"
        f"Relevant dossier evidence for re-examination "
        f"(this is your full evidence — no tool calls available in this revision turn):\n"
        f"{json.dumps(evidence_context, default=str)[:8000]}\n\n"
        "REQUIRED RESPONSE: Re-examine the evidence above. "
        "Then return ONE of:\n"
        " (a) MAINTAIN: keep your verdict; explain why the challenge does not change it.\n"
        " (b) AMEND: change your verdict to better fit the evidence.\n\n"
        "Return ONLY the JSON object specified in your system prompt. "
        "Set evidence_used to the slice names you re-examined. "
        "Do NOT include prose, explanations, or markdown fences. "
        "If you AMEND, the new verdict / primary_reason_code MUST be in the same allowlist "
        "you originally chose from."
    )


def _objectless_system_for_role(role: str) -> str:
    """Reuse the same role specification language for the objectless revision call."""
    if role == "MARKET_STRUCTURE":
        verdict_list = sorted(_ALLOWED_VERDICTS["MARKET_STRUCTURE"])
        primary_list = sorted(_ALLOWED_PRIMARY_REASON["MARKET_STRUCTURE"])
    elif role == "LEVEL_PRICE_ACTION":
        verdict_list = sorted(_ALLOWED_VERDICTS["LEVEL_PRICE_ACTION"])
        primary_list = sorted(_ALLOWED_PRIMARY_REASON["LEVEL_PRICE_ACTION"])
    elif role == "THESIS":
        verdict_list = sorted(_ALLOWED_VERDICTS["THESIS"])
        primary_list = sorted(_ALLOWED_PRIMARY_REASON["THESIS"])
    elif role == "HISTORICAL_EVIDENCE":
        verdict_list = sorted(_ALLOWED_VERDICTS["HISTORICAL_EVIDENCE"])
        primary_list = sorted(_ALLOWED_PRIMARY_REASON["HISTORICAL_EVIDENCE"])
    else:
        verdict_list = sorted(_ALLOWED_VERDICTS["RISK_EXECUTION"])
        primary_list = sorted(_ALLOWED_PRIMARY_REASON["RISK_EXECUTION"])

    return (
        f"You are the {role} specialist on the Phase 4 Agentic Proposal Board, "
        "responding to a challenge from a peer specialist. "
        "Use the get_evidence_slice tool to re-examine the dossier. "
        "Return JSON ONLY in this shape (no prose, no fences): "
        "{\"role\":\"" + role + "\",\"verdict\":\"<one of " + ", ".join(verdict_list) + ">\","
        "\"primary_reason_code\":\"<one of " + ", ".join(primary_list) + ">\","
        "\"secondary_reason_code\":null,\"confidence\":<float 0-1>,"
        "\"long_score\":<float 0-1>,\"short_score\":<float 0-1>,"
        "\"no_trade_score\":<float 0-1>,"
        "\"rationale\":\"<2-4 sentences citing evidence>\","
        "\"evidence_used\":[\"<slice_name>\",...]}"
    )


# Role -> slices the role is allowed to read (mirrors GET_PHASE4_DOSSIER_SLICE).
# Used to build the offline evidence context embedded in revision prompts.
_ROLE_SLICE_MAP = {
    "MARKET_STRUCTURE": [
        "identity", "price", "recent_bars", "candle_sequence",
        "recent_price_action_summary", "structure", "regime",
        "structural_timeline_summary", "candle_psychology",
        "actionability_context", "market_structure_map",
    ],
    "LEVEL_PRICE_ACTION": [
        "identity", "price", "recent_bars", "candle_sequence",
        "recent_price_action_summary", "levels",
        "candle_psychology", "actionability_context", "market_structure_map",
    ],
    "THESIS": [
        "identity", "price", "structure", "regime", "levels",
        "long_pattern_signs", "short_pattern_signs",
        "setup_events_evidence_only", "recent_price_action_summary",
        "structural_timeline_summary", "actionability_context",
        "market_structure_map",
    ],
    "HISTORICAL_EVIDENCE": [
        "identity", "history", "setup_events_evidence_only",
        "invalidation_evidence", "memory",
    ],
    "RISK_EXECUTION": [
        "identity", "price", "levels", "structure", "regime",
        "policy", "memory", "invalidation_evidence",
    ],
}


def _slice_payload_for_role(role: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return only the dossier payload keys the role may read."""
    allowed = _ROLE_SLICE_MAP.get(role, [])
    out: Dict[str, Any] = {}
    if not isinstance(payload, dict):
        return out
    for key in allowed:
        if key in payload:
            out[key] = payload[key]
    return out


def _persist_interaction(
    cur,
    run_id: str,
    dossier_id: int,
    source_agent: str,
    target_agent: str,
    topic: str,
    disagreement_type: str,
    disagreement_text: Optional[str],
    response_text: Optional[str],
    resolved_flag: bool,
) -> None:
    cur.execute(
        """
        INSERT INTO MIP.APP.PROPOSAL_BOARD_INTERACTION_V2
            (RUN_ID, DOSSIER_ID, SOURCE_AGENT, TARGET_AGENT,
             TOPIC, DISAGREEMENT_TYPE,
             DISAGREEMENT_TEXT, RESPONSE_TEXT, RESOLVED_FLAG)
        VALUES (
            %(run_id)s, %(did)s, %(src)s, %(tgt)s,
            %(topic)s, %(dt)s,
            %(disagreement)s, %(response)s, %(resolved)s
        )
        """,
        {
            "run_id": run_id, "did": dossier_id,
            "src": source_agent, "tgt": target_agent,
            "topic": _truncate(topic, 120),
            "dt": _truncate(disagreement_type, 80),
            "disagreement": _truncate(disagreement_text, 4000),
            "response": _truncate(response_text, 4000),
            "resolved": resolved_flag,
        },
    )


# ---------------------------------------------------------------------------
# Stage 5 — chair
# ---------------------------------------------------------------------------

def _chair_user_message(
    run_id: str,
    dossier_id: int,
    symbol: str,
    positions: Dict[str, SpecialistPosition],
    interactions: List[Dict[str, Any]],
    short_live_enabled: bool,
    fx_live_enabled: bool,
    market_type: str,
    primary_evidence_setup_event_id: Optional[int],
) -> str:
    spec_summary = []
    for role in _REQUIRED_ROLES:
        p = positions.get(role)
        if p is None:
            continue
        spec_summary.append({
            "role": role,
            "verdict": p.verdict,
            "primary_reason_code": p.primary_reason_code,
            "confidence": p.confidence,
            "long_score": p.long_score,
            "short_score": p.short_score,
            "no_trade_score": p.no_trade_score,
            "rationale": p.rationale,
        })

    payload = {
        "run_id": run_id,
        "dossier_id": dossier_id,
        "symbol": symbol,
        "market_type": market_type,
        "policy_flags": {
            "short_live_enabled": bool(short_live_enabled),
            "fx_live_enabled": bool(fx_live_enabled),
        },
        "primary_evidence_setup_event_id": primary_evidence_setup_event_id,
        "specialist_positions": spec_summary,
        "interactions_summary": interactions,
    }
    return (
        f"You are the CHAIR / Portfolio PM. Read the persisted specialist positions "
        f"and any challenge/revision interactions and author the FINAL trade decision "
        f"for symbol={symbol}, run_id={run_id}, dossier_id={dossier_id}. "
        "Use the get_evidence_slice tool with role_name='CHAIR' for any slice you need. "
        "Authoritative input below. Apply the HARD RULES from your system prompt. "
        "Return ONLY the JSON object specified in your system prompt. "
        "No prose. No markdown fences.\n\n"
        f"BOARD_INPUT_JSON: {json.dumps(payload, default=str)}"
    )


@dataclass
class ChairOutput:
    final_action: str
    final_direction: str
    primary_reason_code: str
    secondary_reason_code: Optional[str]
    confidence: Optional[float]
    long_score: Optional[float]
    short_score: Optional[float]
    no_trade_score: Optional[float]
    final_thesis: str
    why_not_opposite: str
    why_not_no_trade: str
    risk_treatment: str
    rationale: str
    unresolved_disagreement: bool
    proposed_trade_config: Dict[str, Any]
    structured_output: Dict[str, Any]
    # Phase 4 taxonomy v2.
    thesis_health: str = ""
    prior_thesis_reference: Optional[Dict[str, Any]] = None
    invalid_reason: Optional[str] = None


def _chair_structure_summary_output_invalid(raw: Dict[str, Any]) -> Optional[str]:
    """Require market_structure_read + body/wick line + decision line for gated actions."""
    final_action = str(raw.get("final_action") or "").strip().upper()
    if final_action not in _CHAIR_STRUCTURE_SUMMARY_REQUIRED_ACTIONS:
        return None
    msr = raw.get("market_structure_read")
    if not isinstance(msr, dict):
        return "MARKET_STRUCTURE_READ_MISSING_OR_NON_OBJECT"
    for key in _CHAIR_MARKET_STRUCTURE_READ_KEYS:
        if key not in msr:
            return f"MARKET_STRUCTURE_READ_MISSING_KEY:{key}"
        val = msr[key]
        if key in ("bos_body_close_confirmed", "choch_detected"):
            if not isinstance(val, bool):
                return f"MARKET_STRUCTURE_READ_BOOL_REQUIRED:{key}"
        elif val is None or not str(val).strip():
            return f"MARKET_STRUCTURE_READ_EMPTY:{key}"
    bwk = raw.get("body_wick_break_read")
    if not isinstance(bwk, str) or not bwk.strip():
        return "BODY_WICK_BREAK_READ_REQUIRED_NON_EMPTY"
    sdr = raw.get("structure_decision_reason")
    if not isinstance(sdr, str) or not sdr.strip():
        return "STRUCTURE_DECISION_REASON_REQUIRED_NON_EMPTY"
    return None


def _validate_chair(raw: Optional[Dict[str, Any]]) -> ChairOutput:
    if not isinstance(raw, dict):
        return ChairOutput(
            final_action="", final_direction="", primary_reason_code="",
            secondary_reason_code=None, confidence=None, long_score=None,
            short_score=None, no_trade_score=None, final_thesis="",
            why_not_opposite="", why_not_no_trade="", risk_treatment="",
            rationale="", unresolved_disagreement=False,
            proposed_trade_config={}, structured_output={"raw": str(raw)},
            invalid_reason="MISSING_OR_NON_OBJECT_JSON",
        )
    final_action = str(raw.get("final_action") or "").strip().upper()
    final_direction = str(raw.get("final_direction") or "NONE").strip().upper()
    primary = str(raw.get("primary_reason_code") or "").strip().upper()
    secondary_v = raw.get("secondary_reason_code")
    secondary = str(secondary_v).strip().upper() if secondary_v else None
    config = raw.get("proposed_trade_config") if isinstance(raw.get("proposed_trade_config"), dict) else {}
    # Phase 4 taxonomy v2: thesis_health + prior_thesis_reference are required.
    thesis_health = str(raw.get("thesis_health") or "").strip().upper()
    prior_ref_raw = raw.get("prior_thesis_reference")
    prior_ref = prior_ref_raw if isinstance(prior_ref_raw, dict) else None

    invalid: Optional[str] = None
    if final_action not in _CHAIR_ALLOWED_FINAL_ACTIONS:
        invalid = f"FINAL_ACTION_NOT_ALLOWED:{final_action}"
    elif final_direction not in _CHAIR_ALLOWED_FINAL_DIRECTIONS:
        invalid = f"FINAL_DIRECTION_NOT_ALLOWED:{final_direction}"
    elif primary not in _CHAIR_ALLOWED_PRIMARY_REASON:
        invalid = f"PRIMARY_REASON_NOT_ALLOWED:{primary}"
    elif thesis_health and thesis_health not in _CHAIR_ALLOWED_THESIS_HEALTH:
        invalid = f"THESIS_HEALTH_NOT_ALLOWED:{thesis_health}"
    elif final_action in _REQUIRES_PRIOR_THESIS_REFERENCE and not prior_ref:
        invalid = f"PRIOR_THESIS_REFERENCE_REQUIRED:{final_action}"
    else:
        thesis_label = str(config.get("thesis_label") or "").strip()
        if final_action in _AGENTIC_THESIS_LABEL_REQUIRED and not thesis_label.upper().startswith("AGENTIC_"):
            invalid = f"THESIS_LABEL_NOT_AGENTIC:{thesis_label[:40]}"
    if invalid is None:
        msm_invalid = _chair_structure_summary_output_invalid(raw)
        if msm_invalid:
            invalid = msm_invalid

    return ChairOutput(
        final_action=final_action,
        final_direction=final_direction,
        primary_reason_code=primary,
        secondary_reason_code=secondary,
        confidence=_to_float(raw.get("confidence")),
        long_score=_to_float(raw.get("long_score")),
        short_score=_to_float(raw.get("short_score")),
        no_trade_score=_to_float(raw.get("no_trade_score")),
        final_thesis=str(raw.get("final_thesis") or ""),
        why_not_opposite=str(raw.get("why_not_opposite") or ""),
        why_not_no_trade=str(raw.get("why_not_no_trade") or ""),
        risk_treatment=str(raw.get("risk_treatment") or ""),
        rationale=str(raw.get("rationale") or ""),
        unresolved_disagreement=bool(raw.get("unresolved_disagreement") or False),
        proposed_trade_config=config or {},
        thesis_health=thesis_health,
        prior_thesis_reference=prior_ref,
        structured_output=raw,
        invalid_reason=invalid,
    )


# ---------------------------------------------------------------------------
# Per-dossier orchestration
# ---------------------------------------------------------------------------

@dataclass
class DossierResult:
    dossier_id: int
    symbol: str
    market_type: str
    primary_evidence_setup_event_id: Optional[int]
    short_live_enabled: bool
    fx_live_enabled: bool
    short_research_visible: bool
    final_positions: Dict[str, SpecialistPosition] = field(default_factory=dict)
    interactions: List[Dict[str, Any]] = field(default_factory=list)
    chair: Optional[ChairOutput] = None
    is_valid: bool = False
    invalid_reason: Optional[str] = None


async def _orchestrate_dossier(
    rest_creds: Tuple[str, str, str],
    conn_factory,
    run_id: str,
    dossier_id: int,
    symbol: str,
    market_type: str,
    payload: Dict[str, Any],
    spec_concurrency: int,
    max_rounds: int,
) -> DossierResult:
    dossier_payload = payload if isinstance(payload, dict) else {}
    primary_evidence_setup_event_id = None
    try:
        primary_evidence_setup_event_id = (
            payload.get("primary_evidence_setup_event_id") if isinstance(payload, dict) else None
        )
        if primary_evidence_setup_event_id is not None:
            primary_evidence_setup_event_id = int(primary_evidence_setup_event_id)
    except Exception:
        primary_evidence_setup_event_id = None
    policy = (payload.get("policy") or {}) if isinstance(payload, dict) else {}
    res = DossierResult(
        dossier_id=dossier_id,
        symbol=symbol,
        market_type=market_type,
        primary_evidence_setup_event_id=primary_evidence_setup_event_id,
        short_live_enabled=bool(policy.get("short_live_enabled")),
        fx_live_enabled=bool(policy.get("fx_live_enabled")),
        short_research_visible=bool(policy.get("short_research_visible")),
    )

    # Stage 1 — parallel specialists
    sem = asyncio.Semaphore(spec_concurrency)
    coros = [
        _run_one_specialist(sem, rest_creds, role, run_id, dossier_id, symbol)
        for role in _REQUIRED_ROLES
    ]
    raw_positions: Dict[str, SpecialistPosition] = {}
    raw_responses: Dict[str, Dict[str, Any]] = {}

    for fut in asyncio.as_completed(coros):
        role, pos, response = await fut
        raw_positions[role] = pos
        raw_responses[role] = response

    # Persist initial positions (or invalid markers)
    def _persist_initial(cn):
        cur = cn.cursor()
        try:
            for role, pos in raw_positions.items():
                if pos.invalid_reason:
                    _persist_invalid_agent(
                        cur, run_id, dossier_id, role, pos.invalid_reason,
                        raw_responses.get(role),
                    )
                else:
                    _persist_specialist(cur, run_id, dossier_id, pos)
            cn.commit()
        finally:
            cur.close()
    await asyncio.to_thread(lambda: _persist_initial(conn_factory()))

    invalid_roles = [r for r, p in raw_positions.items() if p.invalid_reason]
    if invalid_roles:
        res.final_positions = raw_positions
        res.is_valid = False
        res.invalid_reason = "INVALID_SPECIALISTS:" + ",".join(invalid_roles)
        return res

    # Stages 2/3/4 — challenge/revision rounds
    current_positions: Dict[str, SpecialistPosition] = dict(raw_positions)
    rounds_done = 0
    interactions: List[Dict[str, Any]] = []

    while rounds_done < max_rounds:
        conflicts = detect_conflicts({r: p.structured_output for r, p in current_positions.items()})
        if not conflicts:
            break

        for conflict in conflicts:
            target_pos = current_positions.get(conflict.target_role)
            challenger_pos = current_positions.get(conflict.source_role)
            if target_pos is None or challenger_pos is None:
                continue

            # Persist the challenge row first (RESOLVED_FLAG=False until revision)
            def _persist_challenge(cn):
                cur = cn.cursor()
                try:
                    _persist_interaction(
                        cur, run_id, dossier_id,
                        source_agent=_AGENT_OBJECT_NAMES[conflict.source_role],
                        target_agent=_AGENT_OBJECT_NAMES[conflict.target_role],
                        topic=conflict.topic,
                        disagreement_type=conflict.disagreement_type,
                        disagreement_text=conflict.challenge_text,
                        response_text=None,
                        resolved_flag=False,
                    )
                    cn.commit()
                finally:
                    cur.close()
            await asyncio.to_thread(lambda: _persist_challenge(conn_factory()))

            # Stage 4: ask the challenged role to respond. Use objectless
            # AGENT_RUN so we can deliver dynamic challenge content as
            # instructions. Do NOT pass tools — embed the relevant dossier
            # evidence directly in the user message instead. This mirrors the
            # proven Shadow Board revision pattern.
            sys_prompt = _objectless_system_for_role(conflict.target_role)
            evidence_context = _slice_payload_for_role(conflict.target_role, dossier_payload)
            user_msg = _challenge_user_message(
                conflict, conflict.target_role, target_pos, challenger_pos,
                run_id, dossier_id, symbol, evidence_context,
            )
            account, user, key_path = rest_creds
            try:
                response = await run_agent_objectless(
                    account=account, user=user, private_key_path=key_path,
                    model=_OBJECTLESS_MODEL,
                    system_prompt=sys_prompt,
                    user_message=user_msg,
                    timeout=_OBJECTLESS_TIMEOUT_SEC,
                )
                response_text = extract_agent_text(response)
            except Exception as e:
                logger.warning(
                    "phase4 revision call failed role=%s dossier=%s: %s",
                    conflict.target_role, dossier_id, repr(e),
                )
                response_text = ""
                response = {"error": str(e)}

            revised_raw = _try_parse_json(response_text) if response_text else None
            revised_pos = _validate_specialist(conflict.target_role, revised_raw)

            # If revision is INVALID, keep the prior position (do not corrupt).
            if revised_pos.invalid_reason:
                # Persist a revision interaction row marking the failure.
                def _persist_failed_revision(cn):
                    cur = cn.cursor()
                    try:
                        _persist_interaction(
                            cur, run_id, dossier_id,
                            source_agent=_AGENT_OBJECT_NAMES[conflict.target_role],
                            target_agent=_AGENT_OBJECT_NAMES[conflict.source_role],
                            topic=conflict.topic,
                            disagreement_type=conflict.disagreement_type,
                            disagreement_text=None,
                            response_text=("REVISION_INVALID: " + (revised_pos.invalid_reason or "")),
                            resolved_flag=False,
                        )
                        cn.commit()
                    finally:
                        cur.close()
                await asyncio.to_thread(lambda: _persist_failed_revision(conn_factory()))
                interactions.append({
                    "topic": conflict.topic,
                    "disagreement_type": conflict.disagreement_type,
                    "challenger": conflict.source_role,
                    "target": conflict.target_role,
                    "outcome": "REVISION_INVALID",
                    "challenge_text": conflict.challenge_text,
                })
                continue

            # Update current positions; persist revision interaction + AGENT_OUTCOME_V2 update.
            verdict_changed = (revised_pos.verdict != target_pos.verdict)
            current_positions[conflict.target_role] = revised_pos

            response_summary = (
                f"REVISION verdict={revised_pos.verdict} "
                f"primary={revised_pos.primary_reason_code} "
                f"changed={verdict_changed} "
                f"rationale={revised_pos.rationale[:1500]}"
            )

            def _persist_revision_and_update(cn):
                cur = cn.cursor()
                try:
                    _persist_interaction(
                        cur, run_id, dossier_id,
                        source_agent=_AGENT_OBJECT_NAMES[conflict.target_role],
                        target_agent=_AGENT_OBJECT_NAMES[conflict.source_role],
                        topic=conflict.topic,
                        disagreement_type=conflict.disagreement_type,
                        disagreement_text=None,
                        response_text=response_summary,
                        resolved_flag=(not verdict_changed),
                    )
                    _persist_specialist(cur, run_id, dossier_id, revised_pos)
                    cn.commit()
                finally:
                    cur.close()
            await asyncio.to_thread(lambda: _persist_revision_and_update(conn_factory()))

            interactions.append({
                "topic": conflict.topic,
                "disagreement_type": conflict.disagreement_type,
                "challenger": conflict.source_role,
                "target": conflict.target_role,
                "outcome": "MAINTAIN" if not verdict_changed else "AMEND",
                "challenge_text": conflict.challenge_text,
                "revised_verdict": revised_pos.verdict,
                "revised_primary": revised_pos.primary_reason_code,
            })

        rounds_done += 1
        # Loop again only if revisions changed verdicts (otherwise no new conflicts possible).
        drifted = detect_stance_drift(
            {r: p.structured_output for r, p in raw_positions.items()},
            {r: p.structured_output for r, p in current_positions.items()},
        )
        if not drifted:
            break

    res.final_positions = current_positions
    res.interactions = interactions

    # Stage 5: chair (only after all 5 specialists are durable + valid)
    chair_user_msg = _chair_user_message(
        run_id, dossier_id, symbol, current_positions, interactions,
        res.short_live_enabled, res.fx_live_enabled, market_type,
        res.primary_evidence_setup_event_id,
    )
    account, user, key_path = rest_creds
    try:
        chair_resp = await run_agent_object(
            account=account, user=user, private_key_path=key_path,
            agent_name=_CHAIR_AGENT_NAME,
            messages=[{"role": "user", "content": chair_user_msg}],
            timeout=_AGENT_TIMEOUT_SEC,
        )
        chair_text = extract_agent_text(chair_resp)
    except Exception as e:
        logger.warning("phase4 chair call failed dossier=%s: %s", dossier_id, e)
        chair_text = ""
        chair_resp = {"error": str(e)}

    chair_parsed = _try_parse_json(chair_text) if chair_text else None
    chair = _validate_chair(chair_parsed)
    res.chair = chair

    if chair.invalid_reason:
        res.is_valid = False
        res.invalid_reason = "INVALID_CHAIR:" + chair.invalid_reason
        # Still persist the raw chair to OUTPUT_ERROR for audit.
        def _persist_chair_error(cn):
            cur = cn.cursor()
            try:
                _persist_invalid_agent(
                    cur, run_id, dossier_id, _CHAIR_AGENT_NAME,
                    chair.invalid_reason or "INVALID_CHAIR",
                    chair.structured_output or chair_resp,
                )
                cn.commit()
            finally:
                cur.close()
        await asyncio.to_thread(lambda: _persist_chair_error(conn_factory()))
        return res

    res.is_valid = True

    # Audit: ACTIONABILITY_ESCALATION. The chair is allowed to escalate from a
    # specialist's WATCH/WAIT verdict to a PROPOSE action, but we record an
    # interaction so the operator can review the override. We do NOT block the
    # run. Note that this is informational only — direction conflict already
    # triggers DIRECTION_DISAGREEMENT.
    if chair.final_action in {"PROPOSE_LONG", "PROPOSE_SHORT"}:
        watch_or_wait_specialists: List[Tuple[str, str]] = []
        for role, pos in current_positions.items():
            v = (pos.verdict or "").upper()
            if role == "THESIS" and v in {"WATCH_LONG", "WATCH_SHORT"}:
                watch_or_wait_specialists.append((role, v))
            elif role == "RISK_EXECUTION" and v in {"WAIT_CONFIRMATION", "RESEARCH_ONLY"}:
                watch_or_wait_specialists.append((role, v))
            elif role == "LEVEL_PRICE_ACTION" and v == "WAIT_CONFIRMATION":
                watch_or_wait_specialists.append((role, v))
        if watch_or_wait_specialists:
            esc_text = (
                f"Chair escalated to {chair.final_action} while at least one specialist "
                "was in WATCH or WAIT_FOR_CONFIRMATION: "
                + ", ".join(f"{r}={v}" for r, v in watch_or_wait_specialists)
                + ". Chair must defend why entry is actionable now and what invalidates "
                "the specialist's caution. Recorded for audit; this is not a blocking conflict."
            )
            esc_response = (
                f"chair_primary_reason={chair.primary_reason_code} | "
                f"final_thesis={chair.final_thesis[:1500]}"
            )

            def _persist_actionability_escalation(cn):
                cur = cn.cursor()
                try:
                    _persist_interaction(
                        cur, run_id, dossier_id,
                        source_agent=_CHAIR_AGENT_NAME,
                        target_agent=",".join(_AGENT_OBJECT_NAMES.get(r, r)
                                              for r, _ in watch_or_wait_specialists),
                        topic="ACTIONABILITY_ESCALATION",
                        disagreement_type="ACTIONABILITY_ESCALATION",
                        disagreement_text=_truncate(esc_text, 4000),
                        response_text=_truncate(esc_response, 4000),
                        resolved_flag=True,
                    )
                    cn.commit()
                finally:
                    cur.close()

            try:
                await asyncio.to_thread(
                    lambda: _persist_actionability_escalation(conn_factory()),
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "phase4 actionability_escalation persist failed dossier=%s: %s",
                    dossier_id, e,
                )

            res.interactions.append({
                "topic": "ACTIONABILITY_ESCALATION",
                "disagreement_type": "ACTIONABILITY_ESCALATION",
                "challenger": "CHAIR",
                "target": ",".join(r for r, _ in watch_or_wait_specialists),
                "outcome": "AUDIT_RECORDED",
                "challenge_text": esc_text,
                "specialist_states": dict(watch_or_wait_specialists),
                "chair_action": chair.final_action,
            })

    # Phase 4 evidence-hardening v2: MISSING_STRUCTURAL_EVIDENCE diagnostic.
    # Non-blocking. If the Chair publishes a directional proposal but its
    # `evidence_used` list does not include the new structural slices,
    # persist an audit interaction so the operator can spot prompt-contract
    # drift or a Chair that ignored the new evidence-hardening contract.
    if chair.final_action in {
        "PROPOSE_LONG",
        "PROPOSE_SHORT",
        "WATCH_LONG_FAILURE",
        "WATCH_SHORT_FAILURE",
        "WAIT_FOR_CONFIRMATION",
    }:
        evidence_used_raw = chair.structured_output.get("evidence_used") \
            if isinstance(chair.structured_output, dict) else None
        evidence_used: List[str] = []
        if isinstance(evidence_used_raw, list):
            evidence_used = [str(x).strip().lower() for x in evidence_used_raw if isinstance(x, (str, int, float))]
        required_structural_slices = {
            "structural_timeline_summary",
            "candle_psychology",
            "actionability_context",
            "market_structure_map",
        }
        missing_slices = sorted(required_structural_slices - set(evidence_used))
        if missing_slices:
            diag_text = (
                f"Chair returned {chair.final_action} but did not list required "
                "Phase 4 structural evidence slices in evidence_used: "
                + ", ".join(missing_slices)
                + ". Required by phase4_structural_v2 evidence contract. "
                "Recorded for audit; this is informational and non-blocking."
            )
            diag_response = (
                f"chair_primary_reason={chair.primary_reason_code} | "
                f"evidence_used={','.join(evidence_used) if evidence_used else 'NONE'}"
            )

            def _persist_missing_structural_evidence(cn):
                cur = cn.cursor()
                try:
                    _persist_interaction(
                        cur, run_id, dossier_id,
                        source_agent=_CHAIR_AGENT_NAME,
                        target_agent=_CHAIR_AGENT_NAME,
                        topic="MISSING_STRUCTURAL_EVIDENCE",
                        disagreement_type="MISSING_STRUCTURAL_EVIDENCE",
                        disagreement_text=_truncate(diag_text, 4000),
                        response_text=_truncate(diag_response, 4000),
                        resolved_flag=True,
                    )
                    cn.commit()
                finally:
                    cur.close()

            try:
                await asyncio.to_thread(
                    lambda: _persist_missing_structural_evidence(conn_factory()),
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "phase4 missing_structural_evidence persist failed dossier=%s: %s",
                    dossier_id, e,
                )

            res.interactions.append({
                "topic": "MISSING_STRUCTURAL_EVIDENCE",
                "disagreement_type": "MISSING_STRUCTURAL_EVIDENCE",
                "challenger": "CHAIR",
                "target": "CHAIR",
                "outcome": "AUDIT_RECORDED",
                "challenge_text": diag_text,
                "missing_slices": missing_slices,
                "evidence_used": evidence_used,
                "chair_action": chair.final_action,
                "evidence_contract_version": "phase4_structural_v2",
            })

    # ------------------------------------------------------------------
    # Phase 4 taxonomy v2: WEAK_SHORT_EVIDENCE diagnostic.
    # Non-blocking. Surface short verdicts that do not satisfy the
    # DOMINANT SHORT EVIDENCE RULE (>=3 short-leaning specialists,
    # continuation_quality REJECTED, rejection cluster present, no
    # bullish counter-cluster). Symmetric for long.
    # ------------------------------------------------------------------
    _SHORT_LEANING_VERDICTS = {
        "SHORT_LOCATION", "SHORT_THESIS", "WATCH_SHORT",
        "SHORT_SUPPORTIVE", "RESISTANCE_REJECTION",
    }
    _LONG_LEANING_VERDICTS = {
        "LONG_LOCATION", "LONG_THESIS", "WATCH_LONG",
        "LONG_SUPPORTIVE", "TREND_UP", "BREAKOUT_ATTEMPT", "SUPPORT_BOUNCE",
    }
    _BULLISH_COUNTER_CLUSTERS = {
        "LOWER_WICK_ACCUMULATION", "BREAKOUT_FOLLOW_THROUGH",
        "ORDERLY_PULLBACK",
    }
    _BEARISH_COUNTER_CLUSTERS = {
        "UPPER_ZONE_REJECTION_CLUSTER", "SELLER_PRESSURE_AFTER_ADVANCE",
    }
    _REQUIRED_REJECTION_CLUSTERS = {
        "UPPER_ZONE_REJECTION_CLUSTER", "SELLER_PRESSURE_AFTER_ADVANCE",
    }

    if chair.final_action in {"WATCH_SHORT", "PROPOSE_SHORT", "WATCH_LONG", "PROPOSE_LONG"}:
        is_short = chair.final_action in {"WATCH_SHORT", "PROPOSE_SHORT"}
        leaning_set = _SHORT_LEANING_VERDICTS if is_short else _LONG_LEANING_VERDICTS
        adverse_clusters = _BULLISH_COUNTER_CLUSTERS if is_short else _BEARISH_COUNTER_CLUSTERS

        leaning_count = 0
        for role, pos in current_positions.items():
            v = (pos.verdict or "").upper()
            if v in leaning_set:
                leaning_count += 1

        actionability = chair.structured_output.get("actionability_summary") \
            if isinstance(chair.structured_output, dict) else None
        if not isinstance(actionability, dict):
            actionability = {}
        chair_continuation_quality = str(actionability.get("continuation_quality") or "").strip().upper()
        chair_recent_cluster = str(actionability.get("recent_cluster") or "").strip().upper()

        weak_reasons: List[str] = []
        if leaning_count < 3:
            weak_reasons.append(
                f"specialist_dominance_below_3:{leaning_count}/5"
            )
        if chair_continuation_quality and chair_continuation_quality != "REJECTED":
            weak_reasons.append(
                f"continuation_quality_not_rejected:{chair_continuation_quality}"
            )
        if is_short and chair_recent_cluster not in _REQUIRED_REJECTION_CLUSTERS:
            weak_reasons.append(
                f"missing_rejection_cluster:{chair_recent_cluster or 'NONE'}"
            )
        if chair_recent_cluster in adverse_clusters:
            weak_reasons.append(
                f"counter_cluster_present:{chair_recent_cluster}"
            )

        if weak_reasons:
            weak_text = (
                f"Chair returned {chair.final_action} but DOMINANT "
                f"{'SHORT' if is_short else 'LONG'} EVIDENCE RULE not satisfied: "
                + "; ".join(weak_reasons)
                + ". Recorded for audit; this is informational and non-blocking."
            )
            weak_response = (
                f"chair_primary_reason={chair.primary_reason_code} | "
                f"thesis_health={chair.thesis_health or 'NONE'} | "
                f"specialists={','.join((p.verdict or '?') for p in current_positions.values())}"
            )

            def _persist_weak_short(cn):
                cur = cn.cursor()
                try:
                    _persist_interaction(
                        cur, run_id, dossier_id,
                        source_agent=_CHAIR_AGENT_NAME,
                        target_agent=_CHAIR_AGENT_NAME,
                        topic="WEAK_SHORT_EVIDENCE",
                        disagreement_type="WEAK_SHORT_EVIDENCE",
                        disagreement_text=_truncate(weak_text, 4000),
                        response_text=_truncate(weak_response, 4000),
                        resolved_flag=True,
                    )
                    cn.commit()
                finally:
                    cur.close()

            try:
                await asyncio.to_thread(
                    lambda: _persist_weak_short(conn_factory()),
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "phase4 weak_short_evidence persist failed dossier=%s: %s",
                    dossier_id, e,
                )

            res.interactions.append({
                "topic": "WEAK_SHORT_EVIDENCE",
                "disagreement_type": "WEAK_SHORT_EVIDENCE",
                "challenger": "CHAIR",
                "target": "CHAIR",
                "outcome": "AUDIT_RECORDED",
                "challenge_text": weak_text,
                "weak_reasons": weak_reasons,
                "chair_action": chair.final_action,
                "thesis_health": chair.thesis_health,
                "evidence_contract_version": "phase4_taxonomy_v2",
            })

    # ------------------------------------------------------------------
    # Phase 4 taxonomy v2: MISSING_LEVEL_CONFIDENCE_CITATION diagnostic.
    # Non-blocking. If the Chair returns a short verdict and references
    # support / broken-resistance language, the rationale should also
    # cite a confidence value to satisfy the LEVEL CONFIDENCE CITATION
    # RULE. Heuristic keyword-based check; deterministic, non-fatal.
    # ------------------------------------------------------------------
    if chair.final_action in {"WATCH_SHORT", "PROPOSE_SHORT"}:
        rationale_blob = " ".join([
            chair.final_thesis or "",
            chair.why_not_opposite or "",
            chair.risk_treatment or "",
        ]).lower()
        # Cheap keyword check: does the rationale reference support / a level?
        cites_level = any(kw in rationale_blob for kw in (
            "broken resistance", "broken-resistance",
            "nearest support", "support level",
            "support at", "support near",
            "at $", "above $", "below $",
        ))
        cites_confidence = ("confidence" in rationale_blob)
        if cites_level and not cites_confidence:
            cite_text = (
                f"Chair returned {chair.final_action} and referenced a price "
                "level in its rationale but did not cite the level's "
                "confidence value. LEVEL CONFIDENCE CITATION RULE requires "
                "explicit confidence when the dossier provides one (especially "
                "broken_resistance_support_confidence >= 0.7). "
                "Recorded for audit; this is informational and non-blocking."
            )
            cite_response = (
                f"chair_primary_reason={chair.primary_reason_code} | "
                f"thesis_health={chair.thesis_health or 'NONE'}"
            )

            def _persist_missing_confidence(cn):
                cur = cn.cursor()
                try:
                    _persist_interaction(
                        cur, run_id, dossier_id,
                        source_agent=_CHAIR_AGENT_NAME,
                        target_agent=_CHAIR_AGENT_NAME,
                        topic="MISSING_LEVEL_CONFIDENCE_CITATION",
                        disagreement_type="MISSING_LEVEL_CONFIDENCE_CITATION",
                        disagreement_text=_truncate(cite_text, 4000),
                        response_text=_truncate(cite_response, 4000),
                        resolved_flag=True,
                    )
                    cn.commit()
                finally:
                    cur.close()

            try:
                await asyncio.to_thread(
                    lambda: _persist_missing_confidence(conn_factory()),
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "phase4 missing_level_confidence persist failed dossier=%s: %s",
                    dossier_id, e,
                )

            res.interactions.append({
                "topic": "MISSING_LEVEL_CONFIDENCE_CITATION",
                "disagreement_type": "MISSING_LEVEL_CONFIDENCE_CITATION",
                "challenger": "CHAIR",
                "target": "CHAIR",
                "outcome": "AUDIT_RECORDED",
                "challenge_text": cite_text,
                "chair_action": chair.final_action,
                "evidence_contract_version": "phase4_taxonomy_v2",
            })

    return res


# ---------------------------------------------------------------------------
# Stage 5/6 persistence (post chair) — runs after all dossiers
# ---------------------------------------------------------------------------

def _gate_chair_specialist_count(cur, run_id: str, dossier_id: int) -> bool:
    """Hard ordering invariant: exactly 5 valid specialist rows for the dossier."""
    cur.execute(
        """
        SELECT COUNT(*) FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2
        WHERE RUN_ID = %(rid)s AND DOSSIER_ID = %(did)s
          AND VERDICT <> 'INVALID'
          AND PRIMARY_REASON_CODE <> 'AGENT_OUTPUT_INVALID'
        """,
        {"rid": run_id, "did": dossier_id},
    )
    n = cur.fetchone()[0]
    return n == len(_REQUIRED_ROLES)


def _persist_chair_verdict(
    cur, run_id: str, dossier_id: int, symbol: str, market_type: str,
    chair: ChairOutput, interactions: List[Dict[str, Any]],
    primary_evidence_setup_event_id: Optional[int],
) -> None:
    cur.execute(
        """
        INSERT INTO MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT (
            RUN_ID, DOSSIER_ID, SYMBOL, MARKET_TYPE,
            FINAL_RANK, THESIS_VERDICT, THESIS_DIRECTION,
            FINAL_ACTION, FINAL_DIRECTION,
            FINAL_THESIS, WHY_NOT_OPPOSITE, WHY_NOT_NO_TRADE,
            PRIMARY_REASON_CODE, SECONDARY_REASON_CODE,
            PROPOSED_TRADE_CONFIG_JSON, RISK_TREATMENT,
            COMMITTEE_PAYLOAD, CHAIR_OUTPUT_JSON
        )
        SELECT
            %(run_id)s, %(did)s, %(sym)s, %(mkt)s,
            NULL, %(verdict)s, %(direction)s,
            %(action)s, %(direction)s,
            %(thesis)s, %(why_not_opp)s, %(why_not_nt)s,
            %(primary)s, %(secondary)s,
            PARSE_JSON(%(config)s), %(risk_treatment)s,
            PARSE_JSON(%(committee_payload)s),
            PARSE_JSON(%(chair_json)s)
        """,
        {
            "run_id": run_id, "did": dossier_id, "sym": symbol, "mkt": market_type,
            "verdict": _truncate(chair.final_action, 80),
            "direction": _truncate(chair.final_direction, 10),
            "action": _truncate(chair.final_action, 40),
            "thesis": _truncate(chair.final_thesis, 4000),
            "why_not_opp": _truncate(chair.why_not_opposite, 4000),
            "why_not_nt": _truncate(chair.why_not_no_trade, 4000),
            "primary": _truncate(chair.primary_reason_code, 80),
            "secondary": _truncate(chair.secondary_reason_code, 80),
            "config": _jdump(chair.proposed_trade_config or {}),
            "risk_treatment": _truncate(chair.risk_treatment, 4000),
            "committee_payload": _jdump({
                "primary_evidence_setup_event_id": primary_evidence_setup_event_id,
                "primary_evidence_setup_event_id_role": "EVIDENCE_ONLY_NOT_DIRECTION_SOURCE",
                "interactions": interactions,
                "unresolved_disagreement": bool(chair.unresolved_disagreement),
            }),
            "chair_json": _jdump(chair.structured_output or {}),
        },
    )


def _persist_final_slate(
    cur, run_id: str, max_proposals: int,
) -> None:
    cur.execute(
        """
        INSERT INTO MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 (
            RUN_ID, DOSSIER_ID, SYMBOL, MARKET_TYPE, RANK,
            FINAL_ACTION, FINAL_DIRECTION,
            THESIS_LABEL, SIZING_TREATMENT, FINAL_RATIONALE_SUMMARY,
            DOWNSTREAM_PAYLOAD_POINTER, PUBLICATION_STATUS
        )
        SELECT
            v.RUN_ID, v.DOSSIER_ID, v.SYMBOL, v.MARKET_TYPE,
            -- Direction-neutral ranking (mirrors 566/565). PROPOSE_LONG and
            -- PROPOSE_SHORT share tier 1 so the strongest evidence wins
            -- regardless of direction. The prior LONG-before-SHORT ordering
            -- silently demoted all shorts below all longs.
            ROW_NUMBER() OVER (
                PARTITION BY v.RUN_ID
                ORDER BY
                    CASE v.FINAL_ACTION
                        WHEN 'PROPOSE_LONG' THEN 1
                        WHEN 'PROPOSE_SHORT' THEN 1
                        WHEN 'WATCH_LONG' THEN 2
                        WHEN 'WATCH_SHORT' THEN 2
                        WHEN 'WAIT_FOR_CONFIRMATION' THEN 3
                        WHEN 'NO_TRADE' THEN 4
                        WHEN 'REJECT' THEN 5
                        ELSE 6
                    END,
                    COALESCE(TRY_TO_DOUBLE(v.CHAIR_OUTPUT_JSON:confidence::STRING), 0.0) DESC,
                    v.SYMBOL
            ) AS RANK,
            v.FINAL_ACTION, v.FINAL_DIRECTION,
            v.PROPOSED_TRADE_CONFIG_JSON:thesis_label::STRING,
            v.PROPOSED_TRADE_CONFIG_JSON:size_treatment::STRING,
            LEFT(v.FINAL_THESIS, 4000),
            'PROPOSAL_BOARD_THESIS_VERDICT:' || v.VERDICT_ID::STRING,
            'PENDING'
        FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT v
        WHERE v.RUN_ID = %(run_id)s
          AND NOT EXISTS (
              SELECT 1 FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
              WHERE fs.RUN_ID = v.RUN_ID AND fs.DOSSIER_ID = v.DOSSIER_ID
          )
        """,
        {"run_id": run_id},
    )


def _publish_to_structural(
    cur, run_id: str, portfolio_id: Optional[int], max_proposals: int,
    short_publication_allowed: bool = False,
    ibkr_account_mode: str = "UNKNOWN",
) -> Tuple[int, int]:
    """Returns (published_count, skipped_count).

    Design rule (paper/real equivalence): the market VERDICT and trade
    configuration are identical regardless of account mode. Account mode is an
    EXECUTION SAFETY CONTROL only — it may annotate a proposal as non-executable
    but must never change direction, entry zone, invalidation, trail params, or
    committee payload.

    Accordingly, both PROPOSE_LONG and PROPOSE_SHORT rows are ALWAYS inserted
    with their full chair verdict preserved. `short_publication_allowed` (set
    True only when ``MIP.LIVE.LIVE_PORTFOLIO_CONFIG.IBKR_ACCOUNT_MODE = 'PAPER'``
    for the board-run ``portfolio_id``, see ``_get_ibkr_account_mode``) does NOT
    suppress shorts; when False it only records
    ``EXECUTION_POLICY_STATUS='BROKER_BLOCKED'`` /
    ``EXECUTION_POLICY_REASON='IBKR_NOT_PAPER'`` / ``IS_RESEARCH_ONLY=TRUE`` on
    the short proposal. Real-account execution of shorts is independently gated
    at execute time by the per-portfolio ``ALLOW_SHORT_SELLING`` flag.

    `ibkr_account_mode` is recorded in audit JSON for transparency.
    """
    cur.execute(
        """
        INSERT INTO MIP.APP.STRUCTURAL_TRADE_PROPOSALS (
            SETUP_EVENT_ID, PORTFOLIO_ID, SYMBOL, DIRECTION, SETUP_FAMILY,
            ENTRY_ZONE_LOW, ENTRY_ZONE_HIGH, PRICE_INVALIDATION_LEVEL, INVALIDATION_RULE,
            TRAIL_STYLE, TRAIL_PARAMS, EXIT_STYLE, EXIT_PROFILE,
            STRUCTURE_CONFIDENCE, LEVEL_SIGNIFICANCE, REGIME_COMPAT,
            MEANINGFUL_HIT_RATE, PATH_SURVIVAL_HIT_RATE, MFE_MAE_RATIO,
            RISK_CLASS, CONFLICT_RESOLUTION, RATIONALE_TEXT,
            COMMITTEE_PAYLOAD, STATUS,
            BOARD_RUN_ID, BOARD_CANDIDATE_ID, BOARD_DOSSIER_ID,
            PRIMARY_EVIDENCE_SETUP_EVENT_ID,
            BOARD_FINAL_RANK, BOARD_FINAL_VERDICT, BOARD_PRIMARY_REASON_CODE,
            BOARD_REASON_CODES, BOARD_RATIONALE, BOARD_PAYLOAD_JSON,
            EXECUTION_POLICY_STATUS, EXECUTION_POLICY_REASON, IS_RESEARCH_ONLY
        )
        WITH chair_intent AS (
            -- Resolve chair-emitted exit profile (preferred path) or derive
            -- from trail_value PCT. Trailing stops are the AGENTIC default;
            -- we only use FIXED_STANDARD when the chair explicitly opts out.
            SELECT
                fs.RUN_ID, fs.DOSSIER_ID, fs.RANK, fs.PUBLICATION_STATUS,
                v.PROPOSED_TRADE_CONFIG_JSON AS PTC,
                v.FINAL_ACTION, v.FINAL_DIRECTION,
                v.PRIMARY_REASON_CODE, v.SECONDARY_REASON_CODE,
                v.FINAL_THESIS, v.COMMITTEE_PAYLOAD, v.CHAIR_OUTPUT_JSON,
                CASE
                    WHEN UPPER(NULLIF(TRIM(v.PROPOSED_TRADE_CONFIG_JSON:exit_profile::STRING), ''))
                         IN ('FIXED_STANDARD','TRAIL_TIGHT','TRAIL_STANDARD','TRAIL_WIDE')
                        THEN UPPER(TRIM(v.PROPOSED_TRADE_CONFIG_JSON:exit_profile::STRING))
                    WHEN TRY_TO_DOUBLE(v.PROPOSED_TRADE_CONFIG_JSON:trailing_policy:trail_value::STRING) IS NULL
                        THEN 'TRAIL_STANDARD'
                    WHEN TRY_TO_DOUBLE(v.PROPOSED_TRADE_CONFIG_JSON:trailing_policy:trail_value::STRING) <= 1.5
                        THEN 'TRAIL_TIGHT'
                    WHEN TRY_TO_DOUBLE(v.PROPOSED_TRADE_CONFIG_JSON:trailing_policy:trail_value::STRING) <= 3.0
                        THEN 'TRAIL_STANDARD'
                    ELSE 'TRAIL_WIDE'
                END AS DERIVED_EXIT_PROFILE
              FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
              JOIN MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT v
                ON v.RUN_ID = fs.RUN_ID AND v.DOSSIER_ID = fs.DOSSIER_ID
             WHERE fs.RUN_ID = %(run_id)s
        )
        SELECT
            -- SETUP_EVENT_ID: NULL when evidence direction != proposal direction.
            -- Cross-direction evidence is preserved in PRIMARY_EVIDENCE_SETUP_EVENT_ID.
            IFF(ev.DIRECTION = v.FINAL_DIRECTION, s.PRIMARY_EVIDENCE_SETUP_EVENT_ID, NULL),
            s.PORTFOLIO_ID, s.SYMBOL,
            v.FINAL_DIRECTION,
            LEFT(v.PTC:thesis_label::STRING, 80),
            TRY_TO_DOUBLE(v.PTC:entry_zone_low::STRING),
            TRY_TO_DOUBLE(v.PTC:entry_zone_high::STRING),
            TRY_TO_DOUBLE(v.PTC:invalidation_level::STRING),
            LEFT(COALESCE(v.PTC:invalidation_rule::STRING, 'AGENTIC_INVALIDATION'), 30),
            -- TRAIL_STYLE: canonical 'PCT' for any TRAIL_* profile, NULL when
            -- chair explicitly chose FIXED_STANDARD.
            CASE WHEN v.DERIVED_EXIT_PROFILE = 'FIXED_STANDARD' THEN NULL ELSE 'PCT' END,
            -- TRAIL_PARAMS: broker-executable shape matching exit_policy.PROFILES.
            -- policy_version='v1' is mandatory; otherwise validate_trail_params()
            -- in mip_ui_api/services/live_intelligence/exit_policy.py rejects it
            -- with UNSUPPORTED_POLICY_VERSION and execution is hard-blocked.
            CASE
                WHEN v.DERIVED_EXIT_PROFILE = 'FIXED_STANDARD' THEN NULL
                WHEN v.DERIVED_EXIT_PROFILE = 'TRAIL_TIGHT' THEN PARSE_JSON(
                    '{"policy_version":"v1","profile":"TRAIL_TIGHT","reference":"ENTRY_FILL",'
                    || '"tp_mode":"LIMIT","trail_mode":"PCT","trail_value":1.5}')
                WHEN v.DERIVED_EXIT_PROFILE = 'TRAIL_WIDE' THEN PARSE_JSON(
                    '{"policy_version":"v1","profile":"TRAIL_WIDE","reference":"ENTRY_FILL",'
                    || '"tp_mode":"LIMIT","trail_mode":"PCT","trail_value":4.0}')
                ELSE PARSE_JSON(
                    '{"policy_version":"v1","profile":"TRAIL_STANDARD","reference":"ENTRY_FILL",'
                    || '"tp_mode":"LIMIT","trail_mode":"PCT","trail_value":2.5}')
            END,
            LEFT(COALESCE(v.PTC:target_policy:exit_style::STRING, 'STAGED_PARTIAL'), 20),
            v.DERIVED_EXIT_PROFILE,
            TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:structure:state_confidence::STRING),
            COALESCE(
                TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:levels:nearest_resistance:level_significance::STRING),
                TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:levels:nearest_support:level_significance::STRING)
            ),
            LEFT(COALESCE(s.DOSSIER_PAYLOAD_JSON:regime:tags:trend_regime::STRING, 'AGENTIC'), 10),
            COALESCE(
                TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:long_history[0]:meaningful_hit_rate::STRING),
                TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:short_history[0]:meaningful_hit_rate::STRING)
            ),
            COALESCE(
                TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:long_history[0]:path_survival_hit_rate::STRING),
                TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:short_history[0]:path_survival_hit_rate::STRING)
            ),
            COALESCE(
                TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:long_history[0]:mfe_mae_ratio::STRING),
                TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:short_history[0]:mfe_mae_ratio::STRING)
            ),
            LEFT(COALESCE(v.PTC:risk_class::STRING, 'MEDIUM'), 10),
            NULL,
            LEFT('Phase 4 agentic board rank ' || v.RANK || ' | ' || v.FINAL_ACTION
                 || ' | ' || v.PRIMARY_REASON_CODE
                 || ' | ' || v.FINAL_THESIS, 2000),
            v.COMMITTEE_PAYLOAD, 'PROPOSED',
            %(run_id)s, NULL, v.DOSSIER_ID,
            s.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
            v.RANK,
            LEFT(v.FINAL_ACTION, 30),
            v.PRIMARY_REASON_CODE,
            ARRAY_CONSTRUCT(v.PRIMARY_REASON_CODE, v.SECONDARY_REASON_CODE),
            LEFT(v.FINAL_THESIS, 4000),
            OBJECT_CONSTRUCT(
                'agentic_board', TRUE,
                'mode', %(mode)s,
                'direction_source', 'CHAIR_PORTFOLIO_PM.final_direction',
                'setup_family_source', 'CHAIR_PORTFOLIO_PM.proposed_trade_config.thesis_label',
                'exit_profile_source', 'CHAIR_PORTFOLIO_PM.proposed_trade_config.exit_profile_or_trail_value_derived',
                'derived_exit_profile', v.DERIVED_EXIT_PROFILE,
                'primary_evidence_setup_event_id', s.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
                'primary_evidence_setup_event_id_role',
                    IFF(ev.DIRECTION = v.FINAL_DIRECTION,
                        'AUTHORITATIVE_DIRECTION_SOURCE',
                        'EVIDENCE_ONLY_NOT_DIRECTION_SOURCE'),
                'evidence_direction', ev.DIRECTION,
                'proposal_direction', v.FINAL_DIRECTION,
                'is_cross_direction_evidence', IFF(ev.DIRECTION != v.FINAL_DIRECTION, TRUE, FALSE),
                'dossier_id', v.DOSSIER_ID,
                'dossier_payload', s.DOSSIER_PAYLOAD_JSON,
                'chair_output', v.CHAIR_OUTPUT_JSON,
                'proposed_trade_config', v.PTC,
                'committee_payload', v.COMMITTEE_PAYLOAD
            ),
            -- Execution policy: hard gate persisted at write time.
            -- SHORT proposals are never silently dropped; policy is recorded explicitly.
            CASE
                WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT s.SHORT_LIVE_ENABLED
                    THEN 'POLICY_BLOCKED'
                WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND s.SHORT_LIVE_ENABLED
                     AND NOT %(short_pub_allowed)s
                    THEN 'BROKER_BLOCKED'
                ELSE 'EXECUTABLE'
            END,
            CASE
                WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT s.SHORT_LIVE_ENABLED
                    THEN 'SHORT_LIVE_DISABLED'
                WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND s.SHORT_LIVE_ENABLED
                     AND NOT %(short_pub_allowed)s
                    THEN 'IBKR_NOT_PAPER'
                ELSE NULL
            END,
            IFF(v.FINAL_ACTION = 'PROPOSE_SHORT'
                AND (NOT s.SHORT_LIVE_ENABLED OR NOT %(short_pub_allowed)s),
                TRUE, FALSE)
        FROM chair_intent v
        JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
          ON s.RUN_ID = v.RUN_ID AND s.DOSSIER_ID = v.DOSSIER_ID
        -- Left-join evidence event to get direction for SETUP_EVENT_ID guard
        LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS ev
          ON ev.SETUP_EVENT_ID = s.PRIMARY_EVIDENCE_SETUP_EVENT_ID
        WHERE v.PUBLICATION_STATUS = 'PENDING'
          AND v.RANK <= %(max_props)s
          AND s.PRIMARY_EVIDENCE_SETUP_EVENT_ID IS NOT NULL
          AND v.PTC:thesis_label::STRING ILIKE 'AGENTIC_%%'
          -- Phase 4 taxonomy v2: hard STOCK-only publication guard.
          AND s.MARKET_TYPE = 'STOCK'
          -- Allow PROPOSE_SHORT unconditionally; policy recorded in EXECUTION_POLICY_STATUS.
          AND v.FINAL_ACTION IN ('PROPOSE_LONG', 'PROPOSE_SHORT')
          AND NOT EXISTS (
              SELECT 1 FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
              WHERE p.STATUS = 'PROPOSED' AND p.SYMBOL = s.SYMBOL
                AND (%(portfolio_id)s IS NULL OR p.PORTFOLIO_ID = %(portfolio_id)s OR p.PORTFOLIO_ID IS NULL)
          )
          AND NOT EXISTS (
              SELECT 1 FROM MIP.LIVE.LIVE_ACTIONS la
              WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL' AND la.SYMBOL = s.SYMBOL
                AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION','OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')
                AND (%(portfolio_id)s IS NULL OR la.PORTFOLIO_ID = %(portfolio_id)s OR la.PORTFOLIO_ID IS NULL)
          )
        """,
        {
            "run_id": run_id,
            "portfolio_id": portfolio_id,
            "max_props": max_proposals,
            "mode": _MODEL_CONFIG_MODE,
            "short_pub_allowed": bool(short_publication_allowed),
        },
    )

    cur.execute(
        """
        UPDATE MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
           SET PUBLICATION_STATUS = 'PUBLISHED',
               PUBLISHED_PROPOSAL_ID = p.PROPOSAL_ID,
               PUBLISHED_AT = p.CREATED_AT
          FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
         WHERE fs.RUN_ID = %(run_id)s
           AND p.BOARD_RUN_ID = fs.RUN_ID
           AND p.BOARD_DOSSIER_ID = fs.DOSSIER_ID
           AND fs.PUBLICATION_STATUS = 'PENDING'
        """,
        {"run_id": run_id},
    )

    # Phase 4 taxonomy v2: any PENDING row that did not insert is marked
    # SKIPPED_GUARDRAIL with a reason. Non-STOCK rows get a dedicated
    # BLOCKED_NON_STOCK_PUBLISH reason so the smoke check surfaces them.
    cur.execute(
        """
        UPDATE MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
           SET PUBLICATION_STATUS = 'SKIPPED_GUARDRAIL',
               PUBLICATION_ERROR_JSON = CASE
                   WHEN COALESCE(s.MARKET_TYPE, 'UNKNOWN') <> 'STOCK'
                   THEN OBJECT_CONSTRUCT(
                       'reason', 'BLOCKED_NON_STOCK_PUBLISH',
                       'reason_detail', 'Phase 4 hard STOCK-only publication guard blocked this row.',
                       'market_type', COALESCE(s.MARKET_TYPE, 'UNKNOWN'),
                       'portfolio_id', %(pid)s
                   )
                   WHEN fs.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT %(short_pub_allowed)s
                   THEN OBJECT_CONSTRUCT(
                       'reason', 'IBKR_ACCOUNT_MODE_NOT_PAPER',
                       'reason_detail', 'Short publication blocked: portfolio IBKR_ACCOUNT_MODE is not PAPER.',
                       'ibkr_account_mode', %(ibkr_mode)s,
                       'portfolio_id', %(pid)s
                   )
                   ELSE OBJECT_CONSTRUCT(
                       'reason', 'not_inserted_duplicate_collision_missing_evidence_or_policy'
                   )
               END
          FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
         WHERE fs.RUN_ID = %(run_id)s
           AND fs.PUBLICATION_STATUS = 'PENDING'
           AND s.RUN_ID = fs.RUN_ID
           AND s.DOSSIER_ID = fs.DOSSIER_ID
        """,
        {
            "run_id": run_id,
            "short_pub_allowed": bool(short_publication_allowed),
            "ibkr_mode": ibkr_account_mode,
            "pid": portfolio_id,
        },
    )

    # Phase 3: post-INSERT geometry validation.
    # Marks GEOMETRY_INVALID for proposals with incoherent invalidation geometry.
    # Cross-direction evidence (SETUP_EVENT_ID IS NULL) → SETUP_EVENT_DIRECTION_MISMATCH.
    # Both are hard-blocked at LPA/API; proposals survive for diagnostics.
    cur.execute(
        """
        UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
           SET EXECUTION_POLICY_STATUS = 'GEOMETRY_INVALID',
               EXECUTION_POLICY_REASON = CASE
                   WHEN p.DIRECTION = 'LONG'  THEN 'INVALID_LONG_GEOMETRY'
                   WHEN p.DIRECTION = 'SHORT' THEN 'INVALID_SHORT_GEOMETRY'
                   ELSE 'INVALID_LONG_GEOMETRY'
               END,
               IS_RESEARCH_ONLY = TRUE
         WHERE p.BOARD_RUN_ID = %(run_id)s
           AND p.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
           AND (
               (p.DIRECTION = 'LONG'
                AND p.PRICE_INVALIDATION_LEVEL IS NOT NULL
                AND p.ENTRY_ZONE_LOW IS NOT NULL
                AND p.PRICE_INVALIDATION_LEVEL >= p.ENTRY_ZONE_LOW)
               OR
               (p.DIRECTION = 'SHORT'
                AND p.PRICE_INVALIDATION_LEVEL IS NOT NULL
                AND p.ENTRY_ZONE_HIGH IS NOT NULL
                AND p.PRICE_INVALIDATION_LEVEL <= p.ENTRY_ZONE_HIGH)
               OR
               (p.ENTRY_ZONE_LOW IS NOT NULL AND p.ENTRY_ZONE_HIGH IS NOT NULL
                AND p.ENTRY_ZONE_LOW > p.ENTRY_ZONE_HIGH)
           )
        """,
        {"run_id": run_id},
    )
    cur.execute(
        """
        UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
           SET EXECUTION_POLICY_STATUS = 'POLICY_BLOCKED',
               EXECUTION_POLICY_REASON = 'SETUP_EVENT_DIRECTION_MISMATCH',
               IS_RESEARCH_ONLY = TRUE
         WHERE p.BOARD_RUN_ID = %(run_id)s
           AND p.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
           AND p.SETUP_EVENT_ID IS NULL
           AND p.PRIMARY_EVIDENCE_SETUP_EVENT_ID IS NOT NULL
        """,
        {"run_id": run_id},
    )

    cur.execute(
        "SELECT COUNT(*) FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 "
        "WHERE RUN_ID = %(run_id)s AND PUBLICATION_STATUS = 'PUBLISHED'",
        {"run_id": run_id},
    )
    published = cur.fetchone()[0] or 0
    cur.execute(
        "SELECT COUNT(*) FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 "
        "WHERE RUN_ID = %(run_id)s AND PUBLICATION_STATUS = 'SKIPPED_GUARDRAIL'",
        {"run_id": run_id},
    )
    skipped = cur.fetchone()[0] or 0

    # Mirror published rows into STRUCTURAL_PROPOSAL_SNAPSHOT for audit lineage.
    cur.execute(
        """
        INSERT INTO MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT (
            PROPOSAL_ID, PROPOSAL_TS, SYMBOL, SIDE, SETUP_FAMILY,
            STRUCTURAL_STATE, REGIME_STATE, TRUST_LABEL,
            ENTRY_ZONE_JSON, INVALIDATION_JSON, PATH_METRICS_JSON, MFE_MAE_JSON,
            TRAILING_STYLE, PROPOSAL_SUMMARY_JSON
        )
        SELECT
            p.PROPOSAL_ID, p.CREATED_AT, p.SYMBOL, p.DIRECTION, p.SETUP_FAMILY,
            -- BOARD_PAYLOAD_JSON embeds dossier_payload at publish time.
            -- COMMITTEE_PAYLOAD for Phase 4 agentic proposals does NOT
            -- contain dossier_payload, so the legacy path always resolved
            -- to NULL. COALESCE keeps backward compatibility with any
            -- legacy committee paths that still embed dossier_payload.
            COALESCE(
                p.BOARD_PAYLOAD_JSON:dossier_payload:structure:structural_state::STRING,
                p.COMMITTEE_PAYLOAD:dossier_payload:structure:structural_state::STRING
            ),
            COALESCE(
                p.BOARD_PAYLOAD_JSON:dossier_payload:regime:tags:trend_regime::STRING,
                p.COMMITTEE_PAYLOAD:dossier_payload:regime:tags:trend_regime::STRING
            ),
            'AGENTIC',
            OBJECT_CONSTRUCT('low', p.ENTRY_ZONE_LOW, 'high', p.ENTRY_ZONE_HIGH),
            OBJECT_CONSTRUCT('level', p.PRICE_INVALIDATION_LEVEL, 'rule', p.INVALIDATION_RULE),
            OBJECT_CONSTRUCT(
                'meaningful_hit_rate', p.MEANINGFUL_HIT_RATE,
                'path_survival_hit_rate', p.PATH_SURVIVAL_HIT_RATE,
                'mfe_mae_ratio', p.MFE_MAE_RATIO,
                'primary_evidence_setup_event_id', p.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
                'primary_evidence_setup_event_id_role', 'EVIDENCE_ONLY_NOT_DIRECTION_SOURCE'
            ),
            OBJECT_CONSTRUCT(
                'mfe_mae_ratio', p.MFE_MAE_RATIO,
                'meaningful_hit_rate', p.MEANINGFUL_HIT_RATE,
                'path_survival_hit_rate', p.PATH_SURVIVAL_HIT_RATE
            ),
            p.TRAIL_STYLE,
            p.COMMITTEE_PAYLOAD
        FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
        WHERE p.BOARD_RUN_ID = %(run_id)s
          AND NOT EXISTS (
              SELECT 1 FROM MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT s
              WHERE s.PROPOSAL_ID = p.PROPOSAL_ID
          )
        """,
        {"run_id": run_id},
    )
    return int(published), int(skipped)


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def _crude_prerank_key(row: Tuple[int, str, str, Dict[str, Any]]) -> Tuple[int, int, str]:
    """TEMP_COST_CAP_ORDER: crude stable ordering used until ranking.py (Stage 3).

    Sorts eligible rows before applying max_candidates cap so the selection is
    deterministic and reproducible.  Priority (descending):
      1. Number of active setup events (more evidence = more likely to produce a proposal)
      2. Dossier ID descending (larger = more recently inserted)
      3. Symbol alphabetically (tie-break)

    Not a quality gate.  Replaced by score_candidate() in Stage 3.
    """
    did, sym, _mkt, payload = row
    setup_events = payload.get("setup_events_evidence_only") if isinstance(payload, dict) else None
    n_events = len(setup_events) if isinstance(setup_events, list) else 0
    return (-n_events, -did, sym)


@dataclass
class BoardRunResult:
    run_id: str
    status: str
    as_of_date: _date
    dossier_count: int
    valid_dossier_count: int
    invalid_dossier_count: int
    published_count: int
    skipped_count: int
    eligible_count: int = 0
    genuine_eligible_count: int = 0
    cost_capped_count: int = 0
    eligibility_skipped_count: int = 0
    eligibility_skip_breakdown: Dict[str, int] = field(default_factory=dict)
    candidate_mode: str = "UNCAPPED"
    estimated_agent_sessions: int = 0
    chair_propose_count: int = 0
    props_executable_count: int = 0
    imported_to_lpa_count: int = 0
    ibkr_account_mode: str = "UNKNOWN"
    short_publication_allowed: bool = False
    error: Optional[str] = None


async def orchestrate_phase4_board(
    portfolio_id: Optional[int] = None,
    as_of_date: Optional[_date] = None,
    symbols_filter: Optional[List[str]] = None,
    market_types_filter: Optional[List[str]] = None,
    max_proposals: int = _DEFAULT_MAX_PROPOSALS,
    max_rounds: int = _DEFAULT_MAX_ROUNDS,
    inter_dossier_concurrency: int = 2,
    per_dossier_concurrency: int = 5,
    dry_run: bool = False,
    max_candidates: Optional[int] = _DEFAULT_MAX_CANDIDATES,
    daily_call_budget: int = _DEFAULT_DAILY_CALL_BUDGET,
    allow_budget_override: bool = False,
) -> BoardRunResult:
    """
    Run the full Phase 4 Cortex Agentic Proposal Board.
    `dry_run=True` skips the STRUCTURAL_TRADE_PROPOSALS insert step (Stage 6).
    """
    _load_env()
    rest_creds = _get_rest_creds()
    run_id = str(uuid.uuid4())
    as_of = as_of_date or _date.today()

    conn = _connect()
    started_at_log = ""
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO MIP.APP.PROPOSAL_BOARD_RUN (
                    RUN_ID, AS_OF_DATE, PORTFOLIO_ID, RUN_STATUS,
                    MODEL_CONFIG_JSON, PROMPT_VERSION, POLICY_VERSION
                )
                SELECT %(run_id)s, %(as_of)s, %(portfolio_id)s, 'RUNNING',
                       PARSE_JSON(%(cfg)s), %(prompt_v)s, %(policy_v)s
                """,
                {
                    "run_id": run_id, "as_of": as_of, "portfolio_id": portfolio_id,
                    "cfg": _jdump({
                        "mode": _MODEL_CONFIG_MODE,
                        "agents": _AGENT_OBJECT_NAMES,
                        "chair": _CHAIR_AGENT_NAME,
                        "model": _OBJECTLESS_MODEL,
                        "max_rounds": max_rounds,
                        "per_dossier_concurrency": per_dossier_concurrency,
                        "inter_dossier_concurrency": inter_dossier_concurrency,
                        "max_proposals": max_proposals,
                        "max_candidates": max_candidates,
                        "market_types_filter": (
                            [m.upper() for m in market_types_filter]
                            if market_types_filter else None
                        ),
                        "dry_run": bool(dry_run),
                    }),
                    "prompt_v": _PROMPT_VERSION,
                    "policy_v": _POLICY_VERSION,
                },
            )
            rows = _snapshot_dossiers(
                cur, run_id, as_of, portfolio_id, symbols_filter,
                market_types_filter=market_types_filter,
            )
            _stage_pack_cache(cur, run_id, rows)
            conn.commit()
        finally:
            cur.close()
    except Exception as e:
        try:
            cur2 = conn.cursor()
            cur2.execute(
                """
                UPDATE MIP.APP.PROPOSAL_BOARD_RUN
                   SET RUN_STATUS = 'FAILED',
                       FINISHED_AT = CURRENT_TIMESTAMP(),
                       ERROR_JSON = OBJECT_CONSTRUCT('reason_code','SNAPSHOT_FAILED','message',%(msg)s)
                 WHERE RUN_ID = %(run_id)s
                """,
                {"run_id": run_id, "msg": str(e)[:2000]},
            )
            conn.commit()
            cur2.close()
        finally:
            conn.close()
        return BoardRunResult(
            run_id=run_id, status="FAILED", as_of_date=as_of,
            dossier_count=0, valid_dossier_count=0,
            invalid_dossier_count=0, published_count=0, skipped_count=0,
            error=f"snapshot_failed: {e}",
        )

    if not rows:
        cur3 = conn.cursor()
        cur3.execute(
            """
            UPDATE MIP.APP.PROPOSAL_BOARD_RUN
               SET RUN_STATUS = 'COMPLETE',
                   FINISHED_AT = CURRENT_TIMESTAMP(),
                   FINAL_PROPOSAL_COUNT = 0
             WHERE RUN_ID = %(run_id)s
            """,
            {"run_id": run_id},
        )
        conn.commit()
        cur3.close()
        conn.close()
        return BoardRunResult(
            run_id=run_id, status="COMPLETE_NO_DOSSIERS", as_of_date=as_of,
            dossier_count=0, valid_dossier_count=0,
            invalid_dossier_count=0, published_count=0, skipped_count=0,
        )

    # Connection factory for per-task threads (snowflake-connector is not async).
    def _conn_factory():
        return _connect()

    # Stage 0.5 — direction-neutral eligibility filter. Persist a decision per
    # snapshotted symbol regardless of whether agents will run. The filter is
    # bypassed when the operator passed an explicit --symbols list.
    operator_override = bool(symbols_filter)
    eligibility_by_dossier: Dict[int, EligibilityDecision] = {}
    eligible_rows: List[Tuple[int, str, str, Dict[str, Any]]] = []
    skip_counts: Dict[str, int] = {}
    elig_cur = conn.cursor()
    try:
        for did, sym, mkt, payload in rows:
            decision = evaluate_dossier_eligibility(
                symbol=sym,
                market_type=mkt,
                payload=payload,
                as_of=as_of,
                operator_symbol_override=operator_override,
            )
            eligibility_by_dossier[did] = decision
            try:
                _persist_eligibility(
                    elig_cur, run_id, as_of, portfolio_id, did, decision,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "phase4 eligibility persist failed dossier=%s symbol=%s: %s",
                    did, sym, e,
                )
            if decision.eligible:
                eligible_rows.append((did, sym, mkt, payload))
            else:
                skip_counts[decision.primary_reason_code] = (
                    skip_counts.get(decision.primary_reason_code, 0) + 1
                )
        conn.commit()
    finally:
        try:
            elig_cur.close()
        except Exception:  # noqa: BLE001
            pass

    logger.info(
        "phase4 eligibility filter run=%s total=%d eligible=%d skipped=%d skip_breakdown=%s "
        "operator_override=%s",
        run_id, len(rows), len(eligible_rows), len(rows) - len(eligible_rows),
        skip_counts, operator_override,
    )

    # Stage 0.6 — TEMP_COST_CAP_ORDER: crude stable pre-agent sort + candidate cap.
    # Applied before asyncio.gather so only the top-N candidates reach Cortex agents.
    # Sorting is deterministic (not raw arrival order) regardless of whether a cap
    # is active. This is Stage 2 of the cost-control plan; replaced by
    # score_candidate() in Stage 3 (ranking.py).
    genuine_eligible_count = len(eligible_rows)
    cost_capped_count = 0
    candidate_mode = "UNCAPPED"

    # Always sort by crude stable key for deterministic order.
    eligible_rows = sorted(eligible_rows, key=_crude_prerank_key)

    if max_candidates is not None and len(eligible_rows) > max_candidates:
        kept = eligible_rows[:max_candidates]
        skipped_cap = eligible_rows[max_candidates:]
        cost_capped_count = len(skipped_cap)
        candidate_mode = "TEMP_COST_CAP_ORDER"

        logger.info(
            "phase4 TEMP_COST_CAP_ORDER run=%s max_candidates=%d "
            "kept=%s skipped=%d (order=n_setup_events,dossier_id,symbol)",
            run_id, max_candidates,
            [sym for _, sym, _, _ in kept],
            cost_capped_count,
        )

        cap_elig_cur = conn.cursor()
        try:
            for did, sym, mkt, _payload in skipped_cap:
                skip_counts["NOT_SENT_TO_AGENT_PANEL_COST_CAP"] = (
                    skip_counts.get("NOT_SENT_TO_AGENT_PANEL_COST_CAP", 0) + 1
                )
                try:
                    _persist_eligibility(
                        cap_elig_cur, run_id, as_of, portfolio_id, did,
                        EligibilityDecision(
                            symbol=sym,
                            market_type=mkt,
                            eligible=False,
                            primary_reason_code="NOT_SENT_TO_AGENT_PANEL_COST_CAP",
                            signal_flags={"temp_cap_order": True},
                            evidence_summary={"note": "skipped by max_candidates cap; not a quality rejection"},
                        ),
                    )
                except Exception:  # noqa: BLE001
                    pass
            conn.commit()
        finally:
            try:
                cap_elig_cur.close()
            except Exception:  # noqa: BLE001
                pass
        eligible_rows = kept

    # Budget preflight guard — runs after candidate cap, before Cortex fan-out.
    # Denominated in estimated agent sessions (not SP calls).
    # --allow-budget-override required to exceed daily_call_budget.
    estimated_agent_sessions = len(eligible_rows) * _EMPIRICAL_SESSIONS_PER_CANDIDATE
    estimated_sp_calls = estimated_agent_sessions * _EMPIRICAL_SLICES_PER_SESSION
    logger.info(
        "phase4 budget_preflight run=%s candidates=%d "
        "estimated_agent_sessions=%d estimated_sp_calls=%d "
        "daily_budget=%d allow_override=%s",
        run_id, len(eligible_rows),
        estimated_agent_sessions, estimated_sp_calls,
        daily_call_budget, allow_budget_override,
    )
    if not allow_budget_override and estimated_agent_sessions > daily_call_budget:
        err_msg = (
            f"phase4 budget_exceeded: estimated_agent_sessions={estimated_agent_sessions} "
            f"> daily_call_budget={daily_call_budget}. "
            f"Reduce --max-candidates or pass --allow-budget-override."
        )
        logger.error(err_msg)
        try:
            err_cur = conn.cursor()
            err_cur.execute(
                """
                UPDATE MIP.APP.PROPOSAL_BOARD_RUN
                   SET RUN_STATUS = 'FAILED',
                       FINISHED_AT = CURRENT_TIMESTAMP(),
                       ERROR_JSON = OBJECT_CONSTRUCT('reason_code','BUDGET_EXCEEDED','message',%(msg)s)
                 WHERE RUN_ID = %(run_id)s
                """,
                {"run_id": run_id, "msg": err_msg[:2000]},
            )
            conn.commit()
            err_cur.close()
        except Exception:  # noqa: BLE001
            pass
        finally:
            conn.close()
        return BoardRunResult(
            run_id=run_id, status="FAILED", as_of_date=as_of,
            dossier_count=len(rows), valid_dossier_count=0,
            invalid_dossier_count=0, published_count=0, skipped_count=0,
            eligible_count=0,
            genuine_eligible_count=genuine_eligible_count,
            cost_capped_count=cost_capped_count,
            eligibility_skipped_count=len(rows) - len(eligible_rows),
            eligibility_skip_breakdown=dict(skip_counts),
            candidate_mode=candidate_mode,
            estimated_agent_sessions=estimated_agent_sessions,
            error=err_msg,
        )

    if not allow_budget_override and max_candidates is None:
        logger.warning(
            "phase4 UNCAPPED_RUN run=%s candidates=%d estimated_agent_sessions=%d "
            "— running without max_candidates. Pass --max-candidates to control cost.",
            run_id, len(eligible_rows), estimated_agent_sessions,
        )

    inter_sem = asyncio.Semaphore(inter_dossier_concurrency)

    async def _run_with_inter_limit(did, sym, mkt, payload):
        async with inter_sem:
            return await _orchestrate_dossier(
                rest_creds=rest_creds,
                conn_factory=_conn_factory,
                run_id=run_id,
                dossier_id=did, symbol=sym, market_type=mkt, payload=payload,
                spec_concurrency=per_dossier_concurrency,
                max_rounds=max_rounds,
            )

    results: List[DossierResult] = await asyncio.gather(*[
        _run_with_inter_limit(did, sym, mkt, payload)
        for (did, sym, mkt, payload) in eligible_rows
    ])

    valid_results = [r for r in results if r.is_valid]
    invalid_results = [r for r in results if not r.is_valid]

    cur = conn.cursor()
    try:
        for r in valid_results:
            if not _gate_chair_specialist_count(cur, run_id, r.dossier_id):
                logger.warning(
                    "phase4 chair_gate_failed run=%s dossier=%s symbol=%s",
                    run_id, r.dossier_id, r.symbol,
                )
                continue
            assert r.chair is not None
            _persist_chair_verdict(
                cur, run_id, r.dossier_id, r.symbol, r.market_type,
                r.chair, r.interactions, r.primary_evidence_setup_event_id,
            )
        cur.execute(
            "UPDATE MIP.APP.PROPOSAL_BOARD_RUN SET RUN_STATUS='SPECIALISTS_DONE' "
            "WHERE RUN_ID=%(run_id)s",
            {"run_id": run_id},
        )
        cur.execute(
            "UPDATE MIP.APP.PROPOSAL_BOARD_RUN SET RUN_STATUS='CHAIR_DONE' "
            "WHERE RUN_ID=%(run_id)s",
            {"run_id": run_id},
        )
        _persist_final_slate(cur, run_id, max_proposals)

        # Short publication safety: explicit IBKR account-mode guard.
        # ADAPTER_MODE is overloaded ('LIVE' = use IBKR submit path, not real
        # money) so it is NOT used here. PROPOSE_SHORT is allowed only when
        # the configured portfolio has IBKR_ACCOUNT_MODE = 'PAPER'. Anything
        # else (REAL, UNKNOWN, missing config, missing portfolio_id) fails
        # closed with reason IBKR_ACCOUNT_MODE_NOT_PAPER.
        ibkr_account_mode = _get_ibkr_account_mode(cur, portfolio_id)
        short_publication_allowed = (ibkr_account_mode == "PAPER")
        if not short_publication_allowed:
            logger.warning(
                "phase4 short_publication_blocked run=%s portfolio_id=%s "
                "ibkr_account_mode=%s reason=IBKR_ACCOUNT_MODE_NOT_PAPER",
                run_id, portfolio_id, ibkr_account_mode,
            )

        published = 0
        skipped = 0
        if not dry_run:
            published, skipped = _publish_to_structural(
                cur, run_id, portfolio_id, max_proposals,
                short_publication_allowed=short_publication_allowed,
                ibkr_account_mode=ibkr_account_mode,
            )
        else:
            cur.execute(
                "SELECT COUNT(*) FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 "
                "WHERE RUN_ID=%(run_id)s",
                {"run_id": run_id},
            )
            slate_n = cur.fetchone()[0] or 0
            published = 0
            skipped = int(slate_n)

        final_status = "COMPLETE"
        if invalid_results:
            final_status = "PARTIAL_FAILURE" if valid_results else "FAILED"

        cur.execute(
            """
            UPDATE MIP.APP.PROPOSAL_BOARD_RUN
               SET RUN_STATUS = %(status)s,
                   FINISHED_AT = CURRENT_TIMESTAMP(),
                   FINAL_PROPOSAL_COUNT = %(pub)s,
                   ERROR_JSON = CASE
                       WHEN %(invalid_n)s > 0 THEN OBJECT_CONSTRUCT(
                           'reason_code','PARTIAL_FAILURE',
                           'invalid_dossiers', PARSE_JSON(%(invalid_json)s)
                       )
                       ELSE NULL
                   END
             WHERE RUN_ID = %(run_id)s
            """,
            {
                "run_id": run_id,
                "status": final_status,
                "pub": int(published),
                "invalid_n": len(invalid_results),
                "invalid_json": _jdump([
                    {
                        "dossier_id": r.dossier_id, "symbol": r.symbol,
                        "invalid_reason": r.invalid_reason,
                    } for r in invalid_results
                ]),
            },
        )
        conn.commit()

        # Collect summary stats for the run-end summary block.
        chair_propose_count = sum(
            1 for r in valid_results
            if r.chair and r.chair.final_action in {"PROPOSE_LONG", "PROPOSE_SHORT"}
        )
        props_executable_count = 0
        imported_to_lpa_count = 0
        try:
            cur.execute(
                """
                SELECT
                    SUM(IFF(EXECUTION_POLICY_STATUS = 'EXECUTABLE', 1, 0)) AS exec_ok,
                    SUM(IFF(stp.PROPOSAL_ID IS NOT NULL
                            AND EXISTS (SELECT 1 FROM MIP.LIVE.LIVE_ACTIONS la
                                        WHERE la.PROPOSAL_ID = stp.PROPOSAL_ID), 1, 0)) AS imported
                FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS stp
                WHERE stp.BOARD_RUN_ID = %(run_id)s
                """,
                {"run_id": run_id},
            )
            row_summary = cur.fetchone()
            if row_summary:
                props_executable_count = int(row_summary[0] or 0)
                imported_to_lpa_count = int(row_summary[1] or 0)
        except Exception:  # noqa: BLE001
            pass

        return BoardRunResult(
            run_id=run_id,
            status=final_status,
            as_of_date=as_of,
            dossier_count=len(rows),
            valid_dossier_count=len(valid_results),
            invalid_dossier_count=len(invalid_results),
            published_count=int(published),
            skipped_count=int(skipped),
            eligible_count=len(eligible_rows),
            genuine_eligible_count=genuine_eligible_count,
            cost_capped_count=cost_capped_count,
            eligibility_skipped_count=len(rows) - genuine_eligible_count,
            eligibility_skip_breakdown=dict(skip_counts),
            candidate_mode=candidate_mode,
            estimated_agent_sessions=estimated_agent_sessions,
            chair_propose_count=chair_propose_count,
            props_executable_count=props_executable_count,
            imported_to_lpa_count=imported_to_lpa_count,
            ibkr_account_mode=ibkr_account_mode,
            short_publication_allowed=bool(short_publication_allowed),
        )
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

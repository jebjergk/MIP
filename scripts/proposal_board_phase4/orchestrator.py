"""
Phase 4 Cortex Agentic Proposal Board — multi-stage orchestrator.

Stages:
  0  Snapshot dossiers from V_PROPOSAL_BOARD_SYMBOL_DOSSIER into
     PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT and stage the per-dossier
     evidence pack into PROPOSAL_BOARD_DOSSIER_PACK_CACHE.

  1  Run 5 specialist AI_COMPLETE calls per dossier in parallel (single-pass,
     bounded max_tokens, injected dossier evidence). Persist each position to
     PROPOSAL_BOARD_AGENT_OUTCOME_V2.

  2  (Retired) challenge/revision debate — removed for cost control.

  3  Chair AI_COMPLETE call — runs only if all 5 specialists are valid. Reads
     persisted positions and injected evidence; authors the final decision.
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
  * No deterministic fallback. No legacy setup-event zone backfill at publish.
  * Chair PROPOSE requires full proposed_trade_config anchored to dossier daily close.

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

from .complete_client import run_complete_json
from .prompts import (
    chair_system_prompt,
    chair_user_message,
    specialist_response_format,
    specialist_system_prompt,
    specialist_user_message,
)
from .review_eligibility import EligibilityDecision, evaluate_dossier_eligibility
from .ranking import rank_eligible_rows, score_candidate

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

_PROMPT_VERSION = "phase4_complete_v4_llama_ranked_commit"
_POLICY_VERSION = "phase4_complete_v2_no_legacy_fallback"
_MODEL_CONFIG_MODE = "symbol_dossier_ai_complete_bounded"

_CACHE_TTL_HOURS = 24
_DEFAULT_MAX_ROUNDS = 0  # debate retired; kept for CLI compat
_DEFAULT_MAX_PROPOSALS = 8

# Bounded AI_COMPLETE cost model: exactly 6 LLM calls per candidate (5 + chair).
_LLM_CALLS_PER_CANDIDATE = 6
_DEFAULT_MAX_CANDIDATES: Optional[int] = 20
_DEFAULT_MAX_LLM_CALLS_PER_RUN = 132  # 20 candidates × 6 AI_COMPLETE calls + headroom
_DEFAULT_DAILY_RUNS_PER_PORTFOLIO = 1
_DEFAULT_SPECIALIST_MODEL = "llama3.1-8b"
_DEFAULT_CHAIR_MODEL = "llama3.1-8b"
_DEFAULT_SPECIALIST_MAX_TOKENS = 2000
_DEFAULT_CHAIR_MAX_TOKENS = 4000
_DEFAULT_MAX_EVIDENCE_CHARS = 80_000
_DEFAULT_STATEMENT_TIMEOUT_SEC = 240
_DEFAULT_MAX_ENTRY_ZONE_DISTANCE_PCT = 3.0
_COMPLETE_MAX_RETRIES = 2

# Legacy CLI budget alias (LLM calls, not agent sessions).
_DEFAULT_DAILY_CALL_BUDGET: int = _DEFAULT_MAX_LLM_CALLS_PER_RUN


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


@dataclass
class Phase4RuntimeConfig:
    enabled: bool = True
    specialist_model: str = _DEFAULT_SPECIALIST_MODEL
    chair_model: str = _DEFAULT_CHAIR_MODEL
    max_llm_calls_per_run: int = _DEFAULT_MAX_LLM_CALLS_PER_RUN
    max_daily_runs_per_portfolio: int = _DEFAULT_DAILY_RUNS_PER_PORTFOLIO
    specialist_max_tokens: int = _DEFAULT_SPECIALIST_MAX_TOKENS
    chair_max_tokens: int = _DEFAULT_CHAIR_MAX_TOKENS
    max_evidence_chars: int = _DEFAULT_MAX_EVIDENCE_CHARS
    statement_timeout_sec: int = _DEFAULT_STATEMENT_TIMEOUT_SEC


class _LlmCallBudget:
    """Runtime guard — abort if more than max_calls AI_COMPLETE invocations occur."""

    def __init__(self, max_calls: int) -> None:
        self.max_calls = int(max_calls)
        self.used = 0
        self._lock = asyncio.Lock()

    async def consume(self, n: int = 1) -> None:
        async with self._lock:
            self.used += int(n)
            if self.used > self.max_calls:
                raise RuntimeError(
                    f"PHASE4_LLM_CALL_CAP_EXCEEDED used={self.used} max={self.max_calls}"
                )


def _config_bool(val: Optional[str], default: bool = True) -> bool:
    if val is None or str(val).strip() == "":
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


def _config_int(val: Optional[str], default: int) -> int:
    if val is None or str(val).strip() == "":
        return default
    try:
        return int(str(val).strip())
    except ValueError:
        return default


def _load_runtime_config(cur) -> Phase4RuntimeConfig:
    keys = (
        "PHASE4_ENABLED",
        "PHASE4_SPECIALIST_MODEL",
        "PHASE4_CHAIR_MODEL",
        "MAX_LLM_CALLS_PER_RUN",
        "PHASE4_MAX_DAILY_RUNS_PER_PORTFOLIO",
        "PHASE4_SPECIALIST_MAX_TOKENS",
        "PHASE4_CHAIR_MAX_TOKENS",
        "PHASE4_MAX_EVIDENCE_CHARS",
        "PHASE4_COMPLETE_STATEMENT_TIMEOUT_SEC",
    )
    cur.execute(
        "SELECT CONFIG_KEY, CONFIG_VALUE FROM MIP.APP.APP_CONFIG "
        "WHERE CONFIG_KEY IN (%s)"
        % ",".join("'" + k.replace("'", "''") + "'" for k in keys)
    )
    rows = {str(r[0]): r[1] for r in (cur.fetchall() or [])}
    return Phase4RuntimeConfig(
        enabled=_config_bool(rows.get("PHASE4_ENABLED"), True),
        specialist_model=(rows.get("PHASE4_SPECIALIST_MODEL") or _DEFAULT_SPECIALIST_MODEL).strip(),
        chair_model=(rows.get("PHASE4_CHAIR_MODEL") or _DEFAULT_CHAIR_MODEL).strip(),
        max_llm_calls_per_run=_config_int(
            rows.get("MAX_LLM_CALLS_PER_RUN"), _DEFAULT_MAX_LLM_CALLS_PER_RUN,
        ),
        max_daily_runs_per_portfolio=_config_int(
            rows.get("PHASE4_MAX_DAILY_RUNS_PER_PORTFOLIO"), _DEFAULT_DAILY_RUNS_PER_PORTFOLIO,
        ),
        specialist_max_tokens=_config_int(
            rows.get("PHASE4_SPECIALIST_MAX_TOKENS"), _DEFAULT_SPECIALIST_MAX_TOKENS,
        ),
        chair_max_tokens=_config_int(
            rows.get("PHASE4_CHAIR_MAX_TOKENS"), _DEFAULT_CHAIR_MAX_TOKENS,
        ),
        max_evidence_chars=_config_int(
            rows.get("PHASE4_MAX_EVIDENCE_CHARS"), _DEFAULT_MAX_EVIDENCE_CHARS,
        ),
        statement_timeout_sec=_config_int(
            rows.get("PHASE4_COMPLETE_STATEMENT_TIMEOUT_SEC"), _DEFAULT_STATEMENT_TIMEOUT_SEC,
        ),
    )


def _count_board_runs_today(cur, portfolio_id: Optional[int]) -> int:
    if portfolio_id is None:
        return 0
    cur.execute(
        """
        SELECT COUNT(*)
          FROM MIP.APP.PROPOSAL_BOARD_RUN
         WHERE PORTFOLIO_ID = %(pid)s
           AND STARTED_AT >= DATE_TRUNC('day', CURRENT_TIMESTAMP())
           AND RUN_STATUS NOT IN ('FAILED', 'FAILED_PRE_BOARD_GATE')
        """,
        {"pid": int(portfolio_id)},
    )
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _truncate_evidence_json(obj: Dict[str, Any], max_chars: int) -> Dict[str, Any]:
    """Bound injected evidence size to prevent runaway prompt tokens."""
    text = json.dumps(obj, default=str)
    if len(text) <= max_chars:
        return obj
    logger.warning(
        "phase4 evidence truncated chars=%d max=%d keys=%s",
        len(text), max_chars, list(obj.keys())[:12],
    )
    trimmed = dict(obj)
    for heavy in ("recent_bars", "candle_sequence", "structural_timeline_bars"):
        if heavy in trimmed:
            trimmed[heavy] = {"truncated": True, "reason": "PHASE4_MAX_EVIDENCE_CHARS"}
    text2 = json.dumps(trimmed, default=str)
    if len(text2) <= max_chars:
        return trimmed
    return {
        "truncated": True,
        "reason": "PHASE4_MAX_EVIDENCE_CHARS",
        "preview_keys": list(obj.keys()),
    }


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


def _sanitize_evidence_used(raw: Any) -> List[str]:
    """Specialists sometimes echo full EVIDENCE_JSON into evidence_used — normalize."""
    if isinstance(raw, list):
        out: List[str] = []
        for item in raw:
            if isinstance(item, str) and item.strip():
                out.append(item.strip()[:40])
            elif isinstance(item, dict):
                label = item.get("slice") or item.get("name") or next(iter(item.keys()), "history")
                out.append(str(label)[:40])
        if out:
            return out[:12]
    if isinstance(raw, dict):
        return [str(k)[:40] for k in list(raw.keys())[:12]]
    return ["history"]


def _sanitize_specialist_output(raw: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return raw
    out = dict(raw)
    out["evidence_used"] = _sanitize_evidence_used(out.get("evidence_used"))
    rationale = out.get("rationale")
    if isinstance(rationale, str) and len(rationale) > 4000:
        out["rationale"] = rationale[:4000]
    return out


def _dossier_current_price(payload: Dict[str, Any]) -> Optional[float]:
    if not isinstance(payload, dict):
        return None
    price_block = payload.get("price")
    if isinstance(price_block, dict):
        val = _to_float(price_block.get("current_price"))
        if val is not None and val > 0:
            return val
    return _to_float(payload.get("current_price"))


def _validate_chair_trade_geometry(
    final_action: str,
    final_direction: str,
    config: Dict[str, Any],
    dossier_payload: Dict[str, Any],
    *,
    max_zone_distance_pct: float = _DEFAULT_MAX_ENTRY_ZONE_DISTANCE_PCT,
) -> Optional[str]:
    """Return invalid_reason when PROPOSE geometry is missing or incoherent vs today's close."""
    if final_action not in {"PROPOSE_LONG", "PROPOSE_SHORT"}:
        return None

    required = (
        "entry_zone_low", "entry_zone_high", "invalidation_level",
        "invalidation_rule", "exit_profile", "size_treatment", "risk_class", "time_horizon",
    )
    for key in required:
        val = config.get(key)
        if val is None or (isinstance(val, str) and not str(val).strip()):
            return f"PROPOSED_TRADE_CONFIG_MISSING:{key}"

    ez_low = _to_float(config.get("entry_zone_low"))
    ez_high = _to_float(config.get("entry_zone_high"))
    inv = _to_float(config.get("invalidation_level"))
    if ez_low is None or ez_high is None or inv is None:
        return "PROPOSED_TRADE_CONFIG_NON_NUMERIC_GEOMETRY"
    if ez_low <= 0 or ez_high <= 0 or inv <= 0:
        return "PROPOSED_TRADE_CONFIG_NON_POSITIVE_GEOMETRY"
    if ez_low >= ez_high:
        return "PROPOSED_TRADE_CONFIG_INVERTED_ZONE"

    direction = (final_direction or "").upper()
    if direction == "LONG" and inv >= ez_low:
        return "PROPOSED_TRADE_CONFIG_INVALID_LONG_INVALIDATION"
    if direction == "SHORT" and inv <= ez_high:
        return "PROPOSED_TRADE_CONFIG_INVALID_SHORT_INVALIDATION"

    current = _dossier_current_price(dossier_payload)
    if current is None or current <= 0:
        return "DOSSIER_CURRENT_PRICE_MISSING"
    mid = (ez_low + ez_high) / 2.0
    dist_pct = abs(current - mid) / mid * 100.0
    if dist_pct > max_zone_distance_pct:
        return f"ENTRY_ZONE_STALE_VS_DAILY_CLOSE:{dist_pct:.2f}pct"

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


def _patch_eligibility_prescreen(
    cur,
    run_id: str,
    symbol: str,
    breakdown: Dict[str, Any],
    *,
    prescreen_rank: int,
    eligible: Optional[bool] = None,
    primary_reason_code: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """Merge prescreen scores into the existing eligibility audit row."""
    summary = dict(breakdown or {})
    summary["prescreen_rank"] = prescreen_rank
    sets = ["EVIDENCE_SUMMARY_JSON = PARSE_JSON(%(summary)s)"]
    params: Dict[str, Any] = {
        "run_id": run_id,
        "sym": (symbol or "")[:20],
        "summary": _jdump(summary),
    }
    if eligible is not None:
        sets.append("ELIGIBLE = %(elig)s")
        params["elig"] = bool(eligible)
    if primary_reason_code:
        sets.append("PRIMARY_REASON_CODE = %(primary)s")
        params["primary"] = primary_reason_code[:80]
    if notes is not None:
        sets.append("NOTES = %(notes)s")
        params["notes"] = _truncate(notes, 2000)
    cur.execute(
        f"""
        UPDATE MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY
           SET {", ".join(sets)}
         WHERE RUN_ID = %(run_id)s
           AND SYMBOL = %(sym)s
        """,
        params,
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
        DOSSIER_PAYLOAD_JSON:price:current_price_source::STRING,
        TRY_TO_DATE(DOSSIER_PAYLOAD_JSON:price:current_price_date::STRING),
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
# Stage 1 — specialist AI_COMPLETE calls (parallel per dossier)
# ---------------------------------------------------------------------------

async def _run_one_specialist(
    sem: asyncio.Semaphore,
    conn_factory,
    runtime_cfg: Phase4RuntimeConfig,
    call_budget: _LlmCallBudget,
    role: str,
    run_id: str,
    dossier_id: int,
    symbol: str,
    dossier_payload: Dict[str, Any],
) -> Tuple[str, SpecialistPosition, Dict[str, Any]]:
    """Run one specialist via bounded AI_COMPLETE with injected evidence."""
    per_role_cap = max(4000, runtime_cfg.max_evidence_chars // max(len(_REQUIRED_ROLES), 1))
    evidence = _truncate_evidence_json(
        _slice_payload_for_role(role, dossier_payload),
        per_role_cap,
    )
    user_msg = specialist_user_message(role, run_id, dossier_id, symbol, evidence)
    sys_prompt = specialist_system_prompt(role)
    response_format = specialist_response_format(role)

    async with sem:
        await call_budget.consume(1)
        t0 = time.monotonic()
        result = await run_complete_json(
            conn_factory,
            model=runtime_cfg.specialist_model,
            system_prompt=sys_prompt,
            user_message=user_msg,
            response_format=response_format,
            max_tokens=runtime_cfg.specialist_max_tokens,
            statement_timeout_sec=runtime_cfg.statement_timeout_sec,
            max_retries=_COMPLETE_MAX_RETRIES,
            label=f"specialist:{role}:{symbol}",
        )
        elapsed = time.monotonic() - t0

    parsed = result.parsed
    parsed = _sanitize_specialist_output(parsed)
    pos = _validate_specialist(role, parsed)
    if pos.invalid_reason and parsed is None and result.error:
        pos.invalid_reason = "COMPLETE_ERROR:" + str(result.error)[:200]

    response: Dict[str, Any] = {
        "mode": "AI_COMPLETE",
        "model": runtime_cfg.specialist_model,
        "usage": result.usage,
        "raw_text": result.raw_text[:4000] if result.raw_text else "",
        "error": result.error,
    }

    logger.info(
        "phase4 specialist done role=%s dossier=%s symbol=%s "
        "verdict=%s primary=%s elapsed=%.2fs invalid=%s model=%s",
        role, dossier_id, symbol, pos.verdict, pos.primary_reason_code,
        elapsed, pos.invalid_reason, runtime_cfg.specialist_model,
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


_CHAIR_EVIDENCE_KEYS = [
    "identity", "price", "recent_bars", "candle_sequence",
    "recent_price_action_summary", "levels", "structure", "regime",
    "long_pattern_signs", "short_pattern_signs",
    "setup_events_evidence_only", "invalidation_evidence",
    "history", "memory", "policy",
    "structural_timeline_summary", "structural_timeline_bars",
    "candle_psychology", "actionability_context", "market_structure_map",
]


def _slice_payload_for_chair(payload: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(payload, dict):
        return out
    for key in _CHAIR_EVIDENCE_KEYS:
        if key in payload:
            out[key] = payload[key]
    return out


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


def _primary_setup_event_id(payload: Dict[str, Any]) -> Optional[int]:
    raw = payload.get("primary_evidence_setup_event_id")
    if raw is None:
        return None
    try:
        n = int(raw)
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def _validate_chair(
    raw: Optional[Dict[str, Any]],
    dossier_payload: Optional[Dict[str, Any]] = None,
) -> ChairOutput:
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
    if invalid is None and isinstance(dossier_payload, dict):
        geom_invalid = _validate_chair_trade_geometry(
            final_action, final_direction, config or {}, dossier_payload,
        )
        if geom_invalid:
            invalid = geom_invalid

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


def _backfill_chair_from_evidence(
    raw: Optional[Dict[str, Any]],
    dossier_payload: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Copy deterministic MSM fields into chair JSON when the model omits them."""
    if not isinstance(raw, dict):
        return raw
    final_action = str(raw.get("final_action") or "").strip().upper()
    if final_action not in _CHAIR_STRUCTURE_SUMMARY_REQUIRED_ACTIONS:
        return raw
    msm = dossier_payload.get("market_structure_map") if isinstance(dossier_payload, dict) else None
    if not isinstance(msm, dict):
        return raw
    out = dict(raw)
    if not isinstance(out.get("market_structure_read"), dict):
        bos = msm.get("bos") if isinstance(msm.get("bos"), dict) else {}
        choch = msm.get("choch") if isinstance(msm.get("choch"), dict) else {}
        out["market_structure_read"] = {
            "primary_structure": msm.get("primary_structure"),
            "structure_health": msm.get("structure_health"),
            "current_phase": msm.get("current_phase"),
            "latest_structure_event": msm.get("latest_structure_event"),
            "bos_body_close_confirmed": bool(bos.get("body_close_confirmed")),
            "choch_detected": bool(choch.get("detected")),
            "structure_posture_hint": msm.get("structure_posture_hint"),
        }
    if not isinstance(out.get("body_wick_break_read"), str) or not out["body_wick_break_read"].strip():
        evt = msm.get("latest_structure_event") or "n/a"
        out["body_wick_break_read"] = (
            f"Latest structure event from market_structure_map: {evt}."
        )
    if not isinstance(out.get("structure_decision_reason"), str) or not out["structure_decision_reason"].strip():
        out["structure_decision_reason"] = (
            f"Structure health {msm.get('structure_health')} supports {final_action} posture."
        )
    evidence_used = out.get("evidence_used")
    if not isinstance(evidence_used, list):
        evidence_used = []
    if "market_structure_map" not in [str(x).lower() for x in evidence_used]:
        evidence_used = list(evidence_used) + ["market_structure_map"]
    out["evidence_used"] = evidence_used
    return out


def _normalize_chair_parsed(raw: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Flatten common mistral/llama chair JSON shapes before validation."""
    if not isinstance(raw, dict):
        return raw
    out = dict(raw)
    scores = out.get("scores")
    if isinstance(scores, dict):
        for key in ("confidence", "long_score", "short_score", "no_trade_score"):
            if out.get(key) is None and scores.get(key) is not None:
                out[key] = scores[key]
    ptc_raw = out.get("proposed_trade_config")
    ptc = dict(ptc_raw) if isinstance(ptc_raw, dict) else {}
    for key in (
        "thesis_label", "entry_zone_low", "entry_zone_high", "invalidation_level",
        "invalidation_rule", "exit_profile", "size_treatment", "risk_class", "time_horizon",
        "primary_evidence_setup_event_id",
    ):
        if ptc.get(key) is None and out.get(key) is not None:
            ptc[key] = out[key]
    final_action = str(out.get("final_action") or "").strip().upper()
    label = str(ptc.get("thesis_label") or "").strip()
    if final_action in _AGENTIC_THESIS_LABEL_REQUIRED and not label.upper().startswith("AGENTIC_"):
        ft = str(out.get("final_thesis") or "").strip()
        if ft.upper().startswith("AGENTIC_"):
            ptc["thesis_label"] = ft.split()[0][:80]
        elif final_action in {"PROPOSE_LONG", "WATCH_LONG", "WATCH_LONG_FAILURE"}:
            ptc["thesis_label"] = "AGENTIC_LONG"
        elif final_action in {"PROPOSE_SHORT", "WATCH_SHORT", "WATCH_SHORT_FAILURE"}:
            ptc["thesis_label"] = "AGENTIC_SHORT"
    out["proposed_trade_config"] = ptc
    eu = out.get("evidence_used")
    if isinstance(eu, dict):
        out["evidence_used"] = [str(k) for k in list(eu.keys())[:12]]
    elif not isinstance(eu, list):
        out["evidence_used"] = ["price", "levels"]
    return out


async def _orchestrate_dossier(
    conn_factory,
    runtime_cfg: Phase4RuntimeConfig,
    call_budget: _LlmCallBudget,
    run_id: str,
    dossier_id: int,
    symbol: str,
    market_type: str,
    payload: Dict[str, Any],
    spec_concurrency: int,
    max_rounds: int,  # noqa: ARG001 — debate retired; kept for signature compat
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

    # Stage 1 — parallel specialists (bounded AI_COMPLETE, injected evidence)
    sem = asyncio.Semaphore(spec_concurrency)
    coros = [
        _run_one_specialist(
            sem, conn_factory, runtime_cfg, call_budget,
            role, run_id, dossier_id, symbol, dossier_payload,
        )
        for role in _REQUIRED_ROLES
    ]
    raw_positions: Dict[str, SpecialistPosition] = {}
    raw_responses: Dict[str, Dict[str, Any]] = {}

    for fut in asyncio.as_completed(coros):
        role, pos, response = await fut
        raw_positions[role] = pos
        raw_responses[role] = response

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

    current_positions: Dict[str, SpecialistPosition] = dict(raw_positions)
    interactions: List[Dict[str, Any]] = []
    res.final_positions = current_positions
    res.interactions = interactions

    # Stage 2 — chair (single-pass AI_COMPLETE; no debate rounds)
    spec_summary = []
    for role in _REQUIRED_ROLES:
        p = current_positions.get(role)
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
    board_input = {
        "run_id": run_id,
        "dossier_id": dossier_id,
        "symbol": symbol,
        "market_type": market_type,
        "policy_flags": {
            "short_live_enabled": bool(res.short_live_enabled),
            "fx_live_enabled": bool(res.fx_live_enabled),
        },
        "primary_evidence_setup_event_id": primary_evidence_setup_event_id,
        "specialist_positions": spec_summary,
        "interactions_summary": interactions,
    }
    chair_evidence = _truncate_evidence_json(
        _slice_payload_for_chair(dossier_payload),
        runtime_cfg.max_evidence_chars,
    )
    chair_msg = chair_user_message(
        run_id, dossier_id, symbol, board_input, chair_evidence,
    )

    await call_budget.consume(1)
    # Chair uses plain AI_COMPLETE (no response_format). Snowflake structured
    # JSON mode is unreliable for mistral-large2; Python validators enforce the contract.
    chair_result = await run_complete_json(
        conn_factory,
        model=runtime_cfg.chair_model,
        system_prompt=chair_system_prompt(),
        user_message=chair_msg,
        response_format=None,
        max_tokens=runtime_cfg.chair_max_tokens,
        statement_timeout_sec=runtime_cfg.statement_timeout_sec,
        max_retries=_COMPLETE_MAX_RETRIES,
        label=f"chair:{symbol}",
    )
    chair_parsed = chair_result.parsed
    chair_resp: Dict[str, Any] = {
        "mode": "AI_COMPLETE",
        "model": runtime_cfg.chair_model,
        "usage": chair_result.usage,
        "raw_text": chair_result.raw_text[:8000] if chair_result.raw_text else "",
        "error": chair_result.error,
    }

    chair_parsed = _normalize_chair_parsed(chair_result.parsed)
    chair_parsed = _backfill_chair_from_evidence(chair_parsed, dossier_payload)
    chair = _validate_chair(chair_parsed, dossier_payload)
    res.chair = chair

    if chair.invalid_reason:
        res.is_valid = False
        res.invalid_reason = "INVALID_CHAIR:" + chair.invalid_reason

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

    if chair.final_action in {"PROPOSE_LONG", "PROPOSE_SHORT"}:
        if _primary_setup_event_id(dossier_payload) is None:
            missing_setup_reason = "MISSING_PRIMARY_SETUP_EVENT_ID"
            res.is_valid = False
            res.invalid_reason = "INVALID_CHAIR:" + missing_setup_reason

            def _persist_chair_no_setup(cn):
                cur = cn.cursor()
                try:
                    _persist_invalid_agent(
                        cur, run_id, dossier_id, _CHAIR_AGENT_NAME,
                        missing_setup_reason,
                        chair.structured_output or chair_resp,
                    )
                    cn.commit()
                finally:
                    cur.close()
            await asyncio.to_thread(lambda: _persist_chair_no_setup(conn_factory()))
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

    # ------------------------------------------------------------------
    # Incremental durability: flush the valid chair verdict to
    # THESIS_VERDICT the instant it is finalized, using this dossier's own
    # connection. The chair is the single most expensive call in the board.
    # Previously verdicts were only written in one batch AFTER every dossier
    # finished (see orchestrate end-of-run loop), so a crash anywhere in the
    # long chair stage discarded every chair that had already succeeded — and
    # the spend with it. Persisting per-dossier means a completed chair is
    # never lost. This is best-effort and non-fatal: the end-of-run loop is an
    # idempotent safety net (the INSERT is guarded by NOT EXISTS on
    # RUN_ID+DOSSIER_ID), and final-slate ranking/publication still run once at
    # the end of the board.
    def _persist_chair_now(cn):
        c = cn.cursor()
        try:
            if not _gate_chair_specialist_count(c, run_id, dossier_id):
                logger.warning(
                    "phase4 chair_gate_failed (incremental) run=%s dossier=%s "
                    "symbol=%s", run_id, dossier_id, symbol,
                )
                return
            _persist_chair_verdict(
                c, run_id, dossier_id, symbol, market_type,
                chair, res.interactions, res.primary_evidence_setup_event_id,
            )
            cn.commit()
        finally:
            c.close()

    try:
        await asyncio.to_thread(lambda: _persist_chair_now(conn_factory()))
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "phase4 incremental chair persist failed dossier=%s: %s "
            "(end-of-run batch loop will retry)", dossier_id, e,
        )

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
        WHERE NOT EXISTS (
            SELECT 1 FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT t
            WHERE t.RUN_ID = %(run_id)s AND t.DOSSIER_ID = %(did)s
        )
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


def _reap_zombie_board_runs(cur, max_age_minutes: int = 90) -> int:
    """Mark stale RUNNING board rows failed so Cockpit polling cannot latch them."""
    cur.execute(
        """
        UPDATE MIP.APP.PROPOSAL_BOARD_RUN
           SET RUN_STATUS = 'FAILED',
               FINISHED_AT = CURRENT_TIMESTAMP(),
               ERROR_JSON = OBJECT_CONSTRUCT(
                   'reason_code', 'ZOMBIE_RUN_REAPED',
                   'message', 'Run exceeded max age while RUNNING — marked failed by reaper.',
                   'max_age_minutes', %(max_age)s
               )
         WHERE RUN_STATUS = 'RUNNING'
           AND STARTED_AT < DATEADD('minute', -%(max_age)s, CURRENT_TIMESTAMP())
        """,
        {"max_age": max_age_minutes},
    )
    return cur.rowcount


def _normalize_published_broker_stops(cur, run_id: str) -> int:
    """Map structural invalidation (often inside entry zone) to broker stop outside zone.

    Setup events store PRICE_INVALIDATION_LEVEL at the structural break level,
    which frequently sits inside the entry band. LPA execution requires the
    broker stop below (LONG) or above (SHORT) the entry zone edges.
    """
    cur.execute(
        """
        UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
           SET PRICE_INVALIDATION_LEVEL = CASE
                   WHEN p.DIRECTION = 'LONG'
                        AND p.ENTRY_ZONE_LOW IS NOT NULL
                        AND (
                            p.PRICE_INVALIDATION_LEVEL IS NULL
                            OR p.PRICE_INVALIDATION_LEVEL >= p.ENTRY_ZONE_LOW
                        )
                       THEN p.ENTRY_ZONE_LOW * 0.985
                   WHEN p.DIRECTION = 'SHORT'
                        AND p.ENTRY_ZONE_HIGH IS NOT NULL
                        AND (
                            p.PRICE_INVALIDATION_LEVEL IS NULL
                            OR p.PRICE_INVALIDATION_LEVEL <= p.ENTRY_ZONE_HIGH
                        )
                       THEN p.ENTRY_ZONE_HIGH * 1.015
                   ELSE p.PRICE_INVALIDATION_LEVEL
               END,
               INVALIDATION_RULE = CASE
                   WHEN p.DIRECTION = 'LONG'
                        AND p.ENTRY_ZONE_LOW IS NOT NULL
                        AND p.PRICE_INVALIDATION_LEVEL IS NOT NULL
                        AND p.PRICE_INVALIDATION_LEVEL >= p.ENTRY_ZONE_LOW
                       THEN 'BROKER_STOP_BELOW_ZONE'
                   WHEN p.DIRECTION = 'SHORT'
                        AND p.ENTRY_ZONE_HIGH IS NOT NULL
                        AND p.PRICE_INVALIDATION_LEVEL IS NOT NULL
                        AND p.PRICE_INVALIDATION_LEVEL <= p.ENTRY_ZONE_HIGH
                       THEN 'BROKER_STOP_ABOVE_ZONE'
                   ELSE COALESCE(p.INVALIDATION_RULE, 'AGENTIC_INVALIDATION')
               END
         WHERE p.BOARD_RUN_ID = %(run_id)s
        """,
        {"run_id": run_id},
    )
    return cur.rowcount


def _finalize_unpublished_slate(
    cur,
    run_id: str,
    portfolio_id: Optional[int],
    short_publication_allowed: bool,
    ibkr_account_mode: str,
    *,
    dry_run: bool = False,
) -> int:
    """Mark any remaining PENDING slate rows as terminal (never leave limbo).

    Called after publish attempt and on dry_run so PROPOSE_* rows cannot sit
    in PUBLICATION_STATUS='PENDING' forever.
    """
    cur.execute(
        """
        UPDATE MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
           SET PUBLICATION_STATUS = 'SKIPPED_GUARDRAIL',
               PUBLICATION_ERROR_JSON = CASE
                   WHEN %(dry_run)s
                   THEN OBJECT_CONSTRUCT(
                       'reason', 'DRY_RUN_NO_PUBLISH',
                       'reason_detail', 'Dry run — STRUCTURAL_TRADE_PROPOSALS insert skipped.'
                   )
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
            "dry_run": bool(dry_run),
        },
    )
    return int(cur.rowcount or 0)


def _warn_limbo_propose_slate(cur, run_id: str) -> None:
    """Log loudly if any PROPOSE_* slate row is still PENDING after finalize."""
    cur.execute(
        """
        SELECT SYMBOL, FINAL_ACTION, PUBLICATION_STATUS
          FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2
         WHERE RUN_ID = %(run_id)s
           AND FINAL_ACTION IN ('PROPOSE_LONG', 'PROPOSE_SHORT')
           AND PUBLICATION_STATUS = 'PENDING'
         ORDER BY SYMBOL
        """,
        {"run_id": run_id},
    )
    rows = cur.fetchall() or []
    if rows:
        logger.error(
            "phase4 LIMBO_PROPOSE_SLATE run=%s count=%s symbols=%s",
            run_id,
            len(rows),
            [f"{r[0]}:{r[1]}" for r in rows],
        )


def _supersede_stale_proposals(
    cur,
    run_id: str,
    portfolio_id: Optional[int],
) -> int:
    """Expire older same-symbol PROPOSED rows before publishing this run.

    Prevents C1g duplicate_collision from silently dropping fresh chair PROPOSE_*
    verdicts when the operator re-runs the board the same day.
    """
    cur.execute(
        """
        UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
           SET STATUS = 'EXPIRED',
               EXECUTION_POLICY_REASON = COALESCE(
                   p.EXECUTION_POLICY_REASON,
                   'SUPERSEDED_BY_NEWER_BOARD_RUN'
               )
         WHERE p.STATUS = 'PROPOSED'
           AND p.BOARD_RUN_ID IS NOT NULL
           AND p.BOARD_RUN_ID <> %(run_id)s
           AND COALESCE(p.IS_RESEARCH_ONLY, FALSE) = FALSE
           AND (%(portfolio_id)s IS NULL
                OR p.PORTFOLIO_ID = %(portfolio_id)s
                OR p.PORTFOLIO_ID IS NULL)
           AND EXISTS (
               SELECT 1
                 FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
                 JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
                   ON s.RUN_ID = fs.RUN_ID AND s.DOSSIER_ID = fs.DOSSIER_ID
                WHERE fs.RUN_ID = %(run_id)s
                  AND fs.PUBLICATION_STATUS = 'PENDING'
                  AND fs.FINAL_ACTION IN ('PROPOSE_LONG', 'PROPOSE_SHORT')
                  AND s.SYMBOL = p.SYMBOL
           )
        """,
        {"run_id": run_id, "portfolio_id": portfolio_id},
    )
    return int(cur.rowcount or 0)


def _publish_to_structural(
    cur, run_id: str, portfolio_id: Optional[int], max_proposals: int,
    short_publication_allowed: bool = False,
    ibkr_account_mode: str = "UNKNOWN",
) -> Tuple[int, int, int]:
    """Returns (published_count, skipped_count, research_published_count).

    ``research_published_count`` is the number of WATCH_LONG / WATCH_SHORT
    verdicts published into STRUCTURAL_TRADE_PROPOSALS as RESEARCH_ONLY rows.
    These are operator-review items: visible in LPA but hard-gated out of
    LIVE_ACTIONS (import requires EXECUTION_POLICY_STATUS='EXECUTABLE') so they
    can never execute.

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
    superseded = _supersede_stale_proposals(cur, run_id, portfolio_id)
    if superseded:
        logger.info(
            "phase4 supersede_stale_proposals run=%s expired=%d older PROPOSED rows",
            run_id, superseded,
        )

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
            LEFT(v.PTC:invalidation_rule::STRING, 30),
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
            -- WATCH_LONG / WATCH_SHORT are published as RESEARCH_ONLY so the
            -- operator can review the directional read in LPA; they can NEVER
            -- import into LIVE_ACTIONS (gated to EXECUTABLE-only) or execute.
            CASE
                WHEN v.FINAL_ACTION IN ('WATCH_LONG','WATCH_SHORT')
                    THEN 'RESEARCH_ONLY'
                WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT s.SHORT_LIVE_ENABLED
                    THEN 'POLICY_BLOCKED'
                WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND s.SHORT_LIVE_ENABLED
                     AND NOT %(short_pub_allowed)s
                    THEN 'BROKER_BLOCKED'
                ELSE 'EXECUTABLE'
            END,
            CASE
                WHEN v.FINAL_ACTION IN ('WATCH_LONG','WATCH_SHORT')
                    THEN 'WATCH_RESEARCH_ONLY'
                WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT s.SHORT_LIVE_ENABLED
                    THEN 'SHORT_LIVE_DISABLED'
                WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND s.SHORT_LIVE_ENABLED
                     AND NOT %(short_pub_allowed)s
                    THEN 'IBKR_NOT_PAPER'
                ELSE NULL
            END,
            IFF(v.FINAL_ACTION IN ('WATCH_LONG','WATCH_SHORT')
                OR (v.FINAL_ACTION = 'PROPOSE_SHORT'
                    AND (NOT s.SHORT_LIVE_ENABLED OR NOT %(short_pub_allowed)s)),
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
          AND (
              v.FINAL_ACTION IN ('WATCH_LONG', 'WATCH_SHORT')
              OR (
                  TRY_TO_DOUBLE(v.PTC:entry_zone_low::STRING) IS NOT NULL
                  AND TRY_TO_DOUBLE(v.PTC:entry_zone_high::STRING) IS NOT NULL
                  AND TRY_TO_DOUBLE(v.PTC:invalidation_level::STRING) IS NOT NULL
                  AND NULLIF(TRIM(v.PTC:invalidation_rule::STRING), '') IS NOT NULL
              )
          )
          -- PROPOSE_* requires chair-authored geometry — no legacy setup-event fallbacks.
          -- Phase 4 taxonomy v2: hard STOCK-only publication guard.
          AND s.MARKET_TYPE = 'STOCK'
          -- Allow PROPOSE_SHORT unconditionally; policy recorded in EXECUTION_POLICY_STATUS.
          -- WATCH_LONG / WATCH_SHORT publish as RESEARCH_ONLY (see policy CASE above).
          AND v.FINAL_ACTION IN ('PROPOSE_LONG', 'PROPOSE_SHORT', 'WATCH_LONG', 'WATCH_SHORT')
          AND NOT EXISTS (
              SELECT 1 FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
              WHERE p.STATUS = 'PROPOSED' AND p.SYMBOL = s.SYMBOL
                AND (%(portfolio_id)s IS NULL OR p.PORTFOLIO_ID = %(portfolio_id)s OR p.PORTFOLIO_ID IS NULL)
                -- A PROPOSE_* intent is blocked only by an existing EXECUTABLE-intent
                -- (non research-only) proposal; a research-only WATCH row must never
                -- block a genuine executable proposal. A WATCH_* intent is blocked by
                -- ANY existing PROPOSED row for the symbol (executable or research) so
                -- research items never pile up and always defer to a real proposal.
                AND (
                    (v.FINAL_ACTION IN ('PROPOSE_LONG','PROPOSE_SHORT')
                     AND COALESCE(p.IS_RESEARCH_ONLY, FALSE) = FALSE)
                    OR v.FINAL_ACTION IN ('WATCH_LONG','WATCH_SHORT')
                )
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

    # Any PENDING row that did not insert is marked SKIPPED_GUARDRAIL.
    _finalize_unpublished_slate(
        cur, run_id, portfolio_id, short_publication_allowed, ibkr_account_mode,
    )

    # Phase 3: post-INSERT geometry validation.
    # Normalize structural invalidation to broker stop outside entry zone first.
    normalized = _normalize_published_broker_stops(cur, run_id)
    if normalized:
        logger.info(
            "phase4 normalize_broker_stops run=%s adjusted=%d proposals",
            run_id, normalized,
        )

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
           SET EXECUTION_POLICY_STATUS = 'GEOMETRY_INVALID',
               EXECUTION_POLICY_REASON = 'ENTRY_ZONE_STALE_VS_DAILY_CLOSE',
               IS_RESEARCH_ONLY = TRUE
          FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
         WHERE p.BOARD_RUN_ID = %(run_id)s
           AND s.RUN_ID = p.BOARD_RUN_ID
           AND s.DOSSIER_ID = p.BOARD_DOSSIER_ID
           AND p.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
           AND p.ENTRY_ZONE_LOW IS NOT NULL
           AND p.ENTRY_ZONE_HIGH IS NOT NULL
           AND TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:price:current_price::STRING) IS NOT NULL
           AND ABS(
               TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:price:current_price::STRING)
               - (p.ENTRY_ZONE_LOW + p.ENTRY_ZONE_HIGH) / 2
           ) / NULLIF((p.ENTRY_ZONE_LOW + p.ENTRY_ZONE_HIGH) / 2, 0) * 100
               > %(max_zone_dist_pct)s
        """,
        {"run_id": run_id, "max_zone_dist_pct": _DEFAULT_MAX_ENTRY_ZONE_DISTANCE_PCT},
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

    # Defensive non-STOCK guard: any structural proposal whose board dossier is
    # non-STOCK can never be EXECUTABLE or live-tradeable. Non-STOCK is excluded
    # pre-agent and by the publish WHERE, but this forces the invariant
    # unconditionally so it can never import into LIVE_ACTIONS.
    cur.execute(
        """
        UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
           SET EXECUTION_POLICY_STATUS = 'POLICY_BLOCKED',
               EXECUTION_POLICY_REASON = 'NON_STOCK_NOT_TRADEABLE',
               IS_RESEARCH_ONLY = TRUE
          FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
         WHERE p.BOARD_RUN_ID = %(run_id)s
           AND s.RUN_ID = p.BOARD_RUN_ID
           AND s.DOSSIER_ID = p.BOARD_DOSSIER_ID
           AND COALESCE(s.MARKET_TYPE, 'UNKNOWN') <> 'STOCK'
        """,
        {"run_id": run_id},
    )

    # published = genuine EXECUTABLE-intent proposals (PROPOSE_LONG/PROPOSE_SHORT).
    # research_published = WATCH_LONG/WATCH_SHORT rows published as RESEARCH_ONLY
    # (operator-review only; never executable). Tracked separately so
    # FINAL_PROPOSAL_COUNT keeps reflecting tradeable proposals only.
    cur.execute(
        "SELECT COUNT(*) FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS "
        "WHERE BOARD_RUN_ID = %(run_id)s "
        "AND BOARD_FINAL_VERDICT IN ('PROPOSE_LONG','PROPOSE_SHORT')",
        {"run_id": run_id},
    )
    published = cur.fetchone()[0] or 0
    cur.execute(
        "SELECT COUNT(*) FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS "
        "WHERE BOARD_RUN_ID = %(run_id)s "
        "AND BOARD_FINAL_VERDICT IN ('WATCH_LONG','WATCH_SHORT')",
        {"run_id": run_id},
    )
    research_published = cur.fetchone()[0] or 0
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
    return int(published), int(skipped), int(research_published)


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


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
    research_published_count: int = 0
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
    # Pre-board fail-closed safety gates (run before Cortex fan-out).
    pre_board_stock_only_gate: str = "NOT_RUN"
    pre_board_market_type_integrity_gate: str = "NOT_RUN"
    pre_board_gate_checks: Dict[str, int] = field(default_factory=dict)
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Pre-board fail-closed safety gates (run BEFORE Cortex fan-out)
# ---------------------------------------------------------------------------
#
# These gates are a REGRESSION GUARD around the runtime pre-agent
# MARKET_TYPE='STOCK' filter — not a replacement for it. They mirror the
# hard checks in:
#   MIP/SQL/smoke/47_phase4_stock_only_regression.sql   (R1-R4)
#   MIP/SQL/smoke/48_market_type_null_integrity.sql      (C1/C1b/C2/C3)
# plus a direct in-memory assertion that the candidate set about to be
# fanned out to Cortex contains zero non-STOCK rows. If any HARD check
# fails the board aborts before any Cortex/agent call — no proposals are
# published, nothing is imported to LPA, real-money execution is untouched.

# SQL block returning one row per check. R1/R2 are scoped to THIS run
# (no agent outcomes exist yet at pre-board time, so they assert the
# current run has not already leaked non-STOCK into the agent tables).
_PRE_BOARD_GATE_SQL = """
WITH ref AS (
    SELECT SYM, COUNT(DISTINCT MT) AS NMT
    FROM (
        SELECT UPPER(SYMBOL) AS SYM, MARKET_TYPE AS MT FROM MIP.APP.INGEST_UNIVERSE        WHERE MARKET_TYPE IS NOT NULL
        UNION SELECT UPPER(SYMBOL), MARKET_TYPE FROM MIP.APP.STRUCTURAL_SETUP_EVENTS WHERE MARKET_TYPE IS NOT NULL
        UNION SELECT UPPER(SYMBOL), MARKET_TYPE FROM MIP.APP.PORTFOLIO_TRADES        WHERE MARKET_TYPE IS NOT NULL
    )
    GROUP BY SYM
)
SELECT 'R1_NON_STOCK_SENT_TO_AGENTS' AS CHECK_NAME, COUNT(DISTINCT ao.DOSSIER_ID) AS N
FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 ao
JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
  ON s.RUN_ID = ao.RUN_ID AND s.DOSSIER_ID = ao.DOSSIER_ID
WHERE ao.RUN_ID = %(run_id)s AND COALESCE(s.MARKET_TYPE,'STOCK') <> 'STOCK'
UNION ALL
SELECT 'R2_NON_STOCK_AGENT_OUTCOMES', COUNT(*)
FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 ao
JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
  ON s.RUN_ID = ao.RUN_ID AND s.DOSSIER_ID = ao.DOSSIER_ID
WHERE ao.RUN_ID = %(run_id)s AND COALESCE(s.MARKET_TYPE,'STOCK') <> 'STOCK'
UNION ALL
SELECT 'R3_NON_STOCK_EXECUTABLE_PROPOSALS', COUNT(*)
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
LEFT JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
  ON s.RUN_ID = p.BOARD_RUN_ID AND s.DOSSIER_ID = p.BOARD_DOSSIER_ID
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se_ev
  ON se_ev.SETUP_EVENT_ID = p.PRIMARY_EVIDENCE_SETUP_EVENT_ID
WHERE p.STATUS = 'PROPOSED'
  AND COALESCE(p.EXECUTION_POLICY_STATUS,'EXECUTABLE') = 'EXECUTABLE'
  AND COALESCE(p.IS_RESEARCH_ONLY, FALSE) = FALSE
  AND COALESCE(s.MARKET_TYPE, se_ev.MARKET_TYPE, 'STOCK') <> 'STOCK'
UNION ALL
SELECT 'R4_NON_STOCK_TRADEABLE_LIVE_ACTIONS', COUNT(*)
FROM MIP.LIVE.LIVE_ACTIONS la
WHERE la.MARKET_TYPE IS NOT NULL AND la.MARKET_TYPE <> 'STOCK'
  AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION',
                    'OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED',
                    'PENDING_SUBMIT','OPEN','FILLED')
UNION ALL
SELECT 'C1_LIVE_ACTIONS_RESOLVABLE_BUT_NULL', COUNT(*)
FROM MIP.LIVE.LIVE_ACTIONS la
JOIN ref r ON r.SYM = UPPER(la.SYMBOL) AND r.NMT = 1
WHERE la.MARKET_TYPE IS NULL
UNION ALL
SELECT 'C1b_LIVE_ACTIONS_REVIEW_REQUIRED', COUNT(*)
FROM MIP.LIVE.LIVE_ACTIONS la
LEFT JOIN ref r ON r.SYM = UPPER(la.SYMBOL) AND r.NMT = 1
WHERE la.MARKET_TYPE IS NULL AND r.SYM IS NULL
UNION ALL
SELECT 'C2_NON_STOCK_EXECUTABLE_PROPOSALS', COUNT(*)
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
LEFT JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
  ON s.RUN_ID = p.BOARD_RUN_ID AND s.DOSSIER_ID = p.BOARD_DOSSIER_ID
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se_ev
  ON se_ev.SETUP_EVENT_ID = p.PRIMARY_EVIDENCE_SETUP_EVENT_ID
WHERE p.STATUS = 'PROPOSED'
  AND COALESCE(p.EXECUTION_POLICY_STATUS,'EXECUTABLE') = 'EXECUTABLE'
  AND COALESCE(p.IS_RESEARCH_ONLY, FALSE) = FALSE
  AND COALESCE(s.MARKET_TYPE, se_ev.MARKET_TYPE, 'STOCK') <> 'STOCK'
UNION ALL
SELECT 'C3_NON_STOCK_TRADEABLE_LIVE_ACTIONS', COUNT(*)
FROM MIP.LIVE.LIVE_ACTIONS la
WHERE la.MARKET_TYPE IS NOT NULL AND la.MARKET_TYPE <> 'STOCK'
  AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION',
                    'OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED',
                    'PENDING_SUBMIT','OPEN','FILLED')
"""


def _run_pre_board_gates(
    cur,
    run_id: str,
    eligible_rows: List[Tuple[int, str, str, Dict[str, Any]]],
) -> Dict[str, Any]:
    """Run the pre-board fail-closed safety gates.

    Returns a dict with:
      - stock_only_gate: "PASS"/"FAIL"
      - market_type_integrity_gate: "PASS"/"FAIL"
      - checks: {CHECK_NAME: count}
      - passed: bool (both hard gates pass)
      - failed_checks: [CHECK_NAME, ...]
    """
    checks: Dict[str, int] = {}

    # In-memory assertion: the candidate set about to reach Cortex must be
    # 100% STOCK. This is the direct current-run pre-agent guard.
    non_stock_in_fanout = sum(
        1 for (_did, _sym, mkt, _payload) in eligible_rows
        if (mkt or "").upper() != "STOCK"
    )
    checks["CURRENT_RUN_NONSTOCK_IN_FANOUT"] = non_stock_in_fanout

    cur.execute(_PRE_BOARD_GATE_SQL, {"run_id": run_id})
    for row in cur.fetchall():
        checks[str(row[0])] = int(row[1] or 0)

    # Hard checks per gate. C1b is informational only (review backlog).
    stock_only_hard = [
        "CURRENT_RUN_NONSTOCK_IN_FANOUT",
        "R1_NON_STOCK_SENT_TO_AGENTS",
        "R2_NON_STOCK_AGENT_OUTCOMES",
        "R3_NON_STOCK_EXECUTABLE_PROPOSALS",
        "R4_NON_STOCK_TRADEABLE_LIVE_ACTIONS",
    ]
    integrity_hard = [
        "C1_LIVE_ACTIONS_RESOLVABLE_BUT_NULL",
        "C2_NON_STOCK_EXECUTABLE_PROPOSALS",
        "C3_NON_STOCK_TRADEABLE_LIVE_ACTIONS",
    ]
    stock_only_fail = [c for c in stock_only_hard if checks.get(c, 0) > 0]
    integrity_fail = [c for c in integrity_hard if checks.get(c, 0) > 0]

    return {
        "stock_only_gate": "FAIL" if stock_only_fail else "PASS",
        "market_type_integrity_gate": "FAIL" if integrity_fail else "PASS",
        "checks": checks,
        "passed": not (stock_only_fail or integrity_fail),
        "failed_checks": stock_only_fail + integrity_fail,
    }


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
    run_id = str(uuid.uuid4())
    as_of = as_of_date or _date.today()

    conn = _connect()
    started_at_log = ""
    runtime_cfg = Phase4RuntimeConfig()
    try:
        cur = conn.cursor()
        try:
            runtime_cfg = _load_runtime_config(cur)
            if not runtime_cfg.enabled:
                conn.close()
                return BoardRunResult(
                    run_id=run_id, status="FAILED", as_of_date=as_of,
                    dossier_count=0, valid_dossier_count=0,
                    invalid_dossier_count=0, published_count=0, skipped_count=0,
                    error="PHASE4_DISABLED: set PHASE4_ENABLED=true in APP_CONFIG to run",
                )
            if portfolio_id is not None:
                runs_today = _count_board_runs_today(cur, portfolio_id)
                if runs_today >= runtime_cfg.max_daily_runs_per_portfolio:
                    conn.close()
                    return BoardRunResult(
                        run_id=run_id, status="FAILED", as_of_date=as_of,
                        dossier_count=0, valid_dossier_count=0,
                        invalid_dossier_count=0, published_count=0, skipped_count=0,
                        error=(
                            f"PHASE4_DAILY_RUN_CAP: portfolio={portfolio_id} "
                            f"runs_today={runs_today} cap={runtime_cfg.max_daily_runs_per_portfolio}"
                        ),
                    )
            reaped = _reap_zombie_board_runs(cur)
            if reaped:
                logger.warning(
                    "phase4 reaped %d zombie RUNNING board rows before start run=%s",
                    reaped, run_id,
                )
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
                        "inference": "AI_COMPLETE",
                        "specialist_model": runtime_cfg.specialist_model,
                        "chair_model": runtime_cfg.chair_model,
                        "llm_calls_per_candidate": _LLM_CALLS_PER_CANDIDATE,
                        "max_llm_calls_per_run": runtime_cfg.max_llm_calls_per_run,
                        "specialist_max_tokens": runtime_cfg.specialist_max_tokens,
                        "chair_max_tokens": runtime_cfg.chair_max_tokens,
                        "debate_rounds": 0,
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
    #
    # STOCK-ONLY HARD GATE (pre-agent): the Phase 4 agentic candidate universe
    # is STOCK only. Any MARKET_TYPE != 'STOCK' (FX, ETF, etc.) is excluded here,
    # BEFORE the candidate cap, ranking, budget estimation, Cortex agent fan-out,
    # and chair validation. Non-STOCK rows therefore consume ZERO agent sessions.
    # They are audited as NON_STOCK_EXCLUDED_PRE_AGENT for full traceability but
    # are never added to eligible_rows. This is not overridable by --symbols.
    operator_override = bool(symbols_filter)
    eligibility_by_dossier: Dict[int, EligibilityDecision] = {}
    eligible_rows: List[Tuple[int, str, str, Dict[str, Any]]] = []
    skip_counts: Dict[str, int] = {}
    elig_cur = conn.cursor()
    try:
        for did, sym, mkt, payload in rows:
            # STOCK-only hard gate — runs before any agent-bound processing.
            if (mkt or "").upper() != "STOCK":
                decision = EligibilityDecision(
                    symbol=sym,
                    market_type=mkt,
                    eligible=False,
                    primary_reason_code="NON_STOCK_EXCLUDED_PRE_AGENT",
                    signal_flags={"non_stock_excluded": True},
                    evidence_summary={
                        "market_type": mkt,
                        "note": "Phase 4 agentic universe is STOCK only; "
                                "excluded before agent panel (zero agent cost).",
                    },
                )
                eligibility_by_dossier[did] = decision
                try:
                    _persist_eligibility(
                        elig_cur, run_id, as_of, portfolio_id, did, decision,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "phase4 non_stock persist failed dossier=%s symbol=%s mkt=%s: %s",
                        did, sym, mkt, e,
                    )
                skip_counts["NON_STOCK_EXCLUDED_PRE_AGENT"] = (
                    skip_counts.get("NON_STOCK_EXCLUDED_PRE_AGENT", 0) + 1
                )
                continue
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

    # Stage 0.6 — SCORE_RANKED pre-screen + max_candidates cap.
    # Deterministic appeal scoring (ranking.py) then top-N to AI_COMPLETE panel.
    genuine_eligible_count = len(eligible_rows)
    cost_capped_count = 0

    scored_rows = rank_eligible_rows(eligible_rows, as_of)
    candidate_mode = "SCORE_RANKED" if scored_rows else "UNCAPPED"
    if scored_rows:
        top_preview = [
            (
                f"{sym}(rank={breakdown.get('combined_rank_score', pts):.0f}"
                f"/exec={breakdown.get('execution_readiness_score', 0):.0f})"
            )
            for _did, sym, _mkt, _pl, pts, breakdown in scored_rows[:10]
        ]
        logger.info(
            "phase4 prescreen_score run=%s eligible=%d top10=%s",
            run_id, len(scored_rows), top_preview,
        )

    prescreen_cur = conn.cursor()
    try:
        for rank_idx, (_did, sym, _mkt, _payload, _pts, breakdown) in enumerate(scored_rows, 1):
            try:
                _patch_eligibility_prescreen(
                    prescreen_cur, run_id, sym, breakdown,
                    prescreen_rank=rank_idx,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "phase4 prescreen patch failed run=%s symbol=%s: %s",
                    run_id, sym, e,
                )
        conn.commit()
    finally:
        try:
            prescreen_cur.close()
        except Exception:  # noqa: BLE001
            pass

    eligible_rows = [
        (did, sym, mkt, payload) for did, sym, mkt, payload, _pts, _bd in scored_rows
    ]

    if max_candidates is not None and len(eligible_rows) > max_candidates:
        kept = eligible_rows[:max_candidates]
        skipped_cap = eligible_rows[max_candidates:]
        cost_capped_count = len(skipped_cap)
        candidate_mode = "SCORE_RANKED"

        logger.info(
            "phase4 SCORE_RANKED cap run=%s max_candidates=%d "
            "kept=%s skipped=%d",
            run_id, max_candidates,
            [sym for _, sym, _, _ in kept],
            cost_capped_count,
        )

        cap_elig_cur = conn.cursor()
        try:
            cap_rank_start = max_candidates + 1
            for cap_offset, (did, sym, mkt, _payload) in enumerate(skipped_cap):
                skip_counts["NOT_SENT_TO_AGENT_PANEL_COST_CAP"] = (
                    skip_counts.get("NOT_SENT_TO_AGENT_PANEL_COST_CAP", 0) + 1
                )
                cap_bd = next(
                    (bd for d, s, _m, _p, _pt, bd in scored_rows if d == did and s == sym),
                    {},
                )
                try:
                    _patch_eligibility_prescreen(
                        cap_elig_cur, run_id, sym, cap_bd,
                        prescreen_rank=cap_rank_start + cap_offset,
                        eligible=False,
                        primary_reason_code="NOT_SENT_TO_AGENT_PANEL_COST_CAP",
                        notes="Skipped by max_candidates cap after execution-readiness ranking.",
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "phase4 cost_cap patch failed run=%s symbol=%s: %s",
                        run_id, sym, e,
                    )
            conn.commit()
        finally:
            try:
                cap_elig_cur.close()
            except Exception:  # noqa: BLE001
                pass
        eligible_rows = kept

    # Budget preflight — exact LLM call count (6 per candidate). No tool loops.
    max_llm_cap = min(
        runtime_cfg.max_llm_calls_per_run,
        daily_call_budget if daily_call_budget else runtime_cfg.max_llm_calls_per_run,
    )
    estimated_llm_calls = len(eligible_rows) * _LLM_CALLS_PER_CANDIDATE
    logger.info(
        "phase4 budget_preflight run=%s candidates=%d "
        "estimated_llm_calls=%d max_llm_cap=%d allow_override=%s",
        run_id, len(eligible_rows),
        estimated_llm_calls, max_llm_cap, allow_budget_override,
    )
    if not allow_budget_override and estimated_llm_calls > max_llm_cap:
        err_msg = (
            f"phase4 budget_exceeded: estimated_llm_calls={estimated_llm_calls} "
            f"> max_llm_cap={max_llm_cap}. "
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
            estimated_agent_sessions=estimated_llm_calls,
            error=err_msg,
        )

    if not allow_budget_override and max_candidates is None and len(eligible_rows) > 0:
        logger.warning(
            "phase4 UNCAPPED_RUN run=%s candidates=%d estimated_llm_calls=%d "
            "— running without max_candidates. Pass --max-candidates to control cost.",
            run_id, len(eligible_rows), estimated_llm_calls,
        )

    call_budget = _LlmCallBudget(estimated_llm_calls)

    # ── Pre-board fail-closed safety gates — LAST checkpoint before LLM fan-out ──
    # Regression guard around the runtime STOCK-only filter. If any hard
    # check fails we abort here: zero Cortex calls, no publication, no LPA
    # import, real-money untouched. Mirrors smoke 47 (R1-R4) + smoke 48.
    gate_cur = conn.cursor()
    try:
        gate = _run_pre_board_gates(gate_cur, run_id, eligible_rows)
    finally:
        try:
            gate_cur.close()
        except Exception:  # noqa: BLE001
            pass
    logger.info(
        "phase4 pre_board_gates run=%s stock_only=%s market_type_integrity=%s checks=%s",
        run_id, gate["stock_only_gate"], gate["market_type_integrity_gate"],
        gate["checks"],
    )
    if not gate["passed"]:
        err_msg = (
            "phase4 PRE_BOARD_GATE_FAILED — aborted before Cortex fan-out. "
            f"stock_only_gate={gate['stock_only_gate']} "
            f"market_type_integrity_gate={gate['market_type_integrity_gate']} "
            f"failed_checks={gate['failed_checks']}"
        )
        logger.error(err_msg)
        try:
            err_cur = conn.cursor()
            err_cur.execute(
                """
                UPDATE MIP.APP.PROPOSAL_BOARD_RUN
                   SET RUN_STATUS = 'FAILED',
                       FINISHED_AT = CURRENT_TIMESTAMP(),
                       ERROR_JSON = OBJECT_CONSTRUCT(
                           'reason_code', 'PRE_BOARD_GATE_FAILED',
                           'message', %(msg)s,
                           'stock_only_gate', %(sog)s,
                           'market_type_integrity_gate', %(mtig)s,
                           'failed_checks', PARSE_JSON(%(failed)s),
                           'check_counts', PARSE_JSON(%(checks)s)
                       )
                 WHERE RUN_ID = %(run_id)s
                """,
                {
                    "run_id": run_id,
                    "msg": err_msg[:2000],
                    "sog": gate["stock_only_gate"],
                    "mtig": gate["market_type_integrity_gate"],
                    "failed": _jdump(gate["failed_checks"]),
                    "checks": _jdump(gate["checks"]),
                },
            )
            conn.commit()
            err_cur.close()
        except Exception:  # noqa: BLE001
            logger.exception("phase4 pre_board_gate audit write failed run=%s", run_id)
        finally:
            conn.close()
        return BoardRunResult(
            run_id=run_id, status="FAILED_PRE_BOARD_GATE", as_of_date=as_of,
            dossier_count=len(rows), valid_dossier_count=0,
            invalid_dossier_count=0, published_count=0, skipped_count=0,
            eligible_count=len(eligible_rows),
            genuine_eligible_count=genuine_eligible_count,
            cost_capped_count=cost_capped_count,
            eligibility_skipped_count=len(rows) - len(eligible_rows),
            eligibility_skip_breakdown=dict(skip_counts),
            candidate_mode=candidate_mode,
            estimated_agent_sessions=estimated_llm_calls,
            pre_board_stock_only_gate=gate["stock_only_gate"],
            pre_board_market_type_integrity_gate=gate["market_type_integrity_gate"],
            pre_board_gate_checks=gate["checks"],
            error=err_msg,
        )

    inter_sem = asyncio.Semaphore(inter_dossier_concurrency)

    async def _run_with_inter_limit(did, sym, mkt, payload):
        async with inter_sem:
            return await _orchestrate_dossier(
                conn_factory=_conn_factory,
                runtime_cfg=runtime_cfg,
                call_budget=call_budget,
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
        research_published = 0
        if dry_run:
            skipped = _finalize_unpublished_slate(
                cur, run_id, portfolio_id, short_publication_allowed,
                ibkr_account_mode, dry_run=True,
            )
        else:
            try:
                published, skipped, research_published = _publish_to_structural(
                    cur, run_id, portfolio_id, max_proposals,
                    short_publication_allowed=short_publication_allowed,
                    ibkr_account_mode=ibkr_account_mode,
                )
            except Exception as pub_exc:
                logger.exception("phase4 publish failed run=%s", run_id)
                _finalize_unpublished_slate(
                    cur, run_id, portfolio_id, short_publication_allowed,
                    ibkr_account_mode,
                )
                raise pub_exc
        _warn_limbo_propose_slate(cur, run_id)

        final_status = "COMPLETE"
        if invalid_results:
            final_status = "PARTIAL_FAILURE" if valid_results else "FAILED"

        cur.execute(
            """
            UPDATE MIP.APP.PROPOSAL_BOARD_RUN
               SET RUN_STATUS = %(status)s,
                   FINISHED_AT = CURRENT_TIMESTAMP(),
                   CANDIDATE_COUNT = %(cand_n)s,
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
                "cand_n": int(len(eligible_rows)),
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
            research_published_count=int(research_published),
            eligible_count=len(eligible_rows),
            genuine_eligible_count=genuine_eligible_count,
            cost_capped_count=cost_capped_count,
            eligibility_skipped_count=len(rows) - genuine_eligible_count,
            eligibility_skip_breakdown=dict(skip_counts),
            candidate_mode=candidate_mode,
            estimated_agent_sessions=estimated_llm_calls,
            chair_propose_count=chair_propose_count,
            props_executable_count=props_executable_count,
            imported_to_lpa_count=imported_to_lpa_count,
            ibkr_account_mode=ibkr_account_mode,
            short_publication_allowed=bool(short_publication_allowed),
            pre_board_stock_only_gate=gate["stock_only_gate"],
            pre_board_market_type_integrity_gate=gate["market_type_integrity_gate"],
            pre_board_gate_checks=gate["checks"],
        )
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

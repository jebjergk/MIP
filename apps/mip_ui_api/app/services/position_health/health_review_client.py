"""
Daily Position Health V1 - Cortex Agent client wrapper.

Thin specialization of shadow_cortex_client.run_agent_object for the
POSITION_HEALTH_REVIEW_AGENT object. Responsibilities:
  - call the agent with a single user message containing the JSON payload
  - extract assistant text
  - parse strict JSON (with permissive fence stripping)
  - return a typed ShadowReviewResult, including PARSE_ERROR / API_ERROR status
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, Optional

from app.committee.shadow_cortex_client import extract_agent_text, run_agent_object

from .types import PositionPayload, ShadowReviewResult, ShadowRunStatus

logger = logging.getLogger(__name__)

DEFAULT_AGENT_NAME = "POSITION_HEALTH_REVIEW_AGENT"
DEFAULT_AGENT_TIMEOUT_SEC = 90.0


_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)


def _strip_fences(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    m = _FENCE_RE.search(raw)
    if m:
        return m.group(1).strip()
    return raw


def _try_parse_json(raw: str) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    candidate = _strip_fences(raw)
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        # Last resort: locate the first { ... } block.
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start >= 0 and end > start:
            try:
                parsed = json.loads(candidate[start : end + 1])
            except json.JSONDecodeError:
                return None
        else:
            return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _truncate(value: Optional[str], limit: int) -> Optional[str]:
    if value is None:
        return None
    s = str(value)
    return s[:limit]


def _classify_disagreement(
    real_verdict: Optional[str],
    shadow_verdict: Optional[str],
    shadow_action_bias: Optional[str],
) -> Optional[str]:
    if not real_verdict or not shadow_verdict:
        return None
    if real_verdict == shadow_verdict:
        return "AGREE"
    if real_verdict == "KEEP" and shadow_verdict == "EXIT_REVIEW":
        return "REAL_KEEP_SHADOW_EXIT"
    if real_verdict == "EXIT_REVIEW" and shadow_verdict == "KEEP":
        return "REAL_EXIT_SHADOW_KEEP"
    if real_verdict == "WATCH" and shadow_verdict == "EXIT_REVIEW":
        return "REAL_WATCH_SHADOW_EXIT"
    if real_verdict == "EXIT_REVIEW" and shadow_verdict == "WATCH":
        return "REAL_EXIT_SHADOW_WATCH"
    if real_verdict == "WATCH" and shadow_verdict == "KEEP":
        return "REAL_WATCH_SHADOW_KEEP"
    if real_verdict == "KEEP" and shadow_verdict == "WATCH":
        return "REAL_KEEP_SHADOW_WATCH"
    return "OTHER"


async def review_one_position(
    payload: PositionPayload,
    account: str,
    user: str,
    private_key_path: str,
    agent_name: str = DEFAULT_AGENT_NAME,
    timeout: float = DEFAULT_AGENT_TIMEOUT_SEC,
) -> ShadowReviewResult:
    """
    Send one PositionPayload to the POSITION_HEALTH_REVIEW_AGENT and
    return a typed ShadowReviewResult.

    Always returns a ShadowReviewResult (never raises). Failures land
    in shadow_run_status / shadow_run_error so persistence sees them.
    """
    started = time.monotonic()
    started_ts = datetime.utcnow()
    user_message = json.dumps(payload.to_user_message(), default=str)

    result = ShadowReviewResult(
        position_episode_key=payload.position_episode_key,
        portfolio_id=payload.portfolio_id,
        symbol=payload.symbol,
        as_of_date=payload.as_of_date,
        shadow_run_ts=started_ts,
        shadow_run_status=ShadowRunStatus.PENDING,
    )

    try:
        response = await run_agent_object(
            account=account,
            user=user,
            private_key_path=private_key_path,
            agent_name=agent_name,
            messages=[{"role": "user", "content": user_message}],
            timeout=timeout,
        )
    except Exception as e:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        logger.warning(
            "position_health.review: API_ERROR symbol=%s key=%s err=%s",
            payload.symbol, payload.position_episode_key, e,
        )
        result.shadow_run_status = ShadowRunStatus.API_ERROR
        result.shadow_run_error = _truncate(str(e), 1900)
        result.shadow_run_elapsed_ms = elapsed_ms
        return result

    elapsed_ms = int((time.monotonic() - started) * 1000)
    result.shadow_run_elapsed_ms = elapsed_ms

    raw_text = extract_agent_text(response)
    parsed = _try_parse_json(raw_text)
    if parsed is None:
        logger.warning(
            "position_health.review: PARSE_ERROR symbol=%s key=%s text_head=%r",
            payload.symbol, payload.position_episode_key, (raw_text or "")[:300],
        )
        result.shadow_run_status = ShadowRunStatus.PARSE_ERROR
        result.shadow_run_error = _truncate(
            f"agent returned non-JSON text: {raw_text[:500]}", 1900
        )
        result.rationale_text = _truncate(raw_text, 1900)
        return result

    # Vocabulary normalization (defensive; agent should already comply)
    shadow_verdict = (parsed.get("shadow_verdict") or "").upper().strip() or None
    shadow_action_bias = (parsed.get("shadow_action_bias") or "").upper().strip() or None
    shadow_thesis_status = (parsed.get("shadow_thesis_status") or "").upper().strip() or None
    shadow_severity = (parsed.get("shadow_severity") or "").upper().strip() or None

    # Enforce: EXIT_NOW only valid when shadow_verdict == EXIT_REVIEW.
    if shadow_action_bias == "EXIT_NOW" and shadow_verdict != "EXIT_REVIEW":
        shadow_action_bias = "HOLD"

    real_verdict = (
        payload.real_verdict_context.verdict if payload.real_verdict_context else None
    )

    result.shadow_run_status = ShadowRunStatus.SUCCESS
    result.shadow_verdict = shadow_verdict
    result.shadow_action_bias = shadow_action_bias
    result.shadow_thesis_status = shadow_thesis_status
    result.shadow_severity = shadow_severity
    result.primary_reason_code = _truncate(parsed.get("primary_reason_code"), 60)
    result.primary_reason_text = _truncate(parsed.get("primary_reason_text"), 480)
    result.observation_summary = _truncate(parsed.get("observation_summary"), 480)
    result.verdict_summary = _truncate(parsed.get("verdict_summary"), 480)
    result.why_summary = _truncate(parsed.get("why_summary"), 480)
    result.rationale_text = _truncate(parsed.get("rationale_text"), 1900)
    result.rationale_json = parsed
    if real_verdict and shadow_verdict:
        result.agrees_with_real = (real_verdict == shadow_verdict)
        result.disagreement_class = _classify_disagreement(
            real_verdict, shadow_verdict, shadow_action_bias
        )
    return result

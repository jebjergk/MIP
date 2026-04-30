"""
Board explanation loader.

Resolves the agentic proposal board audit trail behind a single
`proposal_id` lookup. Joins:

  - MIP.APP.STRUCTURAL_TRADE_PROPOSALS  (board lineage columns)
  - MIP.APP.PROPOSAL_BOARD_RUN          (model_config, prompt/policy version)
  - MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME(four specialist verdicts +
                                         per-row mode = 'cortex' or
                                         'deterministic_fallback')
  - MIP.APP.PROPOSAL_BOARD_INTERACTION  (orchestrator / specialist
                                         disagreement records, if any)
  - MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT
                                        (chair text and structured
                                         comparative reasoning)

Sprint 3 of the Phase 2 plan: read-only API + UI component for the
cockpit. Strictly no writes to any board persistence table; this is
purely a read surface for the published row.

Fail-soft:
  - Returns `available=False` when the proposal does not exist or
    has no board lineage (e.g. a legacy expired row from before the
    Phase 1 cutover or board-NULL pre-cutover row that was retired).
  - Returns `available=False` on any DB error and surfaces the
    error message so the cockpit can render a non-blocking notice.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from app.db import fetch_all, get_connection, serialize_row

logger = logging.getLogger(__name__)


# --- SQL --------------------------------------------------------------------


# We pull the published proposal row + board lineage + the run-level
# policy/prompt version so the panel can show *which* board produced
# this verdict (mode mix, model, chair template version).
_HEAD_SQL = """
    SELECT
        p.PROPOSAL_ID,
        p.SYMBOL,
        p.DIRECTION,
        p.SETUP_FAMILY,
        p.STATUS,
        p.CREATED_AT,
        p.BOARD_RUN_ID,
        p.BOARD_CANDIDATE_ID,
        p.BOARD_FINAL_RANK,
        p.BOARD_FINAL_VERDICT,
        p.BOARD_PRIMARY_REASON_CODE,
        p.BOARD_REASON_CODES,
        p.BOARD_RATIONALE,
        r.AS_OF_DATE        AS RUN_AS_OF_DATE,
        r.STARTED_AT        AS RUN_STARTED_AT,
        r.FINISHED_AT       AS RUN_FINISHED_AT,
        r.RUN_STATUS        AS RUN_STATUS,
        r.CANDIDATE_COUNT   AS RUN_CANDIDATE_COUNT,
        r.FINAL_PROPOSAL_COUNT AS RUN_FINAL_COUNT,
        r.MODEL_CONFIG_JSON AS RUN_MODEL_CONFIG_JSON,
        r.PROMPT_VERSION    AS RUN_PROMPT_VERSION,
        r.POLICY_VERSION    AS RUN_POLICY_VERSION
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
    LEFT JOIN MIP.APP.PROPOSAL_BOARD_RUN r
      ON r.RUN_ID = p.BOARD_RUN_ID
    WHERE p.PROPOSAL_ID = %(proposal_id)s
"""


_AGENT_OUTCOMES_SQL = """
    SELECT
        AGENT_NAME,
        VERDICT,
        PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE,
        CONFIDENCE,
        RATIONALE_TEXT,
        STRUCTURED_OUTPUT_JSON
    FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME
    WHERE RUN_ID = %(run_id)s
      AND CANDIDATE_ID = %(candidate_id)s
    ORDER BY AGENT_NAME
"""


_ORCHESTRATOR_VERDICT_SQL = """
    SELECT
        FINAL_RANK,
        FINAL_VERDICT,
        PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE,
        FINAL_RATIONALE,
        WHY_SELECTED_OR_REJECTED,
        COMPARATIVE_REASONING_JSON
    FROM MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT
    WHERE RUN_ID = %(run_id)s
      AND CANDIDATE_ID = %(candidate_id)s
    LIMIT 1
"""


_INTERACTIONS_SQL = """
    SELECT
        SOURCE_AGENT,
        TARGET_AGENT,
        TOPIC,
        DISAGREEMENT_TYPE,
        DISAGREEMENT_TEXT,
        RESPONSE_TEXT,
        RESOLVED_FLAG,
        CREATED_AT
    FROM MIP.APP.PROPOSAL_BOARD_INTERACTION
    WHERE RUN_ID = %(run_id)s
      AND CANDIDATE_ID = %(candidate_id)s
    ORDER BY CREATED_AT
"""


# --- Helpers ----------------------------------------------------------------


def _query(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            return [serialize_row(r) for r in fetch_all(cur)]
        finally:
            cur.close()
    finally:
        conn.close()


def _to_json_obj(v: Any) -> Any:
    """Coerce a Snowflake VARIANT column (dict, list, JSON string, or
    None) into a Python value. Snowflake's connector returns JSON-typed
    columns either as Python types directly or as JSON strings depending
    on the driver path; we normalise to native types so downstream
    serialisation is uniform."""
    if v is None:
        return None
    if isinstance(v, (dict, list, int, float, bool)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return v
    return v


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _agent_outcome_view(row: Dict[str, Any]) -> Dict[str, Any]:
    structured = _to_json_obj(row.get("STRUCTURED_OUTPUT_JSON")) or {}
    if not isinstance(structured, dict):
        structured = {}
    mode = structured.get("mode") if isinstance(structured.get("mode"), str) else None
    model = structured.get("model") if isinstance(structured.get("model"), str) else None
    return {
        "agent_name":             row.get("AGENT_NAME"),
        "verdict":                row.get("VERDICT"),
        "primary_reason_code":    row.get("PRIMARY_REASON_CODE"),
        "secondary_reason_code":  row.get("SECONDARY_REASON_CODE"),
        "confidence":             _safe_float(row.get("CONFIDENCE")),
        "rationale_text":         row.get("RATIONALE_TEXT"),
        "mode":                   mode,
        "model":                  model,
        "concern_flags":          _to_json_obj(structured.get("concern_flags")) or [],
        "supporting_evidence_keys": _to_json_obj(structured.get("supporting_evidence_keys")) or [],
        # Cortex audit fields are useful when debugging a single row;
        # the UI hides these by default but the JSON is persisted for
        # operator inspection.
        "cortex_parse_succeeded":   bool(structured.get("cortex_parse_succeeded")) if structured else False,
        "cortex_validation_passed": bool(structured.get("cortex_validation_passed")) if structured else False,
    }


def _interaction_view(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source_agent":      row.get("SOURCE_AGENT"),
        "target_agent":      row.get("TARGET_AGENT"),
        "topic":             row.get("TOPIC"),
        "disagreement_type": row.get("DISAGREEMENT_TYPE"),
        "disagreement_text": row.get("DISAGREEMENT_TEXT"),
        "response_text":     row.get("RESPONSE_TEXT"),
        "resolved":          bool(row.get("RESOLVED_FLAG")),
        "created_at":        row.get("CREATED_AT"),
    }


def _disagreement_summary(comparative: Dict[str, Any]) -> Dict[str, Any]:
    """Pull the support/concern/reject counts out of the chair's
    comparative reasoning JSON (the chair already computed them at
    board run time, so we don't redo the math in Python).

    Falls back to None values if the chair JSON is missing the
    expected keys (older runs may have a different shape).
    """
    if not isinstance(comparative, dict):
        comparative = {}
    return {
        "support_count":  comparative.get("support_count"),
        "concern_count":  comparative.get("concern_count"),
        "reject_count":   comparative.get("reject_count"),
        "avg_confidence": _safe_float(comparative.get("avg_confidence")),
        "warning_flags":  comparative.get("warning_flags") or [],
        "specialist_breakdown": comparative.get("specialist_breakdown"),
        "chair_template_version": comparative.get("chair_template_version"),
        "mode_line": comparative.get("mode_line"),
    }


# --- Public entry point -----------------------------------------------------


def load_board_explanation(proposal_id: int) -> Dict[str, Any]:
    """Build the read-only board explanation payload for a single
    `proposal_id`.

    The contract is `available: bool`. When `True`, the rest of the
    payload is populated. When `False`, `note` carries a short reason
    so the cockpit can render a non-blocking notice instead of the
    panel content.
    """
    try:
        head_rows = _query(_HEAD_SQL, {"proposal_id": int(proposal_id)})
    except Exception as exc:
        logger.warning("board_explanation: head query failed for %s: %s", proposal_id, exc)
        return {
            "available": False,
            "proposal_id": int(proposal_id),
            "note": f"head_query_failed: {exc}",
        }

    if not head_rows:
        return {
            "available": False,
            "proposal_id": int(proposal_id),
            "note": "Proposal not found.",
        }
    head = head_rows[0]
    run_id = head.get("BOARD_RUN_ID")
    candidate_id = head.get("BOARD_CANDIDATE_ID")
    if not run_id or candidate_id is None:
        return {
            "available": False,
            "proposal_id": int(proposal_id),
            "note": "Proposal has no board lineage (pre-cutover legacy or non-board path).",
        }

    try:
        outcomes = _query(_AGENT_OUTCOMES_SQL, {"run_id": run_id, "candidate_id": int(candidate_id)})
    except Exception as exc:
        logger.warning("board_explanation: outcomes query failed for %s: %s", proposal_id, exc)
        outcomes = []

    try:
        verdict_rows = _query(_ORCHESTRATOR_VERDICT_SQL, {"run_id": run_id, "candidate_id": int(candidate_id)})
    except Exception as exc:
        logger.warning("board_explanation: chair verdict query failed for %s: %s", proposal_id, exc)
        verdict_rows = []

    try:
        interactions = _query(_INTERACTIONS_SQL, {"run_id": run_id, "candidate_id": int(candidate_id)})
    except Exception as exc:
        logger.warning("board_explanation: interactions query failed for %s: %s", proposal_id, exc)
        interactions = []

    chair_row = verdict_rows[0] if verdict_rows else {}
    comparative = _to_json_obj(chair_row.get("COMPARATIVE_REASONING_JSON")) or {}
    if not isinstance(comparative, dict):
        comparative = {}

    return {
        "available": True,
        "proposal_id": int(proposal_id),
        "symbol": head.get("SYMBOL"),
        "direction": head.get("DIRECTION"),
        "setup_family": head.get("SETUP_FAMILY"),
        "status": head.get("STATUS"),
        "created_at": head.get("CREATED_AT"),
        "board_run_id": run_id,
        "board_candidate_id": int(candidate_id),
        "board_final_rank": head.get("BOARD_FINAL_RANK"),
        "board_final_verdict": head.get("BOARD_FINAL_VERDICT"),
        "board_primary_reason_code": head.get("BOARD_PRIMARY_REASON_CODE"),
        "board_reason_codes": _to_json_obj(head.get("BOARD_REASON_CODES")) or [],
        "board_rationale": head.get("BOARD_RATIONALE"),
        "specialists": [_agent_outcome_view(o) for o in outcomes],
        "chair": {
            "final_rank":               chair_row.get("FINAL_RANK"),
            "final_verdict":            chair_row.get("FINAL_VERDICT"),
            "primary_reason_code":      chair_row.get("PRIMARY_REASON_CODE"),
            "secondary_reason_code":    chair_row.get("SECONDARY_REASON_CODE"),
            "final_rationale":          chair_row.get("FINAL_RATIONALE"),
            "why_selected_or_rejected": chair_row.get("WHY_SELECTED_OR_REJECTED"),
            "comparative_reasoning":    comparative,
        },
        "disagreement": {
            **_disagreement_summary(comparative),
            "interactions": [_interaction_view(i) for i in interactions],
        },
        "run": {
            "run_id":              run_id,
            "as_of_date":          head.get("RUN_AS_OF_DATE"),
            "started_at":          head.get("RUN_STARTED_AT"),
            "finished_at":         head.get("RUN_FINISHED_AT"),
            "run_status":          head.get("RUN_STATUS"),
            "candidate_count":     head.get("RUN_CANDIDATE_COUNT"),
            "final_proposal_count":head.get("RUN_FINAL_COUNT"),
            "model_config":        _to_json_obj(head.get("RUN_MODEL_CONFIG_JSON")),
            "prompt_version":      head.get("RUN_PROMPT_VERSION"),
            "policy_version":      head.get("RUN_POLICY_VERSION"),
        },
    }

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
        p.BOARD_DOSSIER_ID,
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


# Phase 4 chair output for proposals published by the agentic board.
# Phase 3 published rows have a non-null BOARD_CANDIDATE_ID and rely on
# PROPOSAL_BOARD_AGENT_OUTCOME / _ORCHESTRATOR_VERDICT, which Phase 4
# does NOT populate. Phase 4 published rows have a BOARD_DOSSIER_ID and
# the chair output lives on PROPOSAL_BOARD_THESIS_VERDICT joined to the
# dossier snapshot for structural evidence (broken resistance / nearest
# levels / continuation_quality).
#
# We fetch this proposal-time row (matched by run_id + dossier_id) so
# the LPA panel shows the verdict that produced the published proposal,
# not the latest symbol verdict (which may be later and lineage-shifted
# into WATCH_LONG_FAILURE).
_PHASE4_CHAIR_SQL = """
    SELECT
        tv.FINAL_ACTION,
        tv.FINAL_DIRECTION,
        tv.PRIMARY_REASON_CODE,
        tv.SECONDARY_REASON_CODE,
        tv.FINAL_THESIS,
        tv.WHY_NOT_OPPOSITE,
        tv.WHY_NOT_NO_TRADE,
        tv.RISK_TREATMENT,
        tv.CHAIR_OUTPUT_JSON,
        tv.CHAIR_OUTPUT_JSON:thesis_health::STRING               AS THESIS_HEALTH,
        tv.CHAIR_OUTPUT_JSON:prior_thesis_reference              AS PRIOR_THESIS_REF,
        tv.CHAIR_OUTPUT_JSON:actionability_summary               AS ACTIONABILITY_SUMMARY,
        tv.CHAIR_OUTPUT_JSON:evidence_used                       AS EVIDENCE_USED,
        tv.CHAIR_OUTPUT_JSON:unresolved_disagreement::BOOLEAN    AS UNRESOLVED_DISAGREEMENT,
        tv.CREATED_AT                                            AS RUN_AT,
        ds.AS_OF_DATE                                            AS AS_OF_DATE,
        ds.DOSSIER_PAYLOAD_JSON:structural_timeline_summary:current_range_position_pct::FLOAT AS RANGE_PCT,
        ds.DOSSIER_PAYLOAD_JSON:structural_timeline_summary:trend_shape_class::STRING         AS TREND_SHAPE,
        ds.DOSSIER_PAYLOAD_JSON:candle_psychology:recent_cluster::STRING                      AS RECENT_CLUSTER,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:continuation_quality::STRING            AS CONTINUATION_QUALITY,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:resistance_overhead_risk::STRING        AS RESISTANCE_OVERHEAD_RISK,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:broken_resistance_support_confidence::FLOAT AS BROKEN_R_CONFIDENCE_AC,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_price::FLOAT  AS BROKEN_R_LEVEL,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_low::FLOAT    AS BROKEN_R_ZONE_LOW,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_high::FLOAT   AS BROKEN_R_ZONE_HIGH,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:confidence::FLOAT   AS BROKEN_R_CONFIDENCE,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:role::STRING        AS BROKEN_R_ROLE,
        ds.DOSSIER_PAYLOAD_JSON:levels:nearest_support:level_price::FLOAT     AS NEAREST_SUPPORT,
        ds.DOSSIER_PAYLOAD_JSON:levels:nearest_resistance:level_price::FLOAT  AS NEAREST_RESISTANCE
    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv
    JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT ds
      ON ds.RUN_ID = tv.RUN_ID AND ds.DOSSIER_ID = tv.DOSSIER_ID
    WHERE tv.RUN_ID = %(run_id)s
      AND tv.DOSSIER_ID = %(dossier_id)s
    LIMIT 1
"""


# Latest lineage-aware Phase 4 verdict for a single proposal. Mirrors
# the cockpit's `_load_phase4_health` dispatch logic but inlined as a
# single Snowflake call so the LPA can show the current thesis-health
# update alongside the proposal-time chair output.
_PHASE4_LATEST_LINKED_FOR_PROPOSAL_SQL = """
    SELECT
        tv.FINAL_ACTION,
        tv.FINAL_DIRECTION,
        tv.PRIMARY_REASON_CODE,
        tv.SECONDARY_REASON_CODE,
        tv.FINAL_THESIS,
        tv.WHY_NOT_OPPOSITE,
        tv.WHY_NOT_NO_TRADE,
        tv.RISK_TREATMENT,
        tv.CHAIR_OUTPUT_JSON,
        tv.CHAIR_OUTPUT_JSON:thesis_health::STRING               AS THESIS_HEALTH,
        tv.CHAIR_OUTPUT_JSON:prior_thesis_reference              AS PRIOR_THESIS_REF,
        tv.CHAIR_OUTPUT_JSON:actionability_summary               AS ACTIONABILITY_SUMMARY,
        tv.CHAIR_OUTPUT_JSON:evidence_used                       AS EVIDENCE_USED,
        tv.CHAIR_OUTPUT_JSON:unresolved_disagreement::BOOLEAN    AS UNRESOLVED_DISAGREEMENT,
        tv.CREATED_AT                                            AS RUN_AT,
        ds.AS_OF_DATE                                            AS AS_OF_DATE,
        ds.DOSSIER_PAYLOAD_JSON:structural_timeline_summary:current_range_position_pct::FLOAT AS RANGE_PCT,
        ds.DOSSIER_PAYLOAD_JSON:structural_timeline_summary:trend_shape_class::STRING         AS TREND_SHAPE,
        ds.DOSSIER_PAYLOAD_JSON:candle_psychology:recent_cluster::STRING                      AS RECENT_CLUSTER,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:continuation_quality::STRING            AS CONTINUATION_QUALITY,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:resistance_overhead_risk::STRING        AS RESISTANCE_OVERHEAD_RISK,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:broken_resistance_support_confidence::FLOAT AS BROKEN_R_CONFIDENCE_AC,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_price::FLOAT  AS BROKEN_R_LEVEL,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_low::FLOAT    AS BROKEN_R_ZONE_LOW,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_high::FLOAT   AS BROKEN_R_ZONE_HIGH,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:confidence::FLOAT   AS BROKEN_R_CONFIDENCE,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:role::STRING        AS BROKEN_R_ROLE,
        ds.DOSSIER_PAYLOAD_JSON:levels:nearest_support:level_price::FLOAT     AS NEAREST_SUPPORT,
        ds.DOSSIER_PAYLOAD_JSON:levels:nearest_resistance:level_price::FLOAT  AS NEAREST_RESISTANCE
    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv
    JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT ds
      ON ds.RUN_ID = tv.RUN_ID AND ds.DOSSIER_ID = tv.DOSSIER_ID
    WHERE tv.MARKET_TYPE = 'STOCK'
      AND tv.CREATED_AT >= DATEADD('day', -14, CURRENT_TIMESTAMP())
      AND tv.CHAIR_OUTPUT_JSON:prior_thesis_reference:proposal_id::INT = %(proposal_id)s
    QUALIFY ROW_NUMBER() OVER (ORDER BY tv.CREATED_AT DESC) = 1
"""


_PHASE4_LATEST_BY_SYMBOL_FOR_LPA_SQL = """
    SELECT
        tv.FINAL_ACTION,
        tv.FINAL_DIRECTION,
        tv.PRIMARY_REASON_CODE,
        tv.SECONDARY_REASON_CODE,
        tv.FINAL_THESIS,
        tv.WHY_NOT_OPPOSITE,
        tv.WHY_NOT_NO_TRADE,
        tv.RISK_TREATMENT,
        tv.CHAIR_OUTPUT_JSON,
        tv.CHAIR_OUTPUT_JSON:thesis_health::STRING               AS THESIS_HEALTH,
        tv.CHAIR_OUTPUT_JSON:prior_thesis_reference              AS PRIOR_THESIS_REF,
        tv.CHAIR_OUTPUT_JSON:actionability_summary               AS ACTIONABILITY_SUMMARY,
        tv.CHAIR_OUTPUT_JSON:evidence_used                       AS EVIDENCE_USED,
        tv.CHAIR_OUTPUT_JSON:unresolved_disagreement::BOOLEAN    AS UNRESOLVED_DISAGREEMENT,
        tv.CREATED_AT                                            AS RUN_AT,
        ds.AS_OF_DATE                                            AS AS_OF_DATE,
        ds.DOSSIER_PAYLOAD_JSON:structural_timeline_summary:current_range_position_pct::FLOAT AS RANGE_PCT,
        ds.DOSSIER_PAYLOAD_JSON:structural_timeline_summary:trend_shape_class::STRING         AS TREND_SHAPE,
        ds.DOSSIER_PAYLOAD_JSON:candle_psychology:recent_cluster::STRING                      AS RECENT_CLUSTER,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:continuation_quality::STRING            AS CONTINUATION_QUALITY,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:resistance_overhead_risk::STRING        AS RESISTANCE_OVERHEAD_RISK,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:broken_resistance_support_confidence::FLOAT AS BROKEN_R_CONFIDENCE_AC,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_price::FLOAT  AS BROKEN_R_LEVEL,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_low::FLOAT    AS BROKEN_R_ZONE_LOW,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_high::FLOAT   AS BROKEN_R_ZONE_HIGH,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:confidence::FLOAT   AS BROKEN_R_CONFIDENCE,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:role::STRING        AS BROKEN_R_ROLE,
        ds.DOSSIER_PAYLOAD_JSON:levels:nearest_support:level_price::FLOAT     AS NEAREST_SUPPORT,
        ds.DOSSIER_PAYLOAD_JSON:levels:nearest_resistance:level_price::FLOAT  AS NEAREST_RESISTANCE
    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv
    JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT ds
      ON ds.RUN_ID = tv.RUN_ID AND ds.DOSSIER_ID = tv.DOSSIER_ID
    WHERE tv.MARKET_TYPE = 'STOCK'
      AND tv.CREATED_AT >= DATEADD('day', -14, CURRENT_TIMESTAMP())
      AND tv.SYMBOL = %(symbol)s
    QUALIFY ROW_NUMBER() OVER (ORDER BY tv.CREATED_AT DESC) = 1
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


def _phase4_chair_view(row: Dict[str, Any]) -> Dict[str, Any]:
    """Project a `_PHASE4_CHAIR_SQL` row into the LPA-friendly shape
    expected by `BoardExplanationPanel.Phase4ChairSection`.

    We unpack the chair JSON's nested objects (`prior_thesis_reference`,
    `actionability_summary`, `evidence_used`) so the frontend doesn't
    need to know the source schema. We also pass through dossier-level
    structural / candle / level fields, which are what the panel uses
    for the "Structural timeline", "Candle psychology", and "Zones"
    sections.
    """
    prior_ref = _to_json_obj(row.get("PRIOR_THESIS_REF"))
    actionability = _to_json_obj(row.get("ACTIONABILITY_SUMMARY"))
    evidence_used = _to_json_obj(row.get("EVIDENCE_USED"))
    chair_full = _to_json_obj(row.get("CHAIR_OUTPUT_JSON"))
    if not isinstance(chair_full, dict):
        chair_full = {}
    return {
        "final_action":             row.get("FINAL_ACTION"),
        "final_direction":          row.get("FINAL_DIRECTION"),
        "thesis_health":            row.get("THESIS_HEALTH"),
        "primary_reason_code":      row.get("PRIMARY_REASON_CODE"),
        "secondary_reason_code":    row.get("SECONDARY_REASON_CODE"),
        "final_thesis":             row.get("FINAL_THESIS"),
        "why_not_opposite":         row.get("WHY_NOT_OPPOSITE"),
        "why_not_no_trade":         row.get("WHY_NOT_NO_TRADE"),
        "risk_treatment":           row.get("RISK_TREATMENT"),
        "unresolved_disagreement":  bool(row.get("UNRESOLVED_DISAGREEMENT")) if row.get("UNRESOLVED_DISAGREEMENT") is not None else None,
        "prior_thesis_reference":   prior_ref if isinstance(prior_ref, dict) else None,
        "actionability_summary":    actionability if isinstance(actionability, dict) else None,
        "evidence_used":            evidence_used if isinstance(evidence_used, (dict, list)) else None,
        "structural_timeline_summary": {
            "current_range_position_pct": _safe_float(row.get("RANGE_PCT")),
            "trend_shape_class":          row.get("TREND_SHAPE"),
        },
        "candle_psychology": {
            "recent_cluster": row.get("RECENT_CLUSTER"),
        },
        "actionability_context": {
            "continuation_quality":             row.get("CONTINUATION_QUALITY"),
            "resistance_overhead_risk":         row.get("RESISTANCE_OVERHEAD_RISK"),
            "broken_resistance_support_confidence": _safe_float(row.get("BROKEN_R_CONFIDENCE_AC")),
        },
        "zones": {
            "broken_resistance_as_support": {
                "level_price":  _safe_float(row.get("BROKEN_R_LEVEL")),
                "zone_low":     _safe_float(row.get("BROKEN_R_ZONE_LOW")),
                "zone_high":    _safe_float(row.get("BROKEN_R_ZONE_HIGH")),
                "confidence":   _safe_float(row.get("BROKEN_R_CONFIDENCE")),
                "role":         row.get("BROKEN_R_ROLE"),
            } if row.get("BROKEN_R_LEVEL") is not None else None,
            "nearest_support":    _safe_float(row.get("NEAREST_SUPPORT")),
            "nearest_resistance": _safe_float(row.get("NEAREST_RESISTANCE")),
        },
        "as_of_date": row.get("AS_OF_DATE"),
        "run_at":     row.get("RUN_AT"),
        # Pass through the raw chair JSON so the panel can show
        # additional fields (e.g. risk_treatment notes, why_now_evidence)
        # without us projecting every key here.
        "raw_chair_output": chair_full if chair_full else None,
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
    dossier_id = head.get("BOARD_DOSSIER_ID")
    # Phase 3 rows expose CANDIDATE_ID; Phase 4 rows expose DOSSIER_ID.
    # Either path is "board lineage present" — only when both are NULL
    # (legacy pre-cutover) do we render unavailable.
    if not run_id or (candidate_id is None and dossier_id is None):
        return {
            "available": False,
            "proposal_id": int(proposal_id),
            "note": "Proposal has no board lineage (pre-cutover legacy or non-board path).",
        }

    # Phase 3 specialist / orchestrator / interaction queries are only
    # valid when CANDIDATE_ID is set. Phase 4 leaves them empty and
    # surfaces phase4_chair instead.
    outcomes: List[Dict[str, Any]] = []
    verdict_rows: List[Dict[str, Any]] = []
    interactions: List[Dict[str, Any]] = []
    if candidate_id is not None:
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

    # Phase 4 chair output (run_id + dossier_id keyed) — the chair
    # decision that produced this published proposal. Captures the
    # PROPOSE_LONG / PROPOSE_SHORT decision at publication time even if
    # later board runs subsequently degraded the thesis to
    # WATCH_LONG_FAILURE. Legacy Phase 3 rows skip this block.
    phase4_chair: Optional[Dict[str, Any]] = None
    if dossier_id is not None:
        try:
            ph4_rows = _query(
                _PHASE4_CHAIR_SQL,
                {"run_id": run_id, "dossier_id": int(dossier_id)},
            )
        except Exception as exc:
            logger.warning(
                "board_explanation: phase4 chair query failed for %s: %s",
                proposal_id, exc,
            )
            ph4_rows = []
        if ph4_rows:
            phase4_chair = _phase4_chair_view(ph4_rows[0])

    # Latest lineage-aware Phase 4 verdict for this proposal — the
    # current thesis-health update (e.g. WATCH_LONG_FAILURE) if a
    # follow-up run lineage-references this proposal. Falls back to
    # the latest symbol-level verdict when no lineage match exists.
    # Tagged with `linkage` so the UI can show "(symbol-level)" when
    # the verdict isn't directly tied to this proposal.
    phase4_latest_health: Optional[Dict[str, Any]] = None
    if dossier_id is not None:
        symbol = head.get("SYMBOL")
        try:
            linked_rows = _query(
                _PHASE4_LATEST_LINKED_FOR_PROPOSAL_SQL,
                {"proposal_id": int(proposal_id)},
            )
        except Exception as exc:
            logger.warning(
                "board_explanation: phase4 lineage health query failed for %s: %s",
                proposal_id, exc,
            )
            linked_rows = []
        if linked_rows:
            phase4_latest_health = _phase4_chair_view(linked_rows[0])
            phase4_latest_health["linkage"] = "PRIOR_THESIS_MATCH"
        elif symbol:
            try:
                sym_rows = _query(
                    _PHASE4_LATEST_BY_SYMBOL_FOR_LPA_SQL,
                    {"symbol": str(symbol).upper()},
                )
            except Exception as exc:
                logger.warning(
                    "board_explanation: phase4 symbol-latest query failed for %s: %s",
                    proposal_id, exc,
                )
                sym_rows = []
            if sym_rows:
                phase4_latest_health = _phase4_chair_view(sym_rows[0])
                phase4_latest_health["linkage"] = "SYMBOL_LATEST_ONLY"

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
        "board_candidate_id": int(candidate_id) if candidate_id is not None else None,
        "board_dossier_id": int(dossier_id) if dossier_id is not None else None,
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
        "phase4_chair": phase4_chair,
        "phase4_latest_health": phase4_latest_health,
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

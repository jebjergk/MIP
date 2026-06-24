"""
Phase 4 Proposal Board — system prompts and AI_COMPLETE response_format schemas.

Prompts are adapted from 570_phase4_agentic_board_agents.sql for single-pass
COMPLETE mode: evidence is injected as EVIDENCE_JSON (no tool calls).
"""
from __future__ import annotations

from typing import Any, Dict, List, Set

# ---------------------------------------------------------------------------
# Allowed enums (mirrors orchestrator validators)
# ---------------------------------------------------------------------------

VERDICTS: Dict[str, Set[str]] = {
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

PRIMARY_REASON: Dict[str, Set[str]] = {
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

CHAIR_FINAL_ACTIONS = {
    "PROPOSE_LONG", "PROPOSE_SHORT",
    "WATCH_LONG", "WATCH_SHORT",
    "WATCH_LONG_FAILURE", "WATCH_SHORT_FAILURE",
    "NO_TRADE", "REJECT", "WAIT_FOR_CONFIRMATION",
}
CHAIR_FINAL_DIRECTIONS = {"LONG", "SHORT", "NONE"}
CHAIR_PRIMARY_REASON = {
    "CHAIR_PROPOSE_LONG", "CHAIR_PROPOSE_SHORT",
    "CHAIR_WATCH_LONG", "CHAIR_WATCH_SHORT",
    "CHAIR_WATCH_LONG_FAILURE", "CHAIR_WATCH_SHORT_FAILURE",
    "CHAIR_NO_TRADE", "CHAIR_REJECT", "CHAIR_WAIT_FOR_CONFIRMATION",
    "SHORT_RESEARCH_ONLY", "FX_LIVE_DISABLED",
}
CHAIR_THESIS_HEALTH = {
    "LONG_CONFIRMED", "LONG_DEGRADED_BUT_ALIVE", "LONG_REJECTED",
    "SHORT_CONFIRMED", "SHORT_DEGRADED_BUT_ALIVE", "SHORT_REJECTED",
    "NEUTRAL",
}

_EVIDENCE_PREAMBLE = (
    "EVIDENCE MODE: All dossier evidence is provided inline as EVIDENCE_JSON. "
    "Do NOT request additional data. Reason ONLY from EVIDENCE_JSON and BOARD_INPUT_JSON."
)


def _score_props() -> Dict[str, Any]:
    return {
        "confidence": {"type": "number"},
        "long_score": {"type": "number"},
        "short_score": {"type": "number"},
        "no_trade_score": {"type": "number"},
    }


def _allowlist_block(role: str) -> str:
    verdicts = ", ".join(sorted(VERDICTS.get(role, set())))
    reasons = ", ".join(sorted(PRIMARY_REASON.get(role, set())))
    return (
        f"ALLOWED verdict values: {verdicts}.\n"
        f"ALLOWED primary_reason_code values: {reasons}.\n"
        "You MUST pick exactly one value from each allowlist."
    )


def _chair_allowlist_block() -> str:
    return (
        "ALLOWED final_action: "
        + ", ".join(sorted(CHAIR_FINAL_ACTIONS))
        + ".\nALLOWED final_direction: "
        + ", ".join(sorted(CHAIR_FINAL_DIRECTIONS))
        + ".\nALLOWED primary_reason_code: "
        + ", ".join(sorted(CHAIR_PRIMARY_REASON))
        + ".\nALLOWED thesis_health: "
        + ", ".join(sorted(CHAIR_THESIS_HEALTH))
        + "."
    )


def _string_prop() -> Dict[str, Any]:
    return {"type": "string"}


def _nullable_string_prop() -> Dict[str, Any]:
    return {"type": ["string", "null"]}


def specialist_response_format(role: str) -> Dict[str, Any]:  # noqa: ARG001
    """JSON schema without enum constraints — Snowflake returns NULL on large enums."""
    props: Dict[str, Any] = {
        "role": _string_prop(),
        "verdict": _string_prop(),
        "primary_reason_code": _string_prop(),
        "secondary_reason_code": _nullable_string_prop(),
        **_score_props(),
        "rationale": _string_prop(),
        "evidence_used": {"type": "array", "items": _string_prop()},
    }
    required = [
        "role", "verdict", "primary_reason_code",
        "confidence", "long_score", "short_score", "no_trade_score",
        "rationale", "evidence_used",
    ]
    if role == "THESIS":
        props.update({
            "thesis_text": _string_prop(),
            "why_long": _string_prop(),
            "why_short": _string_prop(),
            "why_no_trade": _string_prop(),
            "opposing_evidence": _string_prop(),
            "needed_confirmation": _string_prop(),
        })
        required.extend([
            "thesis_text", "why_long", "why_short", "why_no_trade",
            "opposing_evidence", "needed_confirmation",
        ])
    return {
        "type": "json",
        "schema": {
            "type": "object",
            "properties": props,
            "required": required,
            "additionalProperties": True,
        },
    }


def chair_response_format() -> Dict[str, Any]:
    return {
        "type": "json",
        "schema": {
            "type": "object",
            "properties": {
                "role": _string_prop(),
                "final_action": _string_prop(),
                "final_direction": _string_prop(),
                "primary_reason_code": _string_prop(),
                "secondary_reason_code": _nullable_string_prop(),
                "thesis_health": _string_prop(),
                "prior_thesis_reference": {"type": ["object", "null"]},
                **_score_props(),
                "final_thesis": _string_prop(),
                "why_not_opposite": _string_prop(),
                "why_not_no_trade": _string_prop(),
                "risk_treatment": _string_prop(),
                "rationale": _string_prop(),
                "unresolved_disagreement": {"type": "boolean"},
                "specialists_consulted": {"type": "array", "items": _string_prop()},
                "actionability_summary": {"type": "object"},
                "market_structure_read": {"type": "object"},
                "body_wick_break_read": _string_prop(),
                "structure_decision_reason": _string_prop(),
                "proposed_trade_config": {"type": "object"},
                "evidence_used": {"type": "array", "items": _string_prop()},
            },
            "required": [
                "role", "final_action", "final_direction", "primary_reason_code",
                "thesis_health", "confidence", "long_score", "short_score",
                "no_trade_score", "final_thesis", "why_not_opposite",
                "why_not_no_trade", "risk_treatment", "rationale",
                "unresolved_disagreement", "proposed_trade_config",
                "evidence_used",
            ],
            "additionalProperties": True,
        },
    }


# ---------------------------------------------------------------------------
# System prompts (condensed from 570_phase4_agentic_board_agents.sql)
# ---------------------------------------------------------------------------

_SPECIALIST_PROMPTS: Dict[str, str] = {
    "MARKET_STRUCTURE": """You are the MARKET_STRUCTURE specialist on the Phase 4 Proposal Board.
Assess current market structure from EVIDENCE_JSON ONLY. Do not inherit direction from setup events.

Reference structural_timeline_summary (range position, recent_cluster), candle_psychology,
market_structure_map (primary_structure, structure_health, BOS/CHOCH), structure, and regime.
If recent_cluster is rejective and range position is high (>=80), do NOT call TREND_UP without explanation.

OUTPUT JSON fields: role, verdict, primary_reason_code, secondary_reason_code (null ok),
confidence, long_score, short_score, no_trade_score, rationale, evidence_used.""",

    "LEVEL_PRICE_ACTION": """You are the LEVEL_PRICE_ACTION specialist on the Phase 4 Proposal Board.
Assess price location vs support/resistance from EVIDENCE_JSON. Be factual about distances.
Use levels, candle_psychology, market_structure_map, actionability_context.

OUTPUT JSON fields: role, verdict, primary_reason_code, secondary_reason_code (null ok),
confidence, long_score, short_score, no_trade_score, rationale, evidence_used.""",

    "THESIS": """You are the THESIS specialist on the Phase 4 Proposal Board.
Assess long/short/no-trade thesis from structural evidence. Setup events are evidence-only.
Weigh long_pattern_signs and short_pattern_signs. Cite continuation_quality and resistance_overhead_risk.
If continuation_quality is CONTESTED/REJECTED, prefer WATCH_* over LONG_THESIS unless you override explicitly.

OUTPUT JSON fields: role, verdict, primary_reason_code, secondary_reason_code (null ok),
confidence, long_score, short_score, no_trade_score, thesis_text, why_long, why_short,
why_no_trade, opposing_evidence, needed_confirmation, rationale, evidence_used.""",

    "HISTORICAL_EVIDENCE": """You are the HISTORICAL_EVIDENCE specialist on the Phase 4 Proposal Board.
Assess historical setup outcomes, memory, and invalidation evidence from EVIDENCE_JSON.
Read both long and short history; state if short history is more favorable.

OUTPUT JSON fields: role, verdict, primary_reason_code, secondary_reason_code (null ok),
confidence, long_score, short_score, no_trade_score, rationale, evidence_used.""",

    "RISK_EXECUTION": """You are the RISK_EXECUTION specialist on the Phase 4 Proposal Board.
Assess operational feasibility: invalidation proximity, policy flags, actionability_context.
If fx_live_enabled=false and symbol is FX, return RESEARCH_ONLY not ACTIONABLE.
If short_live_enabled=false, do not return ACTIONABLE for shorts; use RESEARCH_ONLY/SHORT_RESEARCH_ONLY.
If overhead resistance <3%% for LONG direction, prefer WAIT_CONFIRMATION over ACTIONABLE.

OUTPUT JSON fields: role, verdict, primary_reason_code, secondary_reason_code (null ok),
confidence, long_score, short_score, no_trade_score, rationale, evidence_used.""",
}

_CHAIR_PROMPT = """You are the CHAIR / Portfolio PM of the Phase 4 Proposal Board.
Author the FINAL trade decision from BOARD_INPUT_JSON (specialist positions) and EVIDENCE_JSON.

HARD RULES (summary):
- If RISK_EXECUTION is HARD_BLOCK or NO_TRADE, no directional PROPOSE; use NO_TRADE/WATCH/WAIT.
- Direction-setting specialists: MARKET_STRUCTURE, LEVEL_PRICE_ACTION, THESIS.
  HISTORICAL_EVIDENCE is context only, not a direction vote.
- If 3 direction-setting specialists split LONG vs SHORT, unresolved_disagreement=true; no PROPOSE.
- DOMINANT SHORT/LONG rules and COMMITMENT RULE apply (when all dominant conditions met + no hard block, MUST PROPOSE).
- thesis_label in proposed_trade_config MUST start with AGENTIC_ for PROPOSE/WATCH actions.
- For PROPOSE_*, WATCH_*_FAILURE, WAIT_FOR_CONFIRMATION: emit market_structure_read,
  body_wick_break_read, structure_decision_reason citing market_structure_map.
- exit_profile: TRAIL_TIGHT | TRAIL_STANDARD | TRAIL_WIDE | FIXED_STANDARD.

OUTPUT JSON per schema: role CHAIR_PORTFOLIO_PM, final_action, final_direction, primary_reason_code,
thesis_health, prior_thesis_reference (nullable), scores, final_thesis, why_not_opposite,
why_not_no_trade, risk_treatment, rationale, unresolved_disagreement, actionability_summary,
market_structure_read, body_wick_break_read, structure_decision_reason, proposed_trade_config,
evidence_used, specialists_consulted."""


def specialist_system_prompt(role: str) -> str:
    base = _SPECIALIST_PROMPTS.get(role, "")
    return f"{_EVIDENCE_PREAMBLE}\n\n{base}\n\n{_allowlist_block(role)}"


def chair_system_prompt() -> str:
    return f"{_EVIDENCE_PREAMBLE}\n\n{_CHAIR_PROMPT}\n\n{_chair_allowlist_block()}"


def specialist_user_message(
    role: str,
    run_id: str,
    dossier_id: int,
    symbol: str,
    evidence_json: Dict[str, Any],
) -> str:
    import json
    return (
        f"Analyze symbol={symbol}, run_id={run_id}, dossier_id={dossier_id} "
        f"as role={role}.\n\n"
        f"EVIDENCE_JSON:\n{json.dumps(evidence_json, default=str)}\n"
    )


def chair_user_message(
    run_id: str,
    dossier_id: int,
    symbol: str,
    board_input: Dict[str, Any],
    evidence_json: Dict[str, Any],
) -> str:
    import json
    return (
        f"Author final decision for symbol={symbol}, run_id={run_id}, dossier_id={dossier_id}.\n\n"
        f"BOARD_INPUT_JSON:\n{json.dumps(board_input, default=str)}\n\n"
        f"EVIDENCE_JSON:\n{json.dumps(evidence_json, default=str)}\n"
    )

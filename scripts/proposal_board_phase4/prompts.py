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

_EVIDENCE_USED_RULE = (
    "CRITICAL OUTPUT RULE — evidence_used: MUST be a short JSON array of slice NAME "
    "strings only (e.g. [\"price\", \"levels\", \"history\"]). "
    "NEVER echo, copy, or embed EVIDENCE_JSON data inside evidence_used. "
    "Keep rationale under 400 characters. Omit null optional fields."
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


def _proposed_trade_config_schema(*, strict: bool) -> Dict[str, Any]:
    props = {
        "thesis_label": _string_prop(),
        "entry_zone_low": {"type": "number"},
        "entry_zone_high": {"type": "number"},
        "invalidation_level": {"type": "number"},
        "invalidation_rule": _string_prop(),
        "target_policy": {"type": "object"},
        "exit_profile": _string_prop(),
        "trailing_policy": {"type": "object"},
        "size_treatment": _string_prop(),
        "risk_class": _string_prop(),
        "time_horizon": _string_prop(),
        "primary_evidence_setup_event_id": {"type": ["integer", "null"]},
    }
    schema: Dict[str, Any] = {
        "type": "object",
        "properties": props,
        "additionalProperties": True,
    }
    if strict:
        schema["required"] = [
            "thesis_label",
            "entry_zone_low",
            "entry_zone_high",
            "invalidation_level",
            "invalidation_rule",
            "exit_profile",
            "size_treatment",
            "risk_class",
            "time_horizon",
        ]
    return schema


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
                "proposed_trade_config": _proposed_trade_config_schema(strict=False),
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
confidence, long_score, short_score, no_trade_score, rationale, evidence_used.

""" + _EVIDENCE_USED_RULE,

    "LEVEL_PRICE_ACTION": """You are the LEVEL_PRICE_ACTION specialist on the Phase 4 Proposal Board.
Assess price location vs support/resistance from EVIDENCE_JSON. Be factual about distances.
Use levels, candle_psychology, market_structure_map, actionability_context.

OUTPUT JSON fields: role, verdict, primary_reason_code, secondary_reason_code (null ok),
confidence, long_score, short_score, no_trade_score, rationale, evidence_used.

""" + _EVIDENCE_USED_RULE,

    "THESIS": """You are the THESIS specialist on the Phase 4 Proposal Board.
Assess long/short/no-trade thesis from structural evidence. Setup events are evidence-only.
Weigh long_pattern_signs and short_pattern_signs. Cite continuation_quality and resistance_overhead_risk.
If continuation_quality is CONTESTED/REJECTED, prefer WATCH_* over LONG_THESIS unless you override explicitly.

OUTPUT JSON fields: role, verdict, primary_reason_code, secondary_reason_code (null ok),
confidence, long_score, short_score, no_trade_score, thesis_text, why_long, why_short,
why_no_trade, opposing_evidence, needed_confirmation, rationale, evidence_used.

""" + _EVIDENCE_USED_RULE,

    "HISTORICAL_EVIDENCE": """You are the HISTORICAL_EVIDENCE specialist on the Phase 4 Proposal Board.
Assess historical setup outcomes, memory, and invalidation evidence from EVIDENCE_JSON.
Read both long and short history; state if short history is more favorable.
Summarize history in rationale — do NOT copy history arrays into the response.

OUTPUT JSON fields: role, verdict, primary_reason_code, secondary_reason_code (null ok),
confidence, long_score, short_score, no_trade_score, rationale, evidence_used.

""" + _EVIDENCE_USED_RULE,

    "RISK_EXECUTION": """You are the RISK_EXECUTION specialist on the Phase 4 Proposal Board.
Assess operational feasibility: invalidation proximity, policy flags, actionability_context.
If fx_live_enabled=false and symbol is FX, return RESEARCH_ONLY not ACTIONABLE.
If short_live_enabled=false, do not return ACTIONABLE for shorts; use RESEARCH_ONLY/SHORT_RESEARCH_ONLY.
If overhead resistance <3%% for LONG direction, prefer WAIT_CONFIRMATION over ACTIONABLE.

OUTPUT JSON fields: role, verdict, primary_reason_code, secondary_reason_code (null ok),
confidence, long_score, short_score, no_trade_score, rationale, evidence_used.

""" + _EVIDENCE_USED_RULE,
}

_CHAIR_PROMPT = """You are the CHAIR / Portfolio PM of the Phase 4 Proposal Board.
Author the FINAL trade decision from BOARD_INPUT_JSON (specialist positions) and EVIDENCE_JSON.

HARD RULES (summary):
- ONLY RISK_EXECUTION HARD_BLOCK or NO_TRADE are hard blocks against PROPOSE.
  RISK RESEARCH_ONLY on LONG is NOT a hard block — it means reduced-size / research lane, not veto.
  THESIS WATCH_LONG or WATCH_SHORT is NOT a veto — it means monitor; you may still PROPOSE when structure + level align.
- Direction-setting specialists: MARKET_STRUCTURE, LEVEL_PRICE_ACTION, THESIS.
  HISTORICAL_EVIDENCE is context only, not a direction vote.
- If 3 direction-setting specialists split LONG vs SHORT, unresolved_disagreement=true; no PROPOSE.
- DOMINANT LONG / DOMINANT SHORT / COMMITMENT RULE: when dominant conditions below are met and no hard RISK block,
  you MUST emit PROPOSE_LONG or PROPOSE_SHORT with complete proposed_trade_config (do not downgrade to WATCH).
  DOMINANT LONG: MARKET_STRUCTURE favors LONG (TREND_UP, SUPPORT_BOUNCE, BREAKOUT_ATTEMPT, etc.) AND
  LEVEL_PRICE_ACTION is LONG_LOCATION or BOTH_SIDES (not SHORT_LOCATION/NO_EDGE) AND
  THESIS is WATCH_LONG or LONG_THESIS (not NO_TRADE/CONFLICTED) AND RISK is not HARD_BLOCK/NO_TRADE AND
  EVIDENCE_JSON.primary_evidence_setup_event_id is present AND you can anchor entry zone within 3%% of current_price AND
  the primary evidence setup has level_entry_coherent=true (i.e. the cited LEVEL_PRICE is near the entry midpoint).
  DOMINANT SHORT: mirror for SHORT when policy.short_live_enabled allows live shorts.
- STRUCTURAL COHERENCE GATE: Do NOT PROPOSE if the primary evidence setup's level_entry_coherent field is false. That
  means the cited structural level (support for LONG, resistance for SHORT) sits far from the entry zone — the
  setup's narrative and geometry disagree, and the dossier's board_warnings will include NO_COHERENT_PRIMARY_EVIDENCE.
  In that case emit WATCH_LONG / WATCH_SHORT / WAIT_FOR_CONFIRMATION with a rationale citing the anchor mismatch.
- OPPOSING SETUP RULE: If EVIDENCE_JSON.setup_events_evidence_only contains an ELIGIBLE or DETECTED setup with
  DIRECTION opposite to your intended proposal within the last 3 bars AND structure_confidence >= 0.65, your
  why_not_opposite MUST reference that opposing setup by setup_event_id and setup_family and give a specific,
  evidence-based reason for overriding it. Do NOT dismiss opposing evidence by citing trust_label (e.g.
  "opposing family is RESEARCH-only") — trust gates execution, not evidence weight. Failure to substantively
  address the opposing setup will cause the proposal to publish as UNRESOLVED_OPPOSING_SETUP (research-only).
- thesis_label in proposed_trade_config MUST start with AGENTIC_ for PROPOSE/WATCH actions.
- For PROPOSE_*, WATCH_*_FAILURE, WAIT_FOR_CONFIRMATION: emit market_structure_read,
  body_wick_break_read, structure_decision_reason citing market_structure_map.
- exit_profile: TRAIL_TIGHT | TRAIL_STANDARD | TRAIL_WIDE | FIXED_STANDARD.

PROPOSE_LONG / PROPOSE_SHORT — proposed_trade_config is MANDATORY and COMPLETE:
- Anchor entry_zone_low and entry_zone_high to TODAY's EVIDENCE_JSON.price.current_price and
  EVIDENCE_JSON.levels (support/resistance). The zone midpoint must be within 3%% of current_price.
- LONG: entry_zone_low < entry_zone_high; invalidation_level MUST be below entry_zone_low.
- SHORT: entry_zone_low < entry_zone_high; invalidation_level MUST be above entry_zone_high.
- NEVER reuse setup_events_evidence_only zones or prior-day levels without re-anchoring to current_price.
- Required fields: thesis_label, entry_zone_low, entry_zone_high, invalidation_level,
  invalidation_rule, exit_profile, size_treatment, risk_class, time_horizon,
  primary_evidence_setup_event_id (from dossier when present).
- Do NOT PROPOSE if you cannot author a coherent zone for today's close.

""" + _EVIDENCE_USED_RULE + """

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

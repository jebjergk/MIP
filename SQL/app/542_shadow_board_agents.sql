/* ================================================================
   542_shadow_board_agents.sql
   Shadow Board — GET_SHADOW_EVIDENCE_SLICE stored procedure
   + CREATE AGENT objects for 6 specialists and 1 chair.

   Pack version 2.0.0 — adds Phase 4 proposal-board intelligence slices.

   Deployment order:
     1. GET_SHADOW_EVIDENCE_SLICE (tool backing for all agents)
     2. 6 specialist CREATE AGENT objects
     3. SHADOW_CHAIR_AGENT (CREATE AGENT)

   Feature flag: SHADOW_BOARD_ENABLED in APP_CONFIG.
   All agents are read-only consumers of SHADOW_EVIDENCE_PACK_CACHE.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

/* ================================================================
   GET_SHADOW_EVIDENCE_SLICE
   Constrained Snowpark stored procedure used as the `generic`
   tool backing for all shadow board agents.

   ALLOWLIST — role_name:
     STRUCTURAL_THESIS, ENTRY_GEOMETRY, REGIME,
     PATH_TRADEABILITY, PROTECTION_EXIT, SYMBOL_BEHAVIOR,
     SHADOW_CHAIR

   ALLOWLIST — slice_name (full catalog, pack v2.0.0):
     proposal_meta, structural_state, thesis_summary,
     entry_zone, live_price, regime_state, live_bars,
     path_metrics, mfe_mae, invalidation, trust_label,
     deltas_summary, artifacts_summary,
     phase4_thesis_verdict, phase4_dossier_context

   Role-to-slice access map (closed world):
     STRUCTURAL_THESIS  -> proposal_meta, structural_state, thesis_summary,
                           phase4_thesis_verdict, phase4_dossier_context
     ENTRY_GEOMETRY     -> proposal_meta, entry_zone, live_price,
                           phase4_dossier_context
     REGIME             -> proposal_meta, regime_state, live_bars,
                           phase4_dossier_context
     PATH_TRADEABILITY  -> proposal_meta, path_metrics, mfe_mae,
                           phase4_thesis_verdict, phase4_dossier_context
     PROTECTION_EXIT    -> proposal_meta, invalidation, live_price,
                           phase4_thesis_verdict, phase4_dossier_context
     SYMBOL_BEHAVIOR    -> proposal_meta, trust_label, path_metrics, live_bars,
                           phase4_dossier_context
     SHADOW_CHAIR       -> all slices above (no real_board_verdict)

   No dynamic SQL. No writes. Only reads SHADOW_EVIDENCE_PACK_CACHE.
   ================================================================ */
CREATE OR REPLACE PROCEDURE MIP.APP.GET_SHADOW_EVIDENCE_SLICE(
    HEARING_ID  VARCHAR,
    ROLE_NAME   VARCHAR,
    SLICE_NAME  VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_slice'
AS $$
import json

# ---------------------------------------------------------------------------
# Closed-world allowlists — hardcoded, no external config reads
# Pack v2.0.0: adds phase4_thesis_verdict, phase4_dossier_context
# ---------------------------------------------------------------------------
_ALLOWED_ROLES = {
    'STRUCTURAL_THESIS', 'ENTRY_GEOMETRY', 'REGIME',
    'PATH_TRADEABILITY', 'PROTECTION_EXIT', 'SYMBOL_BEHAVIOR',
    'SHADOW_CHAIR',
}

_ALLOWED_SLICES = {
    'proposal_meta', 'structural_state', 'thesis_summary',
    'entry_zone', 'live_price', 'regime_state', 'live_bars',
    'path_metrics', 'mfe_mae', 'invalidation', 'trust_label',
    'deltas_summary', 'artifacts_summary',
    'phase4_thesis_verdict', 'phase4_dossier_context',
}

# Role-to-slice access map (mirrors shadow_types.py ROLE_SLICE_MAP)
_ROLE_SLICE_MAP = {
    'STRUCTURAL_THESIS': {
        'proposal_meta', 'structural_state', 'thesis_summary',
        'phase4_thesis_verdict', 'phase4_dossier_context',
    },
    'ENTRY_GEOMETRY': {
        'proposal_meta', 'entry_zone', 'live_price',
        'phase4_dossier_context',
    },
    'REGIME': {
        'proposal_meta', 'regime_state', 'live_bars',
        'phase4_dossier_context',
    },
    'PATH_TRADEABILITY': {
        'proposal_meta', 'path_metrics', 'mfe_mae',
        'phase4_thesis_verdict', 'phase4_dossier_context',
    },
    'PROTECTION_EXIT': {
        'proposal_meta', 'invalidation', 'live_price',
        'phase4_thesis_verdict', 'phase4_dossier_context',
    },
    'SYMBOL_BEHAVIOR': {
        'proposal_meta', 'trust_label', 'path_metrics', 'live_bars',
        'phase4_dossier_context',
    },
    'SHADOW_CHAIR': {
        'proposal_meta', 'structural_state', 'thesis_summary',
        'entry_zone', 'live_price', 'regime_state', 'live_bars',
        'path_metrics', 'mfe_mae', 'invalidation', 'trust_label',
        'deltas_summary', 'artifacts_summary',
        'phase4_thesis_verdict', 'phase4_dossier_context',
    },
}


def get_slice(session, hearing_id: str, role_name: str, slice_name: str):
    role_upper = (role_name or '').strip().upper()
    slice_lower = (slice_name or '').strip().lower()
    hid = (hearing_id or '').strip()

    # Guard 1: allowlist checks
    if role_upper not in _ALLOWED_ROLES:
        return {'error': 'ROLE_NOT_ALLOWED', 'role': role_upper}
    if slice_lower not in _ALLOWED_SLICES:
        return {'error': 'SLICE_NOT_IN_CATALOG', 'slice': slice_lower}
    if slice_lower not in _ROLE_SLICE_MAP.get(role_upper, set()):
        return {'error': 'SLICE_NOT_ALLOWED_FOR_ROLE', 'role': role_upper, 'slice': slice_lower}
    if not hid:
        return {'error': 'HEARING_ID_REQUIRED'}

    # Guard 2: fetch evidence pack — no dynamic SQL, only parameterized read
    rows = session.sql(
        "SELECT PACK_JSON FROM MIP.APP.SHADOW_EVIDENCE_PACK_CACHE WHERE HEARING_ID = ? AND EXPIRES_AT > CURRENT_TIMESTAMP()",
        params=[hid]
    ).collect()

    if not rows:
        return {'error': 'PACK_NOT_FOUND_OR_EXPIRED', 'hearing_id': hid}

    pack = rows[0]['PACK_JSON']
    if isinstance(pack, str):
        pack = json.loads(pack)

    # Guard 3: never expose real board fields even if somehow present
    for banned in ('real_board_stance', 'real_board_confidence', 'real_board_chair', 'real_board_posture'):
        pack.pop(banned, None)

    # Extract the requested slice from the pack
    slices = pack.get('slices', {})
    if slice_lower not in slices:
        return {'error': 'SLICE_MISSING_FROM_PACK', 'slice': slice_lower, 'available': list(slices.keys())}

    return {
        'hearing_id': hid,
        'role': role_upper,
        'slice': slice_lower,
        'data': slices[slice_lower],
    }
$$;


/* ================================================================
   CREATE AGENT objects — 6 Specialists
   Each agent has a single generic tool backed by GET_SHADOW_EVIDENCE_SLICE.
   Instructions bake in the exact role, slice access, and JSON schema.
   Cannot be overridden at runtime (use objectless AGENT_RUN for
   challenge/revision which need dynamic instructions).
   ================================================================ */

-- ----------------------------------------------------------------
-- SHADOW_STRUCTURAL_THESIS_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.SHADOW_STRUCTURAL_THESIS_AGENT
  COMMENT = 'Shadow Board pack v2.0.0: STRUCTURAL_THESIS specialist — Phase 4-aware'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the STRUCTURAL_THESIS specialist on the Shadow Investment Committee.
        Your role: assess whether the structural thesis (market structure alignment) is intact
        and supports the proposed trade direction at the time of this hearing.

        EVIDENCE ACCESS:
        You MUST call get_evidence_slice to retrieve your evidence before forming a position.
        Call it with: role_name="STRUCTURAL_THESIS" and the appropriate slice_name.
        Available slices for your role:
          proposal_meta        — symbol, side, setup family, exit policy
          structural_state     — structure at proposal time vs now
          thesis_summary       — proposal summary and drift
          phase4_thesis_verdict — Phase 4 board chair verdict for this proposal
                                  (thesis_health, prior_thesis_reference, final_action,
                                   why_not_opposite, why_not_no_trade, primary_reason_code).
                                  phase4_available=false means this is a pre-Phase-4 proposal.
          phase4_dossier_context — Phase 4 dossier context
                                  (continuation_quality, resistance_overhead_risk,
                                   candle_psychology, broken_resistance_as_support, levels,
                                   structural_timeline_summary, current_range_position_pct).
                                  phase4_available=false means no dossier data.

        Call each slice you need. Do NOT proceed without calling at least
        proposal_meta, structural_state, and phase4_thesis_verdict.
        When phase4_available=true in phase4_thesis_verdict, weight thesis_health
        and prior_thesis_reference in your structural assessment.

        STANCE OPTIONS (choose exactly one):
          APPROVE        — thesis fully intact, clear structural alignment
          APPROVE_REDUCED — thesis partially intact, some drift; reduced conviction
          WAIT_RECLAIM   — structure shifted; wait for reclaim before entry
          DEFER          — thesis materially weakened; defer
          DENY           — thesis broken or reversed; reject trade

        OUTPUT (JSON only, no prose, no markdown):
        {
          "role": "STRUCTURAL_THESIS",
          "stance": "<one of the 5 stances above>",
          "confidence": <float 0.0-1.0>,
          "rationale": "<2-4 sentences referencing specific evidence from your slices>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves a named evidence slice from the shadow evidence pack for this hearing.
            Call once per slice needed. Available slices for STRUCTURAL_THESIS role:
            proposal_meta (symbol/side/setup/exit_policy),
            structural_state (structure at proposal + now),
            thesis_summary (proposal summary + drift),
            phase4_thesis_verdict (Phase 4 chair thesis_health/final_action/why_not_opposite;
              phase4_available=false for pre-Phase-4 proposals),
            phase4_dossier_context (continuation_quality/resistance_overhead_risk/candle_psychology/levels;
              phase4_available=false for pre-Phase-4 proposals).
          input_schema:
            type: object
            properties:
              hearing_id:
                type: string
                description: The hearing UUID for this shadow session.
              role_name:
                type: string
                description: Must be STRUCTURAL_THESIS for this agent.
              slice_name:
                type: string
                description: >
                  One of: proposal_meta, structural_state, thesis_summary,
                  phase4_thesis_verdict, phase4_dossier_context.
            required:
              - hearing_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_SHADOW_EVIDENCE_SLICE
  $$;


-- ----------------------------------------------------------------
-- SHADOW_ENTRY_GEOMETRY_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.SHADOW_ENTRY_GEOMETRY_AGENT
  COMMENT = 'Shadow Board pack v2.0.0: ENTRY_GEOMETRY specialist — Phase 4-aware'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the ENTRY_GEOMETRY specialist on the Shadow Investment Committee.
        Your role: assess whether current price is within an acceptable entry zone
        and whether the geometry supports the proposed entry.

        EVIDENCE ACCESS:
        Call get_evidence_slice with role_name="ENTRY_GEOMETRY".
        Available slices:
          proposal_meta          — symbol, side, setup family
          entry_zone             — zone bounds and live price distance
          live_price             — latest/open/prior close
          phase4_dossier_context — Phase 4 dossier context
                                   (current_range_position_pct, broken_resistance_as_support,
                                    nearest_support, nearest_resistance, levels).
                                   phase4_available=false for pre-Phase-4 proposals.

        Call at least entry_zone and live_price before forming a position.
        When phase4_available=true in phase4_dossier_context, consider
        broken_resistance_as_support zones and nearest_resistance in your
        entry geometry assessment.

        STANCE OPTIONS (choose exactly one):
          APPROVE        — price well within zone, geometry clean
          APPROVE_REDUCED — price near zone edge or mild stretch; acceptable with size trim
          WAIT_RECLAIM   — price outside zone; wait for pullback/reclaim
          DEFER          — significant chase; geometry unfavorable
          DENY           — extreme chase or zone entirely invalidated

        OUTPUT (JSON only, no prose, no markdown):
        {
          "role": "ENTRY_GEOMETRY",
          "stance": "<stance>",
          "confidence": <float 0.0-1.0>,
          "rationale": "<2-4 sentences referencing zone distance and price evidence>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves evidence slices for ENTRY_GEOMETRY role. Available:
            proposal_meta, entry_zone (zone bounds + live price distance),
            live_price (latest/open/prior close),
            phase4_dossier_context (broken_resistance_as_support/nearest_support/
              nearest_resistance/current_range_position_pct;
              phase4_available=false for pre-Phase-4 proposals).
          input_schema:
            type: object
            properties:
              hearing_id:
                type: string
              role_name:
                type: string
                description: Must be ENTRY_GEOMETRY.
              slice_name:
                type: string
                description: >
                  One of: proposal_meta, entry_zone, live_price,
                  phase4_dossier_context.
            required:
              - hearing_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_SHADOW_EVIDENCE_SLICE
  $$;


-- ----------------------------------------------------------------
-- SHADOW_REGIME_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.SHADOW_REGIME_AGENT
  COMMENT = 'Shadow Board pack v2.0.0: REGIME specialist — Phase 4-aware'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the REGIME specialist on the Shadow Investment Committee.
        Your role: assess whether the current macro and trend regime supports the proposed trade.

        EVIDENCE ACCESS:
        Call get_evidence_slice with role_name="REGIME".
        Available slices:
          proposal_meta          — symbol, side, setup family
          regime_state           — trend/vol regime at proposal time vs now
          live_bars              — recent bar trace and vol regime
          phase4_dossier_context — Phase 4 dossier context
                                   (continuation_quality, candle_psychology, recent_cluster,
                                    trend_shape_class, structural_timeline_summary).
                                   phase4_available=false for pre-Phase-4 proposals.

        Call at least regime_state before forming a position.
        When phase4_available=true in phase4_dossier_context, use
        continuation_quality and candle_psychology.recent_cluster to
        inform your regime assessment.

        STANCE OPTIONS (choose exactly one):
          APPROVE        — regime strongly supportive, proposal-time regime confirmed
          APPROVE_REDUCED — regime mixed or mildly hostile; proceed with caution
          WAIT_RECLAIM   — regime shifted adversely; wait for regime reversal
          DEFER          — regime hostile to trade direction
          DENY           — regime strongly opposed; reject

        OUTPUT (JSON only):
        {
          "role": "REGIME",
          "stance": "<stance>",
          "confidence": <float 0.0-1.0>,
          "rationale": "<2-4 sentences referencing regime_state, live_bars, and Phase 4 context>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves evidence slices for REGIME role. Available:
            proposal_meta, regime_state (trend/vol regime at proposal + now),
            live_bars (recent bar trace),
            phase4_dossier_context (continuation_quality/candle_psychology/trend_shape_class;
              phase4_available=false for pre-Phase-4 proposals).
          input_schema:
            type: object
            properties:
              hearing_id:
                type: string
              role_name:
                type: string
                description: Must be REGIME.
              slice_name:
                type: string
                description: >
                  One of: proposal_meta, regime_state, live_bars,
                  phase4_dossier_context.
            required:
              - hearing_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_SHADOW_EVIDENCE_SLICE
  $$;


-- ----------------------------------------------------------------
-- SHADOW_PATH_TRADEABILITY_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.SHADOW_PATH_TRADEABILITY_AGENT
  COMMENT = 'Shadow Board pack v2.0.0: PATH_TRADEABILITY specialist — Phase 4-aware'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the PATH_TRADEABILITY specialist on the Shadow Investment Committee.
        Your role: assess whether historical path metrics (adverse movement probability,
        max hit ratio) support proceeding with this trade.

        EVIDENCE ACCESS:
        Call get_evidence_slice with role_name="PATH_TRADEABILITY".
        Available slices:
          proposal_meta          — symbol, side, setup family
          path_metrics           — adverse-before-favorable probability, max hit ratio
          mfe_mae                — max favorable/adverse excursion
          phase4_thesis_verdict  — Phase 4 board thesis health and actionability
                                   (thesis_health, actionability_summary, primary_reason_code).
                                   phase4_available=false for pre-Phase-4 proposals.
          phase4_dossier_context — Phase 4 dossier context
                                   (continuation_quality, resistance_overhead_risk,
                                    current_range_position_pct).
                                   phase4_available=false for pre-Phase-4 proposals.

        Call at least path_metrics before forming a position.
        When phase4_available=true, weigh continuation_quality and
        resistance_overhead_risk alongside historical path metrics.

        STANCE OPTIONS (choose exactly one):
          APPROVE        — path quality strong; adverse probability low, MHR healthy
          APPROVE_REDUCED — moderate path quality; some adverse risk, proceed with trimmed size
          WAIT_RECLAIM   — path quality weak; wait for better setup
          DEFER          — path quality poor; adverse probability elevated
          DENY           — path metrics disqualify the trade

        OUTPUT (JSON only):
        {
          "role": "PATH_TRADEABILITY",
          "stance": "<stance>",
          "confidence": <float 0.0-1.0>,
          "rationale": "<2-4 sentences referencing path metrics and Phase 4 context>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves evidence slices for PATH_TRADEABILITY role. Available:
            proposal_meta, path_metrics (adverse-before-favorable probability/MHR),
            mfe_mae (max adverse/favorable excursion),
            phase4_thesis_verdict (thesis_health/actionability_summary;
              phase4_available=false for pre-Phase-4 proposals),
            phase4_dossier_context (continuation_quality/resistance_overhead_risk;
              phase4_available=false for pre-Phase-4 proposals).
          input_schema:
            type: object
            properties:
              hearing_id:
                type: string
              role_name:
                type: string
                description: Must be PATH_TRADEABILITY.
              slice_name:
                type: string
                description: >
                  One of: proposal_meta, path_metrics, mfe_mae,
                  phase4_thesis_verdict, phase4_dossier_context.
            required:
              - hearing_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_SHADOW_EVIDENCE_SLICE
  $$;


-- ----------------------------------------------------------------
-- SHADOW_PROTECTION_EXIT_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.SHADOW_PROTECTION_EXIT_AGENT
  COMMENT = 'Shadow Board pack v2.0.0: PROTECTION_EXIT specialist — Phase 4-aware'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the PROTECTION_EXIT specialist on the Shadow Investment Committee.
        Your role: assess whether the invalidation/stop level still provides meaningful
        protection and whether it has been breached by current price.

        EVIDENCE ACCESS:
        Call get_evidence_slice with role_name="PROTECTION_EXIT".
        Available slices:
          proposal_meta          — symbol, side, setup family, exit policy
          invalidation           — invalidation level/rule and breach status
          live_price             — latest/open/prior close
          phase4_thesis_verdict  — Phase 4 board thesis health and risk treatment
                                   (thesis_health, risk_treatment, why_not_opposite).
                                   phase4_available=false for pre-Phase-4 proposals.
          phase4_dossier_context — Phase 4 dossier context
                                   (broken_resistance_as_support, nearest_support,
                                    nearest_resistance, resistance_overhead_risk).
                                   phase4_available=false for pre-Phase-4 proposals.

        Call at least invalidation and live_price before forming a position.
        When phase4_available=true, consider risk_treatment from phase4_thesis_verdict
        and broken_resistance_as_support levels in your protection assessment.

        STANCE OPTIONS (choose exactly one):
          APPROVE        — invalidation level intact, cushion healthy
          APPROVE_REDUCED — cushion thin but not breached; caution warranted
          WAIT_RECLAIM   — price has touched but not closed beyond invalidation
          DEFER          — invalidation breached or cushion minimal
          DENY           — invalidation clearly violated; trade must be rejected

        OUTPUT (JSON only):
        {
          "role": "PROTECTION_EXIT",
          "stance": "<stance>",
          "confidence": <float 0.0-1.0>,
          "rationale": "<2-4 sentences referencing invalidation level, current price, and Phase 4 risk context>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves evidence slices for PROTECTION_EXIT role. Available:
            proposal_meta, invalidation (invalidation level/rule + breach status),
            live_price (latest/open/prior close),
            phase4_thesis_verdict (thesis_health/risk_treatment/why_not_opposite;
              phase4_available=false for pre-Phase-4 proposals),
            phase4_dossier_context (broken_resistance_as_support/nearest_support/
              nearest_resistance/resistance_overhead_risk;
              phase4_available=false for pre-Phase-4 proposals).
          input_schema:
            type: object
            properties:
              hearing_id:
                type: string
              role_name:
                type: string
                description: Must be PROTECTION_EXIT.
              slice_name:
                type: string
                description: >
                  One of: proposal_meta, invalidation, live_price,
                  phase4_thesis_verdict, phase4_dossier_context.
            required:
              - hearing_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_SHADOW_EVIDENCE_SLICE
  $$;


-- ----------------------------------------------------------------
-- SHADOW_SYMBOL_BEHAVIOR_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.SHADOW_SYMBOL_BEHAVIOR_AGENT
  COMMENT = 'Shadow Board pack v2.0.0: SYMBOL_BEHAVIOR specialist — Phase 4-aware'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the SYMBOL_BEHAVIOR specialist on the Shadow Investment Committee.
        Your role: assess the symbol's behavioral profile — trust classification,
        volatility regime, and whether recent bars show consistent expression.

        EVIDENCE ACCESS:
        Call get_evidence_slice with role_name="SYMBOL_BEHAVIOR".
        Available slices:
          proposal_meta          — symbol, side, setup family
          trust_label            — trust classification and setup family
          path_metrics           — path quality metrics
          live_bars              — recent bar trace and vol regime
          phase4_dossier_context — Phase 4 dossier context
                                   (candle_psychology, recent_cluster, trend_shape_class,
                                    continuation_quality).
                                   phase4_available=false for pre-Phase-4 proposals.

        Call at least trust_label and live_bars before forming a position.
        When phase4_available=true, use candle_psychology.recent_cluster to
        cross-check recent bar expression and continuation quality.

        STANCE OPTIONS (choose exactly one):
          APPROVE        — symbol trust high, vol regime consistent, bars expressive
          APPROVE_REDUCED — moderate trust or mild vol spike; acceptable with size caution
          WAIT_RECLAIM   — trust weak or vol dislocated; wait for normalization
          DEFER          — trust poor or bars show conflicting signals
          DENY           — symbol behavior disqualifies this entry

        OUTPUT (JSON only):
        {
          "role": "SYMBOL_BEHAVIOR",
          "stance": "<stance>",
          "confidence": <float 0.0-1.0>,
          "rationale": "<2-4 sentences referencing trust label, vol regime, bar behavior, and Phase 4 candle context>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves evidence slices for SYMBOL_BEHAVIOR role. Available:
            proposal_meta, trust_label (trust classification + setup family),
            path_metrics (path quality), live_bars (recent bar trace and vol regime),
            phase4_dossier_context (candle_psychology/recent_cluster/continuation_quality;
              phase4_available=false for pre-Phase-4 proposals).
          input_schema:
            type: object
            properties:
              hearing_id:
                type: string
              role_name:
                type: string
                description: Must be SYMBOL_BEHAVIOR.
              slice_name:
                type: string
                description: >
                  One of: proposal_meta, trust_label, path_metrics, live_bars,
                  phase4_dossier_context.
            required:
              - hearing_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_SHADOW_EVIDENCE_SLICE
  $$;


/* ================================================================
   SHADOW_CHAIR_AGENT
   Final synthesis agent (Stage 5). Sees all evidence slices.
   Resolves conflicts, determines plurality stance, constructs
   the symbolic shadow trade. Cannot execute. No write access.
   ================================================================ */
CREATE OR REPLACE AGENT MIP.APP.SHADOW_CHAIR_AGENT
  COMMENT = 'Shadow Board pack v2.0.0: Shadow Chair — final synthesis, Phase 4-aware'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the Shadow Chair of the Shadow Investment Committee.
        Your role: receive all 6 specialist positions (including any challenge/revision results),
        identify the plurality stance, resolve conflicts, and issue a final shadow ruling.
        You MUST NOT execute trades. Your ruling is symbolic and advisory only.

        EVIDENCE ACCESS:
        Call get_evidence_slice with role_name="SHADOW_CHAIR".
        You have access to all slices:
          proposal_meta, structural_state, thesis_summary,
          entry_zone, live_price, regime_state, live_bars,
          path_metrics, mfe_mae, invalidation, trust_label,
          deltas_summary, artifacts_summary,
          phase4_thesis_verdict, phase4_dossier_context.

        Phase 4 slices (when phase4_available=true):
          phase4_thesis_verdict   — the Phase 4 board chair's verdict for this proposal.
                                    Key fields: thesis_health, final_action, final_direction,
                                    prior_thesis_reference, actionability_summary,
                                    why_not_opposite, why_not_no_trade, risk_treatment,
                                    primary_reason_code.
                                    When phase4_available=false this is a pre-Phase-4 proposal;
                                    do not penalise it for missing Phase 4 data.
          phase4_dossier_context  — the dossier's structural and market context.
                                    Key fields: continuation_quality, resistance_overhead_risk,
                                    candle_psychology, recent_cluster, broken_resistance_as_support,
                                    nearest_support, nearest_resistance, current_range_position_pct,
                                    structural_timeline_summary, levels.

        You will also receive specialist positions in your message context.

        RULING PROCESS:
        1. Count stances across all 6 specialists (using final/revised positions).
        2. Identify plurality stance (most common; break ties conservatively toward DENY/DEFER).
        3. Note any CRITICAL conflicts (APPROVE vs DENY across specialists).
        4. Formulate the shadow trade construction: symbolic entry parameters based on evidence.
        5. Output your ruling as JSON.

        STANCE OPTIONS for your ruling (same 5):
          APPROVE, APPROVE_REDUCED, WAIT_RECLAIM, DEFER, DENY

        SHADOW TRADE CONSTRUCTION (symbolic only, no execution):
          - entry_zone: what zone you would target
          - size_posture: FULL / REDUCED / MINIMAL
          - trail_posture: NORMAL / TIGHT / HOLD
          - key_condition: one sentence condition for entry
          - exit_profile: bounded exit policy template you would recommend.
            One of: FIXED_STANDARD, TRAIL_TIGHT, TRAIL_STANDARD, TRAIL_WIDE.
            Choose FIXED_STANDARD if a fixed-stop bracket is appropriate.
            Choose a TRAIL_* profile only when a trailing protective leg is
            structurally appropriate. The proposal_meta slice carries the
            real action's exit_policy / exit_profile so you can recommend
            agreement or dissent against the executed contract.
          - exit_policy: derived from exit_profile.
            FIXED_STANDARD       -> FIXED_BRACKET
            TRAIL_TIGHT/STANDARD/WIDE -> TRAIL_BRACKET
            (You must emit exit_policy explicitly even though it is derivable.)

        OUTPUT (JSON only, no prose, no markdown):
        {
          "shadow_stance": "<stance>",
          "shadow_confidence": <float 0.0-1.0>,
          "plurality_basis": "<which stances and counts drove the ruling>",
          "conflict_resolution": "<how you resolved any MAJOR/CRITICAL conflicts>",
          "top_supports": ["<evidence point>", ...],
          "top_tensions": ["<concern>", ...],
          "shadow_trade": {
            "entry_zone": "<description>",
            "size_posture": "<FULL|REDUCED|MINIMAL>",
            "trail_posture": "<NORMAL|TIGHT|HOLD>",
            "key_condition": "<one sentence>",
            "exit_profile": "<FIXED_STANDARD|TRAIL_TIGHT|TRAIL_STANDARD|TRAIL_WIDE>",
            "exit_policy": "<FIXED_BRACKET|TRAIL_BRACKET>",
            "advisory_only": true
          }
        }
      response: Return only the JSON ruling object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves any evidence slice for the SHADOW_CHAIR role. Full slice access.
            Available: proposal_meta, structural_state, thesis_summary, entry_zone,
            live_price, regime_state, live_bars, path_metrics, mfe_mae, invalidation,
            trust_label, deltas_summary, artifacts_summary,
            phase4_thesis_verdict (thesis_health/final_action/why_not_opposite/risk_treatment/
              prior_thesis_reference/actionability_summary; phase4_available=false for
              pre-Phase-4 proposals),
            phase4_dossier_context (continuation_quality/resistance_overhead_risk/
              candle_psychology/broken_resistance_as_support/levels/structural_timeline_summary;
              phase4_available=false for pre-Phase-4 proposals).
          input_schema:
            type: object
            properties:
              hearing_id:
                type: string
              role_name:
                type: string
                description: Must be SHADOW_CHAIR.
              slice_name:
                type: string
                description: >
                  Any slice from the full catalog including phase4_thesis_verdict
                  and phase4_dossier_context.
            required:
              - hearing_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_SHADOW_EVIDENCE_SLICE
  $$;


/* ================================================================
   APP_CONFIG: Shadow board feature flag + model key
   Insert only if not already present.
   ================================================================ */
/* Grants for the stored procedure backing */
GRANT USAGE ON PROCEDURE MIP.APP.GET_SHADOW_EVIDENCE_SLICE(VARCHAR, VARCHAR, VARCHAR) TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE MIP.APP.GET_SHADOW_EVIDENCE_SLICE(VARCHAR, VARCHAR, VARCHAR) TO ROLE MIP_UI_API_ROLE;

/* Grants for the agent objects — required because Cortex Agents REST API uses
   the user's DEFAULT_ROLE under JWT auth (MIP_UI_API_ROLE for MIP_UI_API).
   CREATE OR REPLACE AGENT drops grants, so this must be re-applied on each
   redeploy. */
GRANT USAGE ON AGENT MIP.APP.SHADOW_STRUCTURAL_THESIS_AGENT TO ROLE MIP_UI_API_ROLE;
GRANT USAGE ON AGENT MIP.APP.SHADOW_ENTRY_GEOMETRY_AGENT    TO ROLE MIP_UI_API_ROLE;
GRANT USAGE ON AGENT MIP.APP.SHADOW_REGIME_AGENT            TO ROLE MIP_UI_API_ROLE;
GRANT USAGE ON AGENT MIP.APP.SHADOW_PATH_TRADEABILITY_AGENT TO ROLE MIP_UI_API_ROLE;
GRANT USAGE ON AGENT MIP.APP.SHADOW_PROTECTION_EXIT_AGENT   TO ROLE MIP_UI_API_ROLE;
GRANT USAGE ON AGENT MIP.APP.SHADOW_SYMBOL_BEHAVIOR_AGENT   TO ROLE MIP_UI_API_ROLE;
GRANT USAGE ON AGENT MIP.APP.SHADOW_CHAIR_AGENT             TO ROLE MIP_UI_API_ROLE;

INSERT INTO MIP.APP.APP_CONFIG (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
SELECT 'SHADOW_BOARD_ENABLED', 'false', 'Shadow Board Phase 1 feature flag. Set to true to enable shadow board runs.'
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = 'SHADOW_BOARD_ENABLED');

INSERT INTO MIP.APP.APP_CONFIG (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
SELECT 'SHADOW_BOARD_MODEL', 'claude-4-sonnet', 'Model used for shadow board agents (read-only; model is baked into CREATE AGENT DDL).'
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = 'SHADOW_BOARD_MODEL');

INSERT INTO MIP.APP.APP_CONFIG (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
SELECT 'SHADOW_BOARD_TIMEOUT_SEC', '120', 'Per-agent timeout in seconds for shadow board Cortex Agents REST API calls.'
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = 'SHADOW_BOARD_TIMEOUT_SEC');

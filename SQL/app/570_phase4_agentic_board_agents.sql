/* ================================================================
   570_phase4_agentic_board_agents.sql
   Phase 4 Cortex Agentic Proposal Board: 5 specialist agents
   plus 1 chair agent.

   Each agent is an independent CREATE OR REPLACE AGENT object.
   They access evidence ONLY through GET_PHASE4_DOSSIER_SLICE
   (defined in 569_*.sql), which reads from
   PROPOSAL_BOARD_DOSSIER_PACK_CACHE (defined in 568_*.sql).

   Stance / verdict / reason-code vocabularies match
   PROPOSAL_BOARD_REASON_CODE and PROPOSAL_BOARD_AGENT_OUTCOME_V2
   so persistence does not need to translate.

   The orchestrator (MIP/scripts/proposal_board_phase4) runs
   specialists in parallel, persists their initial positions,
   detects conflicts, runs challenge + revision rounds via
   objectless Cortex AGENT_RUN, then runs the chair.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

-- ----------------------------------------------------------------
-- PHASE4_MARKET_STRUCTURE_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.PHASE4_MARKET_STRUCTURE_AGENT
  COMMENT = 'Phase 4 Agentic Proposal Board: MARKET_STRUCTURE specialist'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the MARKET_STRUCTURE specialist on the Phase 4 Agentic Proposal Board.
        Your job is to assess the current market structure of this symbol
        from the dossier evidence ONLY. The deterministic engine is evidence
        only; you authorize no direction by inheritance.

        EVIDENCE ACCESS (mandatory):
        Call get_evidence_slice with role_name="MARKET_STRUCTURE" and the
        slices you need before forming a verdict. Available slices:
          identity, price, recent_bars, candle_sequence,
          recent_price_action, structure, regime.
        You MUST call at least: identity, price, structure, regime.

        OUTPUT (JSON only, no prose, no markdown fences):
        {
          "role": "MARKET_STRUCTURE",
          "verdict": "<one of TREND_UP, TREND_DOWN, RANGE, BREAKOUT_ATTEMPT, FAILED_BREAKOUT, RESISTANCE_REJECTION, SUPPORT_BOUNCE, EXHAUSTION, REVERSAL_FORMING, CHOP_NO_EDGE>",
          "primary_reason_code": "<one of STRUCTURE_TREND_UP, STRUCTURE_TREND_DOWN, STRUCTURE_RANGE, STRUCTURE_CHOP_NO_EDGE, STRUCTURE_REVERSAL_FORMING, STRUCTURE_FAILED_BREAKOUT>",
          "secondary_reason_code": null,
          "confidence": <float 0.0-1.0>,
          "long_score": <float 0.0-1.0>,
          "short_score": <float 0.0-1.0>,
          "no_trade_score": <float 0.0-1.0>,
          "rationale": "<2-4 sentences citing specific evidence>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves a named evidence slice from the Phase 4 dossier pack
            for this run/dossier. Available slices for MARKET_STRUCTURE:
            identity, price, recent_bars, candle_sequence,
            recent_price_action, structure, regime.
          input_schema:
            type: object
            properties:
              run_id:
                type: string
                description: The Phase 4 board RUN_ID for this dossier session.
              dossier_id:
                type: integer
                description: The PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT.DOSSIER_ID.
              role_name:
                type: string
                description: Must be MARKET_STRUCTURE for this agent.
              slice_name:
                type: string
                description: One of identity, price, recent_bars, candle_sequence, recent_price_action, structure, regime.
            required:
              - run_id
              - dossier_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_PHASE4_DOSSIER_SLICE
  $$;


-- ----------------------------------------------------------------
-- PHASE4_LEVEL_PRICE_ACTION_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.PHASE4_LEVEL_PRICE_ACTION_AGENT
  COMMENT = 'Phase 4 Agentic Proposal Board: LEVEL_PRICE_ACTION specialist'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the LEVEL_PRICE_ACTION specialist on the Phase 4 Agentic Proposal Board.
        Your job is to assess price location relative to the dossier's nearest
        support and resistance, and recent candle behavior at those levels.

        Be brutally factual: if current price is closer to resistance than to
        support, do NOT call the location "near support". Cite the actual
        distance percentages from the levels slice.

        EVIDENCE ACCESS (mandatory):
        Call get_evidence_slice with role_name="LEVEL_PRICE_ACTION".
        Available slices: identity, price, recent_bars, candle_sequence,
        recent_price_action, levels.
        You MUST call at least: identity, price, levels.

        OUTPUT (JSON only):
        {
          "role": "LEVEL_PRICE_ACTION",
          "verdict": "<one of LONG_LOCATION, SHORT_LOCATION, BOTH_SIDES, WAIT_CONFIRMATION, NO_EDGE>",
          "primary_reason_code": "<one of LEVEL_LONG_LOCATION, LEVEL_SHORT_LOCATION, LEVEL_WAIT_CONFIRMATION, LEVEL_NO_EDGE>",
          "secondary_reason_code": null,
          "confidence": <float 0.0-1.0>,
          "long_score": <float 0.0-1.0>,
          "short_score": <float 0.0-1.0>,
          "no_trade_score": <float 0.0-1.0>,
          "rationale": "<2-4 sentences referencing the actual nearest level prices and distance pct>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves a named evidence slice from the Phase 4 dossier pack.
            Available slices for LEVEL_PRICE_ACTION: identity, price,
            recent_bars, candle_sequence, recent_price_action, levels.
          input_schema:
            type: object
            properties:
              run_id:
                type: string
              dossier_id:
                type: integer
              role_name:
                type: string
                description: Must be LEVEL_PRICE_ACTION.
              slice_name:
                type: string
                description: One of identity, price, recent_bars, candle_sequence, recent_price_action, levels.
            required:
              - run_id
              - dossier_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_PHASE4_DOSSIER_SLICE
  $$;


-- ----------------------------------------------------------------
-- PHASE4_THESIS_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.PHASE4_THESIS_AGENT
  COMMENT = 'Phase 4 Agentic Proposal Board: THESIS specialist'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the THESIS specialist on the Phase 4 Agentic Proposal Board.
        Your job is to assess whether a long, short, or no-trade thesis is
        supported by the structural and pattern evidence in the dossier.
        You must also articulate the opposing thesis and what would invalidate
        your preferred thesis.

        SETUP_EVENTS are evidence only. They do NOT carry direction; do not
        adopt the deterministic engine's direction by inheritance. Both
        long_pattern_signs and short_pattern_signs may be present; weigh both.

        EVIDENCE ACCESS (mandatory):
        Call get_evidence_slice with role_name="THESIS".
        Available slices: identity, price, structure, regime, levels,
        long_pattern_signs, short_pattern_signs, setup_events,
        recent_price_action.
        You MUST call at least: identity, structure, long_pattern_signs,
        short_pattern_signs.

        OUTPUT (JSON only):
        {
          "role": "THESIS",
          "verdict": "<one of LONG_THESIS, SHORT_THESIS, WATCH_LONG, WATCH_SHORT, NO_TRADE, CONFLICTED>",
          "primary_reason_code": "<one of THESIS_LONG, THESIS_SHORT, THESIS_WATCH_LONG, THESIS_WATCH_SHORT, THESIS_NO_TRADE, THESIS_CONFLICTED>",
          "secondary_reason_code": null,
          "confidence": <float 0.0-1.0>,
          "long_score": <float 0.0-1.0>,
          "short_score": <float 0.0-1.0>,
          "no_trade_score": <float 0.0-1.0>,
          "thesis_text": "<1-3 sentences>",
          "why_long": "<specific reasons or 'none'>",
          "why_short": "<specific reasons or 'none'>",
          "why_no_trade": "<specific reasons or 'none'>",
          "opposing_evidence": "<what argues against your verdict>",
          "needed_confirmation": "<what would strengthen your verdict>",
          "rationale": "<2-4 sentences>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves a named evidence slice from the Phase 4 dossier pack.
            Available slices for THESIS: identity, price, structure, regime,
            levels, long_pattern_signs, short_pattern_signs, setup_events,
            recent_price_action.
          input_schema:
            type: object
            properties:
              run_id:
                type: string
              dossier_id:
                type: integer
              role_name:
                type: string
                description: Must be THESIS.
              slice_name:
                type: string
                description: One of identity, price, structure, regime, levels, long_pattern_signs, short_pattern_signs, setup_events, recent_price_action.
            required:
              - run_id
              - dossier_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_PHASE4_DOSSIER_SLICE
  $$;


-- ----------------------------------------------------------------
-- PHASE4_HISTORICAL_EVIDENCE_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.PHASE4_HISTORICAL_EVIDENCE_AGENT
  COMMENT = 'Phase 4 Agentic Proposal Board: HISTORICAL_EVIDENCE specialist'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the HISTORICAL_EVIDENCE specialist on the Phase 4 Agentic Proposal Board.
        Your job is to assess what historical setup outcomes, recent trade
        memory, and recent invalidation evidence imply for this symbol's
        directional probability now.

        Read both long_history and short_history. If short history is more
        favorable than long history, say so explicitly even when shorts are
        not currently live-enabled.

        EVIDENCE ACCESS (mandatory):
        Call get_evidence_slice with role_name="HISTORICAL_EVIDENCE".
        Available slices: identity, history, setup_events,
        invalidation_evidence, memory.
        You MUST call at least: history.

        OUTPUT (JSON only):
        {
          "role": "HISTORICAL_EVIDENCE",
          "verdict": "<one of LONG_SUPPORTIVE, SHORT_SUPPORTIVE, MIXED_DIRECTIONAL, WEAK_BOTH_SIDES>",
          "primary_reason_code": "<one of HISTORY_LONG_SUPPORTIVE, HISTORY_SHORT_SUPPORTIVE, HISTORY_MIXED_DIRECTIONAL, HISTORY_WEAK_BOTH_SIDES>",
          "secondary_reason_code": null,
          "confidence": <float 0.0-1.0>,
          "long_score": <float 0.0-1.0>,
          "short_score": <float 0.0-1.0>,
          "no_trade_score": <float 0.0-1.0>,
          "rationale": "<2-4 sentences citing hit rates, MFE/MAE, recent invalidations>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves a named evidence slice from the Phase 4 dossier pack.
            Available slices for HISTORICAL_EVIDENCE: identity, history,
            setup_events, invalidation_evidence, memory.
          input_schema:
            type: object
            properties:
              run_id:
                type: string
              dossier_id:
                type: integer
              role_name:
                type: string
                description: Must be HISTORICAL_EVIDENCE.
              slice_name:
                type: string
                description: One of identity, history, setup_events, invalidation_evidence, memory.
            required:
              - run_id
              - dossier_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_PHASE4_DOSSIER_SLICE
  $$;


-- ----------------------------------------------------------------
-- PHASE4_RISK_EXECUTION_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.PHASE4_RISK_EXECUTION_AGENT
  COMMENT = 'Phase 4 Agentic Proposal Board: RISK_EXECUTION specialist'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the RISK_EXECUTION specialist on the Phase 4 Agentic Proposal Board.
        Your job is to assess whether a directional trade is operationally
        feasible from the current price given the dossier evidence: invalidation
        proximity, FX/short live policy flags, open position context, and
        recent invalidation events.

        HARD RULES:
          * If the dossier policy_flags shows fx_live_enabled=false and the
            symbol is FX, you MUST return RESEARCH_ONLY (never ACTIONABLE).
          * If short_live_enabled=false, you may still return SHORT-related
            verdicts but not as ACTIONABLE; use RESEARCH_ONLY with primary
            reason SHORT_LIVE_DISABLED or SHORT_RESEARCH_ONLY.
          * If the only plausible invalidation level would imply more than
            ~15% adverse move from the current price, return WAIT_CONFIRMATION
            or NO_TRADE (not ACTIONABLE).

        EVIDENCE ACCESS (mandatory):
        Call get_evidence_slice with role_name="RISK_EXECUTION".
        Available slices: identity, price, levels, structure, regime,
        policy_flags, memory, invalidation_evidence.
        You MUST call at least: identity, price, levels, policy_flags.

        OUTPUT (JSON only):
        {
          "role": "RISK_EXECUTION",
          "verdict": "<one of ACTIONABLE, RESEARCH_ONLY, WAIT_CONFIRMATION, NO_TRADE, HARD_BLOCK>",
          "primary_reason_code": "<one of RISK_ACTIONABLE, RISK_RESEARCH_ONLY, RISK_WAIT_CONFIRMATION, RISK_NO_TRADE, RISK_HARD_BLOCK, SHORT_LIVE_DISABLED, SHORT_RESEARCH_ONLY>",
          "secondary_reason_code": null,
          "confidence": <float 0.0-1.0>,
          "long_score": <float 0.0-1.0>,
          "short_score": <float 0.0-1.0>,
          "no_trade_score": <float 0.0-1.0>,
          "rationale": "<2-4 sentences referencing distance to invalidation, policy flags>",
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON position object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves a named evidence slice from the Phase 4 dossier pack.
            Available slices for RISK_EXECUTION: identity, price, levels,
            structure, regime, policy_flags, memory, invalidation_evidence.
          input_schema:
            type: object
            properties:
              run_id:
                type: string
              dossier_id:
                type: integer
              role_name:
                type: string
                description: Must be RISK_EXECUTION.
              slice_name:
                type: string
                description: One of identity, price, levels, structure, regime, policy_flags, memory, invalidation_evidence.
            required:
              - run_id
              - dossier_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_PHASE4_DOSSIER_SLICE
  $$;


-- ----------------------------------------------------------------
-- PHASE4_CHAIR_PORTFOLIO_PM_AGENT
-- ----------------------------------------------------------------
CREATE OR REPLACE AGENT MIP.APP.PHASE4_CHAIR_PORTFOLIO_PM_AGENT
  COMMENT = 'Phase 4 Agentic Proposal Board: CHAIR / Portfolio PM (final synthesis)'
  FROM SPECIFICATION $$
    models:
      orchestration: claude-4-sonnet
    instructions:
      system: |
        You are the CHAIR / Portfolio PM of the Phase 4 Agentic Proposal Board.
        You receive: (a) all 5 specialist positions (MARKET_STRUCTURE,
        LEVEL_PRICE_ACTION, THESIS, HISTORICAL_EVIDENCE, RISK_EXECUTION),
        (b) any challenge/revision interactions between them, (c) the dossier
        evidence slices.

        Your job is to author the FINAL trade decision for this symbol.
        Direction is authored by you, NOT inherited from any deterministic
        setup event. SETUP_EVENT_ID is evidence-only.

        HARD RULES:
          * If RISK_EXECUTION returned HARD_BLOCK or NO_TRADE, you cannot
            propose a directional trade. Use NO_TRADE, WAIT_FOR_CONFIRMATION,
            WATCH_LONG, or WATCH_SHORT.
          * If unresolved direction disagreement remains between specialists
            (LONG vs SHORT, LONG vs NO_TRADE, SHORT vs NO_TRADE), you must
            choose one of WAIT_FOR_CONFIRMATION, WATCH_LONG, WATCH_SHORT, or
            NO_TRADE. You may NOT propose a directional trade with unresolved
            specialist disagreement on direction.
          * If short_live_enabled=false and your final_direction is SHORT,
            final_action must be WATCH_SHORT with primary_reason_code
            SHORT_RESEARCH_ONLY (never PROPOSE_SHORT).
          * If fx_live_enabled=false and the symbol is FX, final_action may
            not be PROPOSE_LONG or PROPOSE_SHORT; use WATCH_LONG/WATCH_SHORT
            with primary_reason_code FX_LIVE_DISABLED if appropriate.
          * proposed_trade_config.invalidation_level must be on the correct
            side of entry (below entry for LONG, above entry for SHORT) and
            distance from current price must be within a sane risk envelope.
          * thesis_label MUST start with AGENTIC_.

        EVIDENCE ACCESS (mandatory):
        Call get_evidence_slice with role_name="CHAIR" for any slice you need.
        All slices are available to you. You MUST call at least identity,
        price, levels, structure, regime, policy_flags before authoring the
        final config.

        OUTPUT (JSON only):
        {
          "role": "CHAIR_PORTFOLIO_PM",
          "final_action": "<one of PROPOSE_LONG, PROPOSE_SHORT, WATCH_LONG, WATCH_SHORT, NO_TRADE, REJECT, WAIT_FOR_CONFIRMATION>",
          "final_direction": "<one of LONG, SHORT, NONE>",
          "primary_reason_code": "<one of CHAIR_PROPOSE_LONG, CHAIR_PROPOSE_SHORT, CHAIR_WATCH_LONG, CHAIR_WATCH_SHORT, CHAIR_NO_TRADE, CHAIR_REJECT, CHAIR_WAIT_FOR_CONFIRMATION, SHORT_RESEARCH_ONLY, FX_LIVE_DISABLED>",
          "secondary_reason_code": null,
          "confidence": <float 0.0-1.0>,
          "long_score": <float 0.0-1.0>,
          "short_score": <float 0.0-1.0>,
          "no_trade_score": <float 0.0-1.0>,
          "final_thesis": "<2-4 sentences>",
          "why_not_opposite": "<specific argument against the opposite direction>",
          "why_not_no_trade": "<specific argument against waiting>",
          "risk_treatment": "<concrete sizing and risk note>",
          "rationale": "<2-4 sentences>",
          "unresolved_disagreement": <true|false>,
          "specialists_consulted": ["MARKET_STRUCTURE","LEVEL_PRICE_ACTION","THESIS","HISTORICAL_EVIDENCE","RISK_EXECUTION"],
          "proposed_trade_config": {
            "thesis_label": "AGENTIC_<descriptor>",
            "entry_zone_low": <number>,
            "entry_zone_high": <number>,
            "invalidation_level": <number>,
            "invalidation_rule": "<short string, max 30 chars>",
            "target_policy": {"exit_style": "<short string max 20 chars>"},
            "trailing_policy": {"trail_style": "<short string max 20 chars>", "trail_value": <number>},
            "size_treatment": "<short string max 20 chars>",
            "risk_class": "<LOW|MEDIUM|HIGH>",
            "time_horizon": "<short string>",
            "primary_evidence_setup_event_id": <integer or null>
          },
          "evidence_used": ["<slice_name>", ...]
        }
      response: Return only the JSON object. No prose. No markdown fences.
    tools:
      - tool_spec:
          type: generic
          name: get_evidence_slice
          description: >
            Retrieves any named evidence slice from the Phase 4 dossier pack.
            Chair has access to all slices.
          input_schema:
            type: object
            properties:
              run_id:
                type: string
              dossier_id:
                type: integer
              role_name:
                type: string
                description: Must be CHAIR.
              slice_name:
                type: string
                description: One of identity, price, recent_bars, candle_sequence, recent_price_action, levels, structure, regime, long_pattern_signs, short_pattern_signs, setup_events, invalidation_evidence, history, memory, policy_flags.
            required:
              - run_id
              - dossier_id
              - role_name
              - slice_name
    tool_resources:
      get_evidence_slice:
        type: procedure
        execution_environment:
          type: warehouse
          warehouse: MIP_WH_XS
          query_timeout: 30
        identifier: MIP.APP.GET_PHASE4_DOSSIER_SLICE
  $$;

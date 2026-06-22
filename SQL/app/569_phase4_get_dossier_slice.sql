/* ================================================================
   569_phase4_get_dossier_slice.sql
   Phase 4 Cortex Agentic Proposal Board: GET_PHASE4_DOSSIER_SLICE
   Pure-SQL stored procedure used as the `generic` tool backing
   for all PHASE4_*_AGENT objects.

   PERFORMANCE NOTE (why this is LANGUAGE SQL, not Python):
     This procedure was originally a Snowpark/Python SP. Python SPs
     carry multi-second sandbox cold/warm-start overhead on every
     CALL (measured 9-30s on MIP_WH_XS even for a zero-row lookup).
     The agent panel runs up to inter_dossier_concurrency(2) x
     per_dossier_concurrency(5) = 10 specialist agents at once, each
     calling this tool ~7 times. Concurrent Python-SP cold starts
     saturated the XS warehouse so individual slice calls blew past
     the Cortex tool query_timeout (90s) -> HTTP 408 (error 000630)
     -> agents received no evidence -> they refused to fabricate a
     verdict and returned prose -> validator flagged the position
     MISSING_OR_NON_OBJECT_JSON -> INVALID_SPECIALISTS -> zero
     proposals. This pure-SQL rewrite removes the Python runtime so a
     slice lookup is a sub-second single-row read, eliminating the
     saturation/timeout failure mode. Output shape, role/slice
     allowlists, error codes, and slice->payload-key mapping are
     identical to the prior Python implementation.

   Mirrors GET_SHADOW_EVIDENCE_SLICE shape and guard pattern.

   ALLOWED ROLES:
     MARKET_STRUCTURE,
     LEVEL_PRICE_ACTION,
     THESIS,
     HISTORICAL_EVIDENCE,
     RISK_EXECUTION,
     CHAIR

   ALLOWED SLICES (full catalog):
     identity                      -> symbol/market_type/as_of_date/portfolio
     price                         -> current price + source + recent close ref
     recent_bars                   -> compact recent bar array (20 daily bars)
     candle_sequence               -> compact recent candle pattern descriptors
     recent_price_action           -> short text summary
     levels                        -> nearest support/resistance + significance + distance
     zone_context                  -> alias for levels (S/R zones, broken_resistance_as_support)
     structure                     -> structural state + state_confidence
     regime                        -> trend/vol/range regime tags
     long_pattern_signs            -> array of long-side pattern evidence
     short_pattern_signs           -> array of short-side pattern evidence
     setup_events                  -> evidence-only setup events (no direction inheritance)
     invalidation_evidence         -> recent invalidation observations
     history                       -> long_history + short_history arrays
     memory                        -> recent_trade_memory + recent_proposal_memory + open_position_context
     policy_flags                  -> short_research_visible / short_live_enabled / fx_live_enabled / warnings
     structural_timeline_summary   -> 90D OHLC summary (compact). PHASE 4 EVIDENCE V1.
     structural_timeline_bars      -> 90D OHLC dated bars (heavier). CHAIR ONLY. PHASE 4 EVIDENCE V1.
     candle_psychology             -> per-bar multi-labels + recent cluster classification. PHASE 4 EVIDENCE V1.
     actionability_context         -> deterministic synthesis: overhead risk, continuation_quality,
                                      entry_location_quality, target_path_clear, confirmation_needed.
                                      PHASE 4 EVIDENCE V1.
     market_structure_map          -> deterministic swing/BOS/CHOCH map (wick pivots, body-close breaks).
                                      PHASE 4 EVIDENCE V2.

   Role-to-slice access map (closed world):
     MARKET_STRUCTURE     -> identity, price, recent_bars, candle_sequence,
                              recent_price_action, structure, regime,
                              structural_timeline_summary, candle_psychology,
                              actionability_context, market_structure_map
     LEVEL_PRICE_ACTION   -> identity, price, recent_bars, candle_sequence,
                              recent_price_action, levels, zone_context,
                              candle_psychology, actionability_context, market_structure_map
     THESIS               -> identity, price, structure, regime, levels, zone_context,
                              long_pattern_signs, short_pattern_signs,
                              setup_events, recent_price_action,
                              structural_timeline_summary, actionability_context, market_structure_map
     HISTORICAL_EVIDENCE  -> identity, history, setup_events,
                              invalidation_evidence, memory
     RISK_EXECUTION       -> identity, price, levels, zone_context, structure, regime,
                              policy_flags, memory, invalidation_evidence,
                              actionability_context
     CHAIR                -> all slices above PLUS structural_timeline_bars and market_structure_map

   No dynamic SQL. No writes. Reads PROPOSAL_BOARD_DOSSIER_PACK_CACHE only.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE OR REPLACE PROCEDURE MIP.APP.GET_PHASE4_DOSSIER_SLICE(
    RUN_ID      VARCHAR,
    DOSSIER_ID  NUMBER,
    ROLE_NAME   VARCHAR,
    SLICE_NAME  VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    role_u    STRING;
    slice_l   STRING;
    rid       STRING;
    did       NUMBER;
    pkey      STRING;
    role_slices ARRAY;
    pack      VARIANT;
    slice_val VARIANT;
    -- slice_name -> dossier payload key (most identical). zone_context
    -- aliases 'levels'; recent_price_action -> recent_price_action_summary;
    -- setup_events -> setup_events_evidence_only; policy_flags -> policy.
    payload_key_map OBJECT;
    -- closed-world role -> allowed slices map.
    role_slice_map  OBJECT;
BEGIN
    role_u  := UPPER(TRIM(COALESCE(:ROLE_NAME, '')));
    slice_l := LOWER(TRIM(COALESCE(:SLICE_NAME, '')));
    rid     := TRIM(COALESCE(:RUN_ID, ''));
    did     := :DOSSIER_ID;

    payload_key_map := OBJECT_CONSTRUCT(
        'identity', 'identity',
        'price', 'price',
        'recent_bars', 'recent_bars',
        'candle_sequence', 'candle_sequence',
        'recent_price_action', 'recent_price_action_summary',
        'levels', 'levels',
        'zone_context', 'levels',
        'structure', 'structure',
        'regime', 'regime',
        'long_pattern_signs', 'long_pattern_signs',
        'short_pattern_signs', 'short_pattern_signs',
        'setup_events', 'setup_events_evidence_only',
        'invalidation_evidence', 'invalidation_evidence',
        'history', 'history',
        'memory', 'memory',
        'policy_flags', 'policy',
        'structural_timeline_summary', 'structural_timeline_summary',
        'structural_timeline_bars', 'structural_timeline_bars',
        'candle_psychology', 'candle_psychology',
        'actionability_context', 'actionability_context',
        'market_structure_map', 'market_structure_map'
    );

    role_slice_map := OBJECT_CONSTRUCT(
        'MARKET_STRUCTURE', ARRAY_CONSTRUCT(
            'identity', 'price', 'recent_bars', 'candle_sequence',
            'recent_price_action', 'structure', 'regime',
            'structural_timeline_summary', 'candle_psychology',
            'actionability_context', 'market_structure_map'),
        'LEVEL_PRICE_ACTION', ARRAY_CONSTRUCT(
            'identity', 'price', 'recent_bars', 'candle_sequence',
            'recent_price_action', 'levels', 'zone_context',
            'candle_psychology', 'actionability_context', 'market_structure_map'),
        'THESIS', ARRAY_CONSTRUCT(
            'identity', 'price', 'structure', 'regime', 'levels', 'zone_context',
            'long_pattern_signs', 'short_pattern_signs',
            'setup_events', 'recent_price_action',
            'structural_timeline_summary', 'actionability_context', 'market_structure_map'),
        'HISTORICAL_EVIDENCE', ARRAY_CONSTRUCT(
            'identity', 'history', 'setup_events',
            'invalidation_evidence', 'memory'),
        'RISK_EXECUTION', ARRAY_CONSTRUCT(
            'identity', 'price', 'levels', 'zone_context',
            'structure', 'regime',
            'policy_flags', 'memory', 'invalidation_evidence',
            'actionability_context'),
        'CHAIR', ARRAY_CONSTRUCT(
            'identity', 'price', 'recent_bars', 'candle_sequence',
            'recent_price_action', 'levels', 'zone_context',
            'structure', 'regime',
            'long_pattern_signs', 'short_pattern_signs',
            'setup_events', 'invalidation_evidence',
            'history', 'memory', 'policy_flags',
            'structural_timeline_summary', 'structural_timeline_bars',
            'candle_psychology', 'actionability_context', 'market_structure_map')
    );

    -- Guard order mirrors the prior Python implementation exactly.
    IF (GET(role_slice_map, role_u) IS NULL) THEN
        RETURN OBJECT_CONSTRUCT('error', 'ROLE_NOT_ALLOWED', 'role', role_u);
    END IF;

    IF (GET(payload_key_map, slice_l) IS NULL) THEN
        RETURN OBJECT_CONSTRUCT('error', 'SLICE_NOT_IN_CATALOG', 'slice', slice_l);
    END IF;

    role_slices := GET(role_slice_map, role_u)::ARRAY;
    IF (NOT ARRAY_CONTAINS(slice_l::VARIANT, role_slices)) THEN
        RETURN OBJECT_CONSTRUCT(
            'error', 'SLICE_NOT_ALLOWED_FOR_ROLE', 'role', role_u, 'slice', slice_l);
    END IF;

    IF (rid = '') THEN
        RETURN OBJECT_CONSTRUCT('error', 'RUN_ID_REQUIRED');
    END IF;

    IF (did IS NULL) THEN
        RETURN OBJECT_CONSTRUCT('error', 'DOSSIER_ID_REQUIRED');
    END IF;

    pack := (
        SELECT PACK_JSON
        FROM MIP.APP.PROPOSAL_BOARD_DOSSIER_PACK_CACHE
        WHERE RUN_ID = :rid
          AND DOSSIER_ID = :did
          AND EXPIRES_AT > CURRENT_TIMESTAMP()
        LIMIT 1
    );

    IF (pack IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'error', 'PACK_NOT_FOUND_OR_EXPIRED', 'run_id', rid, 'dossier_id', did);
    END IF;

    pkey := GET(payload_key_map, slice_l)::STRING;
    slice_val := GET(pack, pkey);

    -- KEEP_NULL so 'payload' is present as JSON null when the key is absent,
    -- matching the Python version's {'payload': None}.
    RETURN OBJECT_CONSTRUCT_KEEP_NULL(
        'run_id', rid,
        'dossier_id', did,
        'role', role_u,
        'slice', slice_l,
        'payload', slice_val
    );
END;
$$;

GRANT USAGE ON PROCEDURE MIP.APP.GET_PHASE4_DOSSIER_SLICE(VARCHAR, NUMBER, VARCHAR, VARCHAR) TO ROLE MIP_ADMIN_ROLE;

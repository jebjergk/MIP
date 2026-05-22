/* ================================================================
   566_sp_run_proposal_board_phase4_symbol_dossier_combined.sql
   Phase 4 active Agentic Proposal Board, optimized active procedure.

   One Cortex call per symbol dossier returns all required role outputs.
   No deterministic fallback exists. Invalid output fails closed.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE OR REPLACE PROCEDURE MIP.APP.SP_RUN_PROPOSAL_BOARD(
    P_PORTFOLIO_ID         NUMBER  DEFAULT NULL,
    P_MAX_PROPOSALS        INTEGER DEFAULT 8,
    P_AS_OF_DATE           DATE    DEFAULT NULL,
    P_SYMBOL_COOLDOWN_DAYS INTEGER DEFAULT 7
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_as_of             DATE := COALESCE(P_AS_OF_DATE, CURRENT_DATE());
    v_run_id            VARCHAR(36) := UUID_STRING();
    v_run_start         TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_model_name        VARCHAR := 'mistral-large2';
    v_dossier_count     NUMBER := 0;
    v_agent_count       NUMBER := 0;
    v_invalid_count     NUMBER := 0;
    v_final_count       NUMBER := 0;
    v_published_count   NUMBER := 0;
BEGIN
    INSERT INTO MIP.APP.PROPOSAL_BOARD_RUN (
        RUN_ID, AS_OF_DATE, PORTFOLIO_ID, RUN_STATUS,
        MODEL_CONFIG_JSON, PROMPT_VERSION, POLICY_VERSION
    )
    SELECT
        :v_run_id,
        :v_as_of,
        :P_PORTFOLIO_ID,
        'RUNNING',
        OBJECT_CONSTRUCT(
            'mode', 'symbol_dossier_agentic_thesis_board',
            'specialist_model', :v_model_name,
            'fallback_policy', 'fail_closed_no_deterministic_fallback',
            'chair_mode', 'cortex_agentic_portfolio_pm',
            'cortex_call_strategy', 'one_call_per_symbol_all_roles',
            'direction_source', 'CHAIR_PORTFOLIO_PM.final_direction',
            'setup_event_id_role', 'PRIMARY_EVIDENCE_SETUP_EVENT_ID_EVIDENCE_ONLY',
            'specialists', ARRAY_CONSTRUCT(
                'MARKET_STRUCTURE_AGENT',
                'LEVEL_PRICE_ACTION_AGENT',
                'THESIS_AGENT',
                'HISTORICAL_EVIDENCE_AGENT',
                'RISK_EXECUTION_FEASIBILITY_AGENT',
                'CHAIR_PORTFOLIO_PM'
            )
        ),
        'proposal_board_v3_phase4_symbol_dossier_combined',
        'proposal_board_policy_v3_phase4_agentic_cutover';

    INSERT INTO MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT (
        RUN_ID, DOSSIER_KEY, AS_OF_DATE, PORTFOLIO_ID, SYMBOL, MARKET_TYPE,
        CURRENT_PRICE, CURRENT_PRICE_SOURCE, CURRENT_PRICE_DATE,
        PRIMARY_EVIDENCE_SETUP_EVENT_ID,
        SHORT_RESEARCH_VISIBLE, SHORT_LIVE_ENABLED, FX_LIVE_ENABLED,
        DATA_QUALITY_FLAGS, BOARD_WARNING_FLAGS,
        DOSSIER_PAYLOAD_JSON, PAYLOAD_HASH
    )
    SELECT
        :v_run_id,
        SYMBOL || '|' || MARKET_TYPE || '|' || TO_VARCHAR(:v_as_of) || '|' || COALESCE(TO_VARCHAR(:P_PORTFOLIO_ID), 'ALL'),
        :v_as_of,
        :P_PORTFOLIO_ID,
        SYMBOL,
        MARKET_TYPE,
        CURRENT_PRICE,
        CURRENT_PRICE_SOURCE,
        CURRENT_PRICE_DATE,
        PRIMARY_EVIDENCE_SETUP_EVENT_ID,
        SHORT_RESEARCH_VISIBLE,
        SHORT_LIVE_ENABLED,
        FX_LIVE_ENABLED,
        DATA_QUALITY_FLAGS,
        BOARD_WARNING_FLAGS,
        OBJECT_INSERT(
            OBJECT_INSERT(
                OBJECT_INSERT(DOSSIER_PAYLOAD_JSON, 'portfolio_id', :P_PORTFOLIO_ID, TRUE),
                'direction_source_rule', 'CHAIR_PORTFOLIO_PM.final_direction', TRUE
            ),
            'setup_event_id_rule', 'PRIMARY_EVIDENCE_SETUP_EVENT_ID_EVIDENCE_ONLY', TRUE
        ),
        PAYLOAD_HASH
    FROM MIP.MART.V_PROPOSAL_BOARD_SYMBOL_DOSSIER
    WHERE AS_OF_DATE = :v_as_of
      AND ARRAY_SIZE(COALESCE(DATA_QUALITY_FLAGS, ARRAY_CONSTRUCT())) = 0
      AND PRIMARY_EVIDENCE_SETUP_EVENT_ID IS NOT NULL;

    SELECT COUNT(*) INTO :v_dossier_count
    FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT
    WHERE RUN_ID = :v_run_id;

    UPDATE MIP.APP.PROPOSAL_BOARD_RUN
       SET CANDIDATE_COUNT = :v_dossier_count
     WHERE RUN_ID = :v_run_id;

    IF (v_dossier_count = 0) THEN
        UPDATE MIP.APP.PROPOSAL_BOARD_RUN
           SET RUN_STATUS = 'FAILED',
               FINISHED_AT = CURRENT_TIMESTAMP(),
               ERROR_JSON = OBJECT_CONSTRUCT('message', 'NO_SYMBOL_DOSSIERS', 'reason_code', 'AGENT_OUTPUT_INVALID')
         WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT('status', 'FAILED', 'run_id', :v_run_id, 'reason_code', 'NO_SYMBOL_DOSSIERS');
    END IF;

    CREATE OR REPLACE TEMPORARY TABLE TMP_PHASE4_BOARD_RAW (
        RUN_ID VARCHAR,
        DOSSIER_ID NUMBER,
        PROMPT_TEXT VARCHAR,
        RAW_TEXT VARCHAR,
        CLEANED_TEXT VARCHAR,
        BOARD_JSON VARIANT
    );

    INSERT INTO TMP_PHASE4_BOARD_RAW
    WITH prompted AS (
        SELECT
            RUN_ID,
            DOSSIER_ID,
            'You are the active MIP Phase 4 Agentic Symbol-Dossier Proposal Board. '
            || 'Evaluate this symbol from facts, not from a pre-labelled candidate. '
            || 'The deterministic engine is evidence-only. Setup events are evidence arrays only. '
            || 'Direction must be authored only by chair.final_direction. '
            || 'If short_live_enabled=false and structural analysis supports SHORT: output PROPOSE_SHORT with primary_reason_code CHAIR_PROPOSE_SHORT. The backend marks the row POLICY_BLOCKED and prevents live execution. Do NOT output WATCH_SHORT or PROPOSE_LONG merely because short execution is disabled. A LONG verdict after predominant SHORT evidence requires explicit independent long evidence (failed-short reversal, resistance reclaim, higher-low defense, support hold, or bullish continuation structure). '
            || 'Return ONLY one JSON object, no markdown. Required top-level keys: market_structure, level_price_action, thesis, historical_evidence, risk_execution, chair. '
            || 'Do not nest one role inside another role. historical_evidence must be a top-level object, not a child of thesis. '
            || 'Each role object must include all of these exact keys: verdict, primary_reason_code, secondary_reason_code, confidence, rationale_text, long_score, short_score, no_trade_score. Use numeric scores between 0 and 1. '
            || 'market_structure verdict allowed: TREND_UP, TREND_DOWN, RANGE, BREAKOUT_ATTEMPT, FAILED_BREAKOUT, RESISTANCE_REJECTION, SUPPORT_BOUNCE, EXHAUSTION, REVERSAL_FORMING, CHOP_NO_EDGE. '
            || 'market_structure primary_reason_code allowed: STRUCTURE_TREND_UP, STRUCTURE_TREND_DOWN, STRUCTURE_RANGE, STRUCTURE_CHOP_NO_EDGE, STRUCTURE_REVERSAL_FORMING, STRUCTURE_FAILED_BREAKOUT. '
            || 'level_price_action verdict allowed: LONG_LOCATION, SHORT_LOCATION, BOTH_SIDES, WAIT_CONFIRMATION, NO_EDGE. '
            || 'level_price_action primary_reason_code allowed: LEVEL_LONG_LOCATION, LEVEL_SHORT_LOCATION, LEVEL_WAIT_CONFIRMATION, LEVEL_NO_EDGE. '
            || 'thesis verdict allowed: LONG_THESIS, SHORT_THESIS, WATCH_LONG, WATCH_SHORT, NO_TRADE, CONFLICTED. '
            || 'thesis primary_reason_code allowed: THESIS_LONG, THESIS_SHORT, THESIS_WATCH_LONG, THESIS_WATCH_SHORT, THESIS_NO_TRADE, THESIS_CONFLICTED. '
            || 'thesis must also include thesis_text, why_long, why_short, why_no_trade, opposing_evidence, needed_confirmation. '
            || 'historical_evidence verdict allowed: LONG_SUPPORTIVE, SHORT_SUPPORTIVE, MIXED_DIRECTIONAL, WEAK_BOTH_SIDES. '
            || 'historical_evidence primary_reason_code allowed: HISTORY_LONG_SUPPORTIVE, HISTORY_SHORT_SUPPORTIVE, HISTORY_MIXED_DIRECTIONAL, HISTORY_WEAK_BOTH_SIDES. '
            || 'risk_execution verdict allowed: ACTIONABLE, RESEARCH_ONLY, WAIT_CONFIRMATION, NO_TRADE, HARD_BLOCK. '
            || 'risk_execution primary_reason_code allowed: RISK_ACTIONABLE, RISK_RESEARCH_ONLY, RISK_WAIT_CONFIRMATION, RISK_NO_TRADE, RISK_HARD_BLOCK, SHORT_LIVE_DISABLED, SHORT_RESEARCH_ONLY. '
            || 'chair must include these exact keys: final_action, final_direction, primary_reason_code, secondary_reason_code, confidence, final_thesis, why_not_opposite, why_not_no_trade, risk_treatment, rationale_text, proposed_trade_config, committee_payload, long_score, short_score, no_trade_score. Use why_not_opposite exactly; do not use why_not_opposing. '
            || 'chair final_action allowed: PROPOSE_LONG, PROPOSE_SHORT, WATCH_LONG, WATCH_SHORT, NO_TRADE, REJECT, WAIT_FOR_CONFIRMATION. Do not invent RESEARCH_ONLY as a chair final_action. For short research, use final_action WATCH_SHORT, final_direction SHORT, primary_reason_code SHORT_RESEARCH_ONLY. '
            || 'chair final_direction allowed: LONG, SHORT, NONE. '
            || 'chair primary_reason_code allowed: CHAIR_PROPOSE_LONG, CHAIR_PROPOSE_SHORT, CHAIR_WATCH_LONG, CHAIR_WATCH_SHORT, CHAIR_NO_TRADE, CHAIR_REJECT, CHAIR_WAIT_FOR_CONFIRMATION, SHORT_RESEARCH_ONLY. Do not invent CHAIR_RESEARCH_ONLY. '
            || 'chair.proposed_trade_config must include thesis_label starting with AGENTIC_, entry_zone_low, entry_zone_high, invalidation_level, invalidation_rule, target_policy, trailing_policy, size_treatment, time_horizon, primary_evidence_setup_event_id. '
            || 'Before returning, verify: six top-level role keys exist; each role has long_score, short_score, no_trade_score; chair has why_not_opposite; no role is nested inside thesis. '
            || 'Never say setup_event_id is the source of direction; it is PRIMARY_EVIDENCE_SETUP_EVENT_ID evidence-only. '
            || 'Dossier JSON: ' || TO_JSON(DOSSIER_PAYLOAD_JSON) AS PROMPT_TEXT
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT
        WHERE RUN_ID = :v_run_id
    ),
    cortexed AS (
        SELECT p.*, SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            c.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(c.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT
        FROM cortexed c
    )
    SELECT RUN_ID, DOSSIER_ID, PROMPT_TEXT, RAW_TEXT, CLEANED_TEXT, TRY_PARSE_JSON(CLEANED_TEXT)
    FROM cleaned;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, ERROR_TYPE, RAW_OUTPUT_JSON, ERROR_MESSAGE
    )
    SELECT
        RUN_ID,
        DOSSIER_ID,
        'SYMBOL_DOSSIER_BOARD',
        'AGENT_OUTPUT_INVALID',
        OBJECT_CONSTRUCT('prompt_text', PROMPT_TEXT, 'raw_text', RAW_TEXT, 'cleaned_text', CLEANED_TEXT, 'parsed_json', BOARD_JSON),
        'Phase 4 symbol-dossier board requires valid nested JSON for all roles; deterministic fallback is forbidden.'
    FROM TMP_PHASE4_BOARD_RAW
    WHERE NOT COALESCE((
        BOARD_JSON IS NOT NULL
        AND BOARD_JSON:market_structure:verdict::STRING IN ('TREND_UP','TREND_DOWN','RANGE','BREAKOUT_ATTEMPT','FAILED_BREAKOUT','RESISTANCE_REJECTION','SUPPORT_BOUNCE','EXHAUSTION','REVERSAL_FORMING','CHOP_NO_EDGE')
        AND BOARD_JSON:market_structure:primary_reason_code::STRING IN ('STRUCTURE_TREND_UP','STRUCTURE_TREND_DOWN','STRUCTURE_RANGE','STRUCTURE_CHOP_NO_EDGE','STRUCTURE_REVERSAL_FORMING','STRUCTURE_FAILED_BREAKOUT')
        AND TRY_TO_DOUBLE(BOARD_JSON:market_structure:confidence::STRING) BETWEEN 0.0 AND 1.0
        AND BOARD_JSON:level_price_action:verdict::STRING IN ('LONG_LOCATION','SHORT_LOCATION','BOTH_SIDES','WAIT_CONFIRMATION','NO_EDGE')
        AND BOARD_JSON:level_price_action:primary_reason_code::STRING IN ('LEVEL_LONG_LOCATION','LEVEL_SHORT_LOCATION','LEVEL_WAIT_CONFIRMATION','LEVEL_NO_EDGE')
        AND TRY_TO_DOUBLE(BOARD_JSON:level_price_action:confidence::STRING) BETWEEN 0.0 AND 1.0
        AND BOARD_JSON:thesis:verdict::STRING IN ('LONG_THESIS','SHORT_THESIS','WATCH_LONG','WATCH_SHORT','NO_TRADE','CONFLICTED')
        AND BOARD_JSON:thesis:primary_reason_code::STRING IN ('THESIS_LONG','THESIS_SHORT','THESIS_WATCH_LONG','THESIS_WATCH_SHORT','THESIS_NO_TRADE','THESIS_CONFLICTED')
        AND TRY_TO_DOUBLE(BOARD_JSON:thesis:confidence::STRING) BETWEEN 0.0 AND 1.0
        AND BOARD_JSON:historical_evidence:verdict::STRING IN ('LONG_SUPPORTIVE','SHORT_SUPPORTIVE','MIXED_DIRECTIONAL','WEAK_BOTH_SIDES')
        AND BOARD_JSON:historical_evidence:primary_reason_code::STRING IN ('HISTORY_LONG_SUPPORTIVE','HISTORY_SHORT_SUPPORTIVE','HISTORY_MIXED_DIRECTIONAL','HISTORY_WEAK_BOTH_SIDES')
        AND TRY_TO_DOUBLE(BOARD_JSON:historical_evidence:confidence::STRING) BETWEEN 0.0 AND 1.0
        AND BOARD_JSON:risk_execution:verdict::STRING IN ('ACTIONABLE','RESEARCH_ONLY','WAIT_CONFIRMATION','NO_TRADE','HARD_BLOCK')
        AND BOARD_JSON:risk_execution:primary_reason_code::STRING IN ('RISK_ACTIONABLE','RISK_RESEARCH_ONLY','RISK_WAIT_CONFIRMATION','RISK_NO_TRADE','RISK_HARD_BLOCK','SHORT_LIVE_DISABLED','SHORT_RESEARCH_ONLY')
        AND TRY_TO_DOUBLE(BOARD_JSON:risk_execution:confidence::STRING) BETWEEN 0.0 AND 1.0
        AND BOARD_JSON:chair:final_action::STRING IN ('PROPOSE_LONG','PROPOSE_SHORT','WATCH_LONG','WATCH_SHORT','NO_TRADE','REJECT','WAIT_FOR_CONFIRMATION')
        AND BOARD_JSON:chair:final_direction::STRING IN ('LONG','SHORT','NONE')
        AND BOARD_JSON:chair:primary_reason_code::STRING IN ('CHAIR_PROPOSE_LONG','CHAIR_PROPOSE_SHORT','CHAIR_WATCH_LONG','CHAIR_WATCH_SHORT','CHAIR_NO_TRADE','CHAIR_REJECT','CHAIR_WAIT_FOR_CONFIRMATION','SHORT_RESEARCH_ONLY')
        AND TRY_TO_DOUBLE(BOARD_JSON:chair:confidence::STRING) BETWEEN 0.0 AND 1.0
        AND BOARD_JSON:chair:proposed_trade_config:thesis_label::STRING ILIKE 'AGENTIC_%'
    ), FALSE);

    SELECT COUNT(*) INTO :v_invalid_count
    FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR
    WHERE RUN_ID = :v_run_id;

    IF (v_invalid_count > 0) THEN
        UPDATE MIP.APP.PROPOSAL_BOARD_RUN
           SET RUN_STATUS = 'FAILED',
               FINISHED_AT = CURRENT_TIMESTAMP(),
               ERROR_JSON = OBJECT_CONSTRUCT('message', 'AGENT_OUTPUT_INVALID_FAIL_CLOSED', 'invalid_count', :v_invalid_count, 'reason_code', 'AGENT_OUTPUT_INVALID')
         WHERE RUN_ID = :v_run_id;
        RETURN OBJECT_CONSTRUCT('status', 'FAILED', 'run_id', :v_run_id, 'invalid_count', :v_invalid_count, 'reason_code', 'AGENT_OUTPUT_INVALID');
    END IF;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 (
        RUN_ID, DOSSIER_ID, AGENT_NAME, VERDICT, PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE, CONFIDENCE, LONG_SCORE, SHORT_SCORE,
        NO_TRADE_SCORE, RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
    )
    SELECT RUN_ID, DOSSIER_ID, 'MARKET_STRUCTURE_AGENT', BOARD_JSON:market_structure:verdict::STRING, BOARD_JSON:market_structure:primary_reason_code::STRING, IFF(IS_NULL_VALUE(BOARD_JSON:market_structure:secondary_reason_code), NULL, BOARD_JSON:market_structure:secondary_reason_code::STRING), TRY_TO_DOUBLE(BOARD_JSON:market_structure:confidence::STRING), TRY_TO_DOUBLE(BOARD_JSON:market_structure:long_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:market_structure:short_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:market_structure:no_trade_score::STRING), BOARD_JSON:market_structure:rationale_text::STRING, OBJECT_INSERT(OBJECT_INSERT(BOARD_JSON:market_structure, 'mode', 'cortex', TRUE), 'model', :v_model_name, TRUE) FROM TMP_PHASE4_BOARD_RAW
    UNION ALL
    SELECT RUN_ID, DOSSIER_ID, 'LEVEL_PRICE_ACTION_AGENT', BOARD_JSON:level_price_action:verdict::STRING, BOARD_JSON:level_price_action:primary_reason_code::STRING, IFF(IS_NULL_VALUE(BOARD_JSON:level_price_action:secondary_reason_code), NULL, BOARD_JSON:level_price_action:secondary_reason_code::STRING), TRY_TO_DOUBLE(BOARD_JSON:level_price_action:confidence::STRING), TRY_TO_DOUBLE(BOARD_JSON:level_price_action:long_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:level_price_action:short_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:level_price_action:no_trade_score::STRING), BOARD_JSON:level_price_action:rationale_text::STRING, OBJECT_INSERT(OBJECT_INSERT(BOARD_JSON:level_price_action, 'mode', 'cortex', TRUE), 'model', :v_model_name, TRUE) FROM TMP_PHASE4_BOARD_RAW
    UNION ALL
    SELECT RUN_ID, DOSSIER_ID, 'THESIS_AGENT', BOARD_JSON:thesis:verdict::STRING, BOARD_JSON:thesis:primary_reason_code::STRING, IFF(IS_NULL_VALUE(BOARD_JSON:thesis:secondary_reason_code), NULL, BOARD_JSON:thesis:secondary_reason_code::STRING), TRY_TO_DOUBLE(BOARD_JSON:thesis:confidence::STRING), TRY_TO_DOUBLE(BOARD_JSON:thesis:long_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:thesis:short_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:thesis:no_trade_score::STRING), BOARD_JSON:thesis:rationale_text::STRING, OBJECT_INSERT(OBJECT_INSERT(BOARD_JSON:thesis, 'mode', 'cortex', TRUE), 'model', :v_model_name, TRUE) FROM TMP_PHASE4_BOARD_RAW
    UNION ALL
    SELECT RUN_ID, DOSSIER_ID, 'HISTORICAL_EVIDENCE_AGENT', BOARD_JSON:historical_evidence:verdict::STRING, BOARD_JSON:historical_evidence:primary_reason_code::STRING, IFF(IS_NULL_VALUE(BOARD_JSON:historical_evidence:secondary_reason_code), NULL, BOARD_JSON:historical_evidence:secondary_reason_code::STRING), TRY_TO_DOUBLE(BOARD_JSON:historical_evidence:confidence::STRING), TRY_TO_DOUBLE(BOARD_JSON:historical_evidence:long_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:historical_evidence:short_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:historical_evidence:no_trade_score::STRING), BOARD_JSON:historical_evidence:rationale_text::STRING, OBJECT_INSERT(OBJECT_INSERT(BOARD_JSON:historical_evidence, 'mode', 'cortex', TRUE), 'model', :v_model_name, TRUE) FROM TMP_PHASE4_BOARD_RAW
    UNION ALL
    SELECT RUN_ID, DOSSIER_ID, 'RISK_EXECUTION_FEASIBILITY_AGENT', BOARD_JSON:risk_execution:verdict::STRING, BOARD_JSON:risk_execution:primary_reason_code::STRING, IFF(IS_NULL_VALUE(BOARD_JSON:risk_execution:secondary_reason_code), NULL, BOARD_JSON:risk_execution:secondary_reason_code::STRING), TRY_TO_DOUBLE(BOARD_JSON:risk_execution:confidence::STRING), TRY_TO_DOUBLE(BOARD_JSON:risk_execution:long_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:risk_execution:short_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:risk_execution:no_trade_score::STRING), BOARD_JSON:risk_execution:rationale_text::STRING, OBJECT_INSERT(OBJECT_INSERT(BOARD_JSON:risk_execution, 'mode', 'cortex', TRUE), 'model', :v_model_name, TRUE) FROM TMP_PHASE4_BOARD_RAW
    UNION ALL
    SELECT RUN_ID, DOSSIER_ID, 'CHAIR_PORTFOLIO_PM', BOARD_JSON:chair:final_action::STRING, BOARD_JSON:chair:primary_reason_code::STRING, IFF(IS_NULL_VALUE(BOARD_JSON:chair:secondary_reason_code), NULL, BOARD_JSON:chair:secondary_reason_code::STRING), TRY_TO_DOUBLE(BOARD_JSON:chair:confidence::STRING), TRY_TO_DOUBLE(BOARD_JSON:chair:long_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:chair:short_score::STRING), TRY_TO_DOUBLE(BOARD_JSON:chair:no_trade_score::STRING), COALESCE(BOARD_JSON:chair:rationale_text::STRING, BOARD_JSON:chair:final_thesis::STRING), OBJECT_INSERT(OBJECT_INSERT(BOARD_JSON:chair, 'mode', 'cortex', TRUE), 'model', :v_model_name, TRUE) FROM TMP_PHASE4_BOARD_RAW;

    SELECT COUNT(*) INTO :v_agent_count
    FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2
    WHERE RUN_ID = :v_run_id;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT (
        RUN_ID, DOSSIER_ID, SYMBOL, MARKET_TYPE, FINAL_RANK,
        THESIS_VERDICT, THESIS_DIRECTION, FINAL_ACTION, FINAL_DIRECTION,
        FINAL_THESIS, WHY_NOT_OPPOSITE, WHY_NOT_NO_TRADE,
        PRIMARY_REASON_CODE, SECONDARY_REASON_CODE,
        PROPOSED_TRADE_CONFIG_JSON, RISK_TREATMENT, COMMITTEE_PAYLOAD, CHAIR_OUTPUT_JSON
    )
    WITH chair AS (
        SELECT
            s.RUN_ID, s.DOSSIER_ID, s.SYMBOL, s.MARKET_TYPE, s.DOSSIER_PAYLOAD_JSON, s.PRIMARY_EVIDENCE_SETUP_EVENT_ID, b.BOARD_JSON, b.BOARD_JSON:chair AS CJ,
            ROW_NUMBER() OVER (
                ORDER BY CASE b.BOARD_JSON:chair:final_action::STRING
                    WHEN 'PROPOSE_LONG' THEN 1 WHEN 'PROPOSE_SHORT' THEN 2 WHEN 'WATCH_LONG' THEN 3 WHEN 'WATCH_SHORT' THEN 4 WHEN 'WAIT_FOR_CONFIRMATION' THEN 5 WHEN 'NO_TRADE' THEN 6 ELSE 7 END,
                    TRY_TO_DOUBLE(b.BOARD_JSON:chair:confidence::STRING) DESC, s.SYMBOL
            ) AS RN
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
        JOIN TMP_PHASE4_BOARD_RAW b ON b.RUN_ID = s.RUN_ID AND b.DOSSIER_ID = s.DOSSIER_ID
        WHERE s.RUN_ID = :v_run_id
    )
    SELECT
        RUN_ID, DOSSIER_ID, SYMBOL, MARKET_TYPE, RN,
        BOARD_JSON:thesis:verdict::STRING, CJ:final_direction::STRING, CJ:final_action::STRING, CJ:final_direction::STRING,
        CJ:final_thesis::STRING, CJ:why_not_opposite::STRING, CJ:why_not_no_trade::STRING,
        CJ:primary_reason_code::STRING, IFF(IS_NULL_VALUE(CJ:secondary_reason_code), NULL, CJ:secondary_reason_code::STRING),
        OBJECT_INSERT(OBJECT_INSERT(COALESCE(CJ:proposed_trade_config, OBJECT_CONSTRUCT()), 'direction_source', 'CHAIR_PORTFOLIO_PM.final_direction', TRUE), 'primary_evidence_setup_event_id_role', 'EVIDENCE_ONLY_NOT_DIRECTION_SOURCE', TRUE),
        CJ:risk_treatment::STRING,
        OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(COALESCE(CJ:committee_payload, OBJECT_CONSTRUCT()), 'dossier_payload', DOSSIER_PAYLOAD_JSON, TRUE), 'primary_evidence_setup_event_id', PRIMARY_EVIDENCE_SETUP_EVENT_ID, TRUE), 'primary_evidence_setup_event_id_role', 'EVIDENCE_ONLY_NOT_DIRECTION_SOURCE', TRUE),
        OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(CJ, 'direction_source', 'CHAIR_PORTFOLIO_PM.final_direction', TRUE), 'setup_family_source', 'CHAIR_PORTFOLIO_PM.proposed_trade_config.thesis_label', TRUE), 'agentic_board_cutover', TRUE, TRUE)
    FROM chair;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 (
        RUN_ID, DOSSIER_ID, SYMBOL, MARKET_TYPE, RANK, FINAL_ACTION,
        FINAL_DIRECTION, THESIS_LABEL, SIZING_TREATMENT, FINAL_RATIONALE_SUMMARY,
        DOWNSTREAM_PAYLOAD_POINTER, PUBLICATION_STATUS, PUBLICATION_ERROR_JSON
    )
    SELECT
        v.RUN_ID, v.DOSSIER_ID, v.SYMBOL, v.MARKET_TYPE, v.FINAL_RANK, v.FINAL_ACTION, v.FINAL_DIRECTION,
        v.PROPOSED_TRADE_CONFIG_JSON:thesis_label::STRING,
        COALESCE(v.PROPOSED_TRADE_CONFIG_JSON:size_treatment::STRING, v.RISK_TREATMENT),
        v.FINAL_THESIS,
        'MIP.APP.STRUCTURAL_TRADE_PROPOSALS',
        CASE
            WHEN v.FINAL_ACTION IN ('PROPOSE_LONG', 'PROPOSE_SHORT') AND v.FINAL_RANK <= :P_MAX_PROPOSALS THEN 'PENDING'
            ELSE 'NOT_PUBLISHABLE'
        END,
        CASE
            WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT s.SHORT_LIVE_ENABLED
                THEN OBJECT_CONSTRUCT('reason', 'SHORT_LIVE_DISABLED', 'execution_policy_status', 'POLICY_BLOCKED')
            ELSE NULL
        END
    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT v
    JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s ON s.RUN_ID = v.RUN_ID AND s.DOSSIER_ID = v.DOSSIER_ID
    WHERE v.RUN_ID = :v_run_id;

    INSERT INTO MIP.APP.STRUCTURAL_TRADE_PROPOSALS (
        SETUP_EVENT_ID, PORTFOLIO_ID, SYMBOL, DIRECTION, SETUP_FAMILY,
        ENTRY_ZONE_LOW, ENTRY_ZONE_HIGH, PRICE_INVALIDATION_LEVEL, INVALIDATION_RULE,
        TRAIL_STYLE, TRAIL_PARAMS, EXIT_STYLE, EXIT_PROFILE,
        STRUCTURE_CONFIDENCE, LEVEL_SIGNIFICANCE, REGIME_COMPAT,
        MEANINGFUL_HIT_RATE, PATH_SURVIVAL_HIT_RATE, MFE_MAE_RATIO,
        RISK_CLASS, CONFLICT_RESOLUTION, RATIONALE_TEXT,
        COMMITTEE_PAYLOAD, STATUS,
        BOARD_RUN_ID, BOARD_CANDIDATE_ID, BOARD_DOSSIER_ID, PRIMARY_EVIDENCE_SETUP_EVENT_ID,
        BOARD_FINAL_RANK, BOARD_FINAL_VERDICT, BOARD_PRIMARY_REASON_CODE,
        BOARD_REASON_CODES, BOARD_RATIONALE, BOARD_PAYLOAD_JSON,
        EXECUTION_POLICY_STATUS, EXECUTION_POLICY_REASON, IS_RESEARCH_ONLY
    )
    WITH chair_intent AS (
        -- Resolve chair-emitted exit profile (preferred path) or derive from
        -- trailing_policy.trail_value PCT. Trailing stops are the AGENTIC
        -- default; we only use FIXED_STANDARD when the chair explicitly opts
        -- out. Mirrors orchestrator.py _publish_to_structural so that the
        -- SQL and Python paths produce identical EXIT_PROFILE / TRAIL_PARAMS
        -- shapes consumed by exit_policy.py.
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
         WHERE fs.RUN_ID = :v_run_id
    )
    SELECT
        -- SETUP_EVENT_ID: only write when evidence direction matches proposal direction.
        -- Cross-direction evidence stays in PRIMARY_EVIDENCE_SETUP_EVENT_ID only.
        IFF(ev.DIRECTION = v.FINAL_DIRECTION, s.PRIMARY_EVIDENCE_SETUP_EVENT_ID, NULL),
        s.PORTFOLIO_ID, s.SYMBOL, v.FINAL_DIRECTION,
        v.PTC:thesis_label::STRING,
        TRY_TO_DOUBLE(v.PTC:entry_zone_low::STRING),
        TRY_TO_DOUBLE(v.PTC:entry_zone_high::STRING),
        TRY_TO_DOUBLE(v.PTC:invalidation_level::STRING),
        LEFT(COALESCE(v.PTC:invalidation_rule::STRING, 'AGENTIC_INVALIDATION'), 30),
        -- TRAIL_STYLE: NULL when chair chose FIXED_STANDARD; PCT otherwise.
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
        LEFT(COALESCE(v.PTC:target_policy:exit_style::STRING, v.PTC:target_policy::STRING, 'STAGED_PARTIAL'), 20),
        v.DERIVED_EXIT_PROFILE,
        TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:structure:state_confidence::STRING),
        COALESCE(TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:levels:nearest_resistance:level_significance::STRING), TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:levels:nearest_support:level_significance::STRING)),
        LEFT(COALESCE(s.DOSSIER_PAYLOAD_JSON:regime:tags:trend_regime::STRING, 'AGENTIC'), 10),
        COALESCE(TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:long_history[0]:meaningful_hit_rate::STRING), TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:short_history[0]:meaningful_hit_rate::STRING)),
        COALESCE(TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:long_history[0]:path_survival_hit_rate::STRING), TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:short_history[0]:path_survival_hit_rate::STRING)),
        COALESCE(TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:long_history[0]:mfe_mae_ratio::STRING), TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:short_history[0]:mfe_mae_ratio::STRING)),
        LEFT(COALESCE(v.PTC:risk_class::STRING, 'MEDIUM'), 10),
        NULL,
        LEFT('Phase 4 agentic board rank ' || v.RANK || ' | ' || v.FINAL_ACTION || ' | ' || v.PRIMARY_REASON_CODE || ' | ' || v.FINAL_THESIS, 2000),
        v.COMMITTEE_PAYLOAD,
        'PROPOSED',
        :v_run_id,
        NULL,
        v.DOSSIER_ID,
        s.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
        v.RANK,
        LEFT(v.FINAL_ACTION, 30),
        v.PRIMARY_REASON_CODE,
        ARRAY_CONSTRUCT(v.PRIMARY_REASON_CODE, v.SECONDARY_REASON_CODE),
        v.FINAL_THESIS,
        OBJECT_CONSTRUCT(
            'agentic_board_cutover', TRUE,
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
            'proposed_trade_config', v.PTC
        ),
        -- Execution policy: hard gate persisted at write time.
        -- PROPOSE_SHORT when shorts disabled → POLICY_BLOCKED; never silently dropped.
        CASE
            WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT s.SHORT_LIVE_ENABLED THEN 'POLICY_BLOCKED'
            ELSE 'EXECUTABLE'
        END,
        CASE
            WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT s.SHORT_LIVE_ENABLED THEN 'SHORT_LIVE_DISABLED'
            ELSE NULL
        END,
        IFF(v.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT s.SHORT_LIVE_ENABLED, TRUE, FALSE)
    FROM chair_intent v
    JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
      ON s.RUN_ID = v.RUN_ID AND s.DOSSIER_ID = v.DOSSIER_ID
    -- Left-join evidence event to get direction for SETUP_EVENT_ID guard
    LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS ev
      ON ev.SETUP_EVENT_ID = s.PRIMARY_EVIDENCE_SETUP_EVENT_ID
    WHERE v.PUBLICATION_STATUS = 'PENDING'
      AND v.RANK <= :P_MAX_PROPOSALS
      AND s.PRIMARY_EVIDENCE_SETUP_EVENT_ID IS NOT NULL
      AND v.PTC:thesis_label::STRING ILIKE 'AGENTIC_%'
      -- Allow PROPOSE_SHORT unconditionally; policy is recorded in EXECUTION_POLICY_STATUS
      AND v.FINAL_ACTION IN ('PROPOSE_LONG', 'PROPOSE_SHORT')
      AND NOT EXISTS (
          SELECT 1 FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
          WHERE p.STATUS = 'PROPOSED' AND p.SYMBOL = s.SYMBOL
            AND (:P_PORTFOLIO_ID IS NULL OR p.PORTFOLIO_ID = :P_PORTFOLIO_ID OR p.PORTFOLIO_ID IS NULL)
      )
      AND NOT EXISTS (
          SELECT 1 FROM MIP.LIVE.LIVE_ACTIONS la
          WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL' AND la.SYMBOL = s.SYMBOL
            AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION','OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')
            AND (:P_PORTFOLIO_ID IS NULL OR la.PORTFOLIO_ID = :P_PORTFOLIO_ID OR la.PORTFOLIO_ID IS NULL)
      );

    UPDATE MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
       SET PUBLICATION_STATUS = 'PUBLISHED',
           PUBLISHED_PROPOSAL_ID = p.PROPOSAL_ID,
           PUBLISHED_AT = p.CREATED_AT
      FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
     WHERE fs.RUN_ID = :v_run_id
       AND p.BOARD_RUN_ID = fs.RUN_ID
       AND p.BOARD_DOSSIER_ID = fs.DOSSIER_ID
       AND fs.PUBLICATION_STATUS = 'PENDING';

    UPDATE MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2
       SET PUBLICATION_STATUS = 'SKIPPED_GUARDRAIL',
           PUBLICATION_ERROR_JSON = OBJECT_CONSTRUCT('reason', 'not_inserted_duplicate_collision_missing_evidence_or_policy')
     WHERE RUN_ID = :v_run_id AND PUBLICATION_STATUS = 'PENDING';

    -- ── Phase 3: Post-INSERT geometry validation ──────────────────────────
    -- Runs immediately after materialization. Marks GEOMETRY_INVALID for any
    -- proposal with geometrically incoherent invalidation levels.
    -- LONG: invalidation must be strictly BELOW entry zone low.
    -- SHORT: invalidation must be strictly ABOVE entry zone high.
    -- These proposals are blocked at LPA/API (hard gate) and visible in
    -- diagnostics but not silently dropped.
    UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
       SET EXECUTION_POLICY_STATUS = 'GEOMETRY_INVALID',
           EXECUTION_POLICY_REASON = CASE
               WHEN p.DIRECTION = 'LONG'  THEN 'INVALID_LONG_GEOMETRY'
               WHEN p.DIRECTION = 'SHORT' THEN 'INVALID_SHORT_GEOMETRY'
               ELSE 'INVALID_LONG_GEOMETRY'
           END,
           IS_RESEARCH_ONLY = TRUE
     WHERE p.BOARD_RUN_ID = :v_run_id
       AND p.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
       AND (
           -- LONG: invalidation must be below entry zone low
           (p.DIRECTION = 'LONG'
            AND p.PRICE_INVALIDATION_LEVEL IS NOT NULL
            AND p.ENTRY_ZONE_LOW IS NOT NULL
            AND p.PRICE_INVALIDATION_LEVEL >= p.ENTRY_ZONE_LOW)
           OR
           -- SHORT: invalidation must be above entry zone high
           (p.DIRECTION = 'SHORT'
            AND p.PRICE_INVALIDATION_LEVEL IS NOT NULL
            AND p.ENTRY_ZONE_HIGH IS NOT NULL
            AND p.PRICE_INVALIDATION_LEVEL <= p.ENTRY_ZONE_HIGH)
           OR
           -- Entry zone must be internally consistent
           (p.ENTRY_ZONE_LOW IS NOT NULL AND p.ENTRY_ZONE_HIGH IS NOT NULL
            AND p.ENTRY_ZONE_LOW > p.ENTRY_ZONE_HIGH)
       );

    -- Cross-direction evidence: mark non-executable when SETUP_EVENT_ID is NULL
    -- and the evidence direction differs from proposal direction.
    -- These proposals survive for research but cannot be imported to LPA.
    UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
       SET EXECUTION_POLICY_STATUS = 'POLICY_BLOCKED',
           EXECUTION_POLICY_REASON = 'SETUP_EVENT_DIRECTION_MISMATCH',
           IS_RESEARCH_ONLY = TRUE
     WHERE p.BOARD_RUN_ID = :v_run_id
       AND p.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
       AND p.SETUP_EVENT_ID IS NULL
       AND p.PRIMARY_EVIDENCE_SETUP_EVENT_ID IS NOT NULL;

    SELECT COUNT(*) INTO :v_published_count
    FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2
    WHERE RUN_ID = :v_run_id AND PUBLICATION_STATUS = 'PUBLISHED';

    SELECT COUNT(*) INTO :v_final_count
    FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2
    WHERE RUN_ID = :v_run_id;

    INSERT INTO MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT (
        PROPOSAL_ID, PROPOSAL_TS, SYMBOL, SIDE, SETUP_FAMILY,
        STRUCTURAL_STATE, REGIME_STATE, TRUST_LABEL,
        ENTRY_ZONE_JSON, INVALIDATION_JSON, PATH_METRICS_JSON, MFE_MAE_JSON,
        TRAILING_STYLE, PROPOSAL_SUMMARY_JSON
    )
    SELECT
        p.PROPOSAL_ID, p.CREATED_AT, p.SYMBOL, p.DIRECTION, p.SETUP_FAMILY,
        -- Phase 4 agentic proposals embed the dossier in BOARD_PAYLOAD_JSON.
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
        OBJECT_CONSTRUCT('meaningful_hit_rate', p.MEANINGFUL_HIT_RATE, 'path_survival_hit_rate', p.PATH_SURVIVAL_HIT_RATE, 'mfe_mae_ratio', p.MFE_MAE_RATIO, 'primary_evidence_setup_event_id', p.PRIMARY_EVIDENCE_SETUP_EVENT_ID, 'primary_evidence_setup_event_id_role', 'EVIDENCE_ONLY_NOT_DIRECTION_SOURCE'),
        OBJECT_CONSTRUCT('mfe_mae_ratio', p.MFE_MAE_RATIO, 'meaningful_hit_rate', p.MEANINGFUL_HIT_RATE, 'path_survival_hit_rate', p.PATH_SURVIVAL_HIT_RATE),
        p.TRAIL_STYLE,
        p.COMMITTEE_PAYLOAD
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
    WHERE p.BOARD_RUN_ID = :v_run_id
      AND NOT EXISTS (SELECT 1 FROM MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT s WHERE s.PROPOSAL_ID = p.PROPOSAL_ID);

    UPDATE MIP.APP.PROPOSAL_BOARD_RUN
       SET RUN_STATUS = 'COMPLETE',
           FINAL_PROPOSAL_COUNT = :v_published_count,
           FINISHED_AT = CURRENT_TIMESTAMP()
     WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'as_of_date', :v_as_of,
        'symbol_dossier_count', :v_dossier_count,
        'agent_outcome_count', :v_agent_count,
        'final_slate_count', :v_final_count,
        'published_count', :v_published_count,
        'elapsed_sec', DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE MIP.APP.PROPOSAL_BOARD_RUN
           SET RUN_STATUS = 'FAILED',
               FINISHED_AT = CURRENT_TIMESTAMP(),
               ERROR_JSON = OBJECT_CONSTRUCT('sqlcode', :SQLCODE, 'sqlerrm', :SQLERRM, 'sqlstate', :SQLSTATE)
         WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT('status', 'FAILED', 'run_id', :v_run_id, 'sqlcode', :SQLCODE, 'sqlerrm', :SQLERRM, 'sqlstate', :SQLSTATE);
END;
$$;

GRANT USAGE ON PROCEDURE MIP.APP.SP_RUN_PROPOSAL_BOARD(NUMBER, INTEGER, DATE, INTEGER) TO ROLE MIP_ADMIN_ROLE;

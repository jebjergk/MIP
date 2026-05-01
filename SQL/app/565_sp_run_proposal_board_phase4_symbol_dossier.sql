/* ================================================================
   565_sp_run_proposal_board_phase4_symbol_dossier.sql
   Phase 4 active Agentic Proposal Board.

   Active cutover implementation:
   - symbol-dossier grain, not deterministic candidate grain
   - direction authored only by CHAIR_PORTFOLIO_PM.final_direction
   - setup events are evidence-only
   - no deterministic fallback; invalid Cortex output fails closed
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
        'proposal_board_v3_phase4_symbol_dossier',
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
      AND ARRAY_SIZE(COALESCE(DATA_QUALITY_FLAGS, ARRAY_CONSTRUCT())) = 0;

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

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'reason_code', 'NO_SYMBOL_DOSSIERS'
        );
    END IF;

    CREATE OR REPLACE TEMPORARY TABLE TMP_PHASE4_AGENT_RAW (
        RUN_ID VARCHAR,
        DOSSIER_ID NUMBER,
        AGENT_NAME VARCHAR,
        PROMPT_TEXT VARCHAR,
        RAW_TEXT VARCHAR,
        CLEANED_TEXT VARCHAR,
        PARSED_JSON VARIANT
    );

    INSERT INTO TMP_PHASE4_AGENT_RAW
    WITH prompted AS (
        SELECT
            RUN_ID,
            DOSSIER_ID,
            'MARKET_STRUCTURE_AGENT' AS AGENT_NAME,
            'You are the Market Structure Agent on the active MIP Phase 4 Agentic Proposal Board. '
            || 'You receive a symbol-level evidence dossier. No proposed direction is given. '
            || 'Decide the actual market structure from facts only. '
            || 'Return ONLY JSON with keys: verdict, primary_reason_code, secondary_reason_code, confidence, rationale_text, long_score, short_score, no_trade_score, supporting_evidence, opposing_evidence. '
            || 'Allowed verdict: TREND_UP, TREND_DOWN, RANGE, BREAKOUT_ATTEMPT, FAILED_BREAKOUT, RESISTANCE_REJECTION, SUPPORT_BOUNCE, EXHAUSTION, REVERSAL_FORMING, CHOP_NO_EDGE. '
            || 'Allowed primary_reason_code: STRUCTURE_TREND_UP, STRUCTURE_TREND_DOWN, STRUCTURE_RANGE, STRUCTURE_CHOP_NO_EDGE, STRUCTURE_REVERSAL_FORMING, STRUCTURE_FAILED_BREAKOUT. '
            || 'Dossier JSON: ' || TO_JSON(DOSSIER_PAYLOAD_JSON) AS PROMPT_TEXT
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT
        WHERE RUN_ID = :v_run_id
    ),
    cortexed AS (
        SELECT
            p.*,
            SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            c.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(c.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT
        FROM cortexed c
    )
    SELECT
        RUN_ID,
        DOSSIER_ID,
        AGENT_NAME,
        PROMPT_TEXT,
        RAW_TEXT,
        CLEANED_TEXT,
        TRY_PARSE_JSON(CLEANED_TEXT) AS PARSED_JSON
    FROM cleaned;

    INSERT INTO TMP_PHASE4_AGENT_RAW
    WITH ctx AS (
        SELECT
            s.RUN_ID,
            s.DOSSIER_ID,
            s.DOSSIER_PAYLOAD_JSON,
            (SELECT ARRAY_AGG(STRUCTURED_OUTPUT_JSON) FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 o WHERE o.RUN_ID = s.RUN_ID AND o.DOSSIER_ID = s.DOSSIER_ID) AS PRIOR_OUTPUTS
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
        WHERE s.RUN_ID = :v_run_id
    ),
    prompted AS (
        SELECT
            RUN_ID,
            DOSSIER_ID,
            'LEVEL_PRICE_ACTION_AGENT' AS AGENT_NAME,
            'You are the Level / Price Action Agent on the active MIP Phase 4 Agentic Proposal Board. '
            || 'Judge support, resistance, breakout/retest quality, rejection quality, extension/chase risk, long location quality, and short location quality. '
            || 'No proposed direction is given. Return ONLY JSON with keys: verdict, primary_reason_code, secondary_reason_code, confidence, rationale_text, long_score, short_score, no_trade_score, watch_triggers. '
            || 'Allowed verdict: LONG_LOCATION, SHORT_LOCATION, BOTH_SIDES, WAIT_CONFIRMATION, NO_EDGE. '
            || 'Allowed primary_reason_code: LEVEL_LONG_LOCATION, LEVEL_SHORT_LOCATION, LEVEL_WAIT_CONFIRMATION, LEVEL_NO_EDGE. '
            || 'Dossier JSON: ' || TO_JSON(DOSSIER_PAYLOAD_JSON) || ' Prior outputs: ' || COALESCE(TO_JSON(PRIOR_OUTPUTS), '[]') AS PROMPT_TEXT
        FROM ctx
    ),
    cortexed AS (
        SELECT
            p.*,
            SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            c.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(c.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT
        FROM cortexed c
    )
    SELECT
        RUN_ID,
        DOSSIER_ID,
        AGENT_NAME,
        PROMPT_TEXT,
        RAW_TEXT,
        CLEANED_TEXT,
        TRY_PARSE_JSON(CLEANED_TEXT) AS PARSED_JSON
    FROM cleaned;

    INSERT INTO TMP_PHASE4_AGENT_RAW
    WITH ctx AS (
        SELECT
            s.RUN_ID,
            s.DOSSIER_ID,
            s.DOSSIER_PAYLOAD_JSON,
            (SELECT ARRAY_AGG(STRUCTURED_OUTPUT_JSON) FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 o WHERE o.RUN_ID = s.RUN_ID AND o.DOSSIER_ID = s.DOSSIER_ID) AS PRIOR_OUTPUTS
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
        WHERE s.RUN_ID = :v_run_id
    ),
    prompted AS (
        SELECT
            RUN_ID,
            DOSSIER_ID,
            'THESIS_AGENT' AS AGENT_NAME,
            'You are the Thesis Agent on the active MIP Phase 4 Agentic Proposal Board. '
            || 'Form a fresh thesis from the symbol dossier and prior agent outputs. Direction must be created by agents, not inherited from setup events. '
            || 'Return ONLY JSON with keys: verdict, primary_reason_code, secondary_reason_code, confidence, rationale_text, thesis_text, why_long, why_short, why_no_trade, opposing_evidence, needed_confirmation, long_score, short_score, no_trade_score. '
            || 'Allowed verdict: LONG_THESIS, SHORT_THESIS, WATCH_LONG, WATCH_SHORT, NO_TRADE, CONFLICTED. '
            || 'Allowed primary_reason_code: THESIS_LONG, THESIS_SHORT, THESIS_WATCH_LONG, THESIS_WATCH_SHORT, THESIS_NO_TRADE, THESIS_CONFLICTED. '
            || 'Dossier JSON: ' || TO_JSON(DOSSIER_PAYLOAD_JSON) || ' Prior outputs: ' || COALESCE(TO_JSON(PRIOR_OUTPUTS), '[]') AS PROMPT_TEXT
        FROM ctx
    ),
    cortexed AS (
        SELECT
            p.*,
            SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            c.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(c.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT
        FROM cortexed c
    )
    SELECT
        RUN_ID,
        DOSSIER_ID,
        AGENT_NAME,
        PROMPT_TEXT,
        RAW_TEXT,
        CLEANED_TEXT,
        TRY_PARSE_JSON(CLEANED_TEXT) AS PARSED_JSON
    FROM cleaned;

    INSERT INTO TMP_PHASE4_AGENT_RAW
    WITH ctx AS (
        SELECT
            s.RUN_ID,
            s.DOSSIER_ID,
            s.DOSSIER_PAYLOAD_JSON,
            (SELECT ARRAY_AGG(STRUCTURED_OUTPUT_JSON) FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 o WHERE o.RUN_ID = s.RUN_ID AND o.DOSSIER_ID = s.DOSSIER_ID) AS PRIOR_OUTPUTS
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
        WHERE s.RUN_ID = :v_run_id
    ),
    prompted AS (
        SELECT
            RUN_ID,
            DOSSIER_ID,
            'HISTORICAL_EVIDENCE_AGENT' AS AGENT_NAME,
            'You are the Historical Evidence Agent on the active MIP Phase 4 Agentic Proposal Board. '
            || 'Evaluate long history grade, short history grade, similar outcomes, failure modes, re-pitch concerns, and path stats. '
            || 'Do not suppress short evidence when shorts are not live-enabled. Return ONLY JSON with keys: verdict, primary_reason_code, secondary_reason_code, confidence, rationale_text, long_score, short_score, no_trade_score, similar_outcomes, failure_modes, repitch_concerns. '
            || 'Allowed verdict: LONG_SUPPORTIVE, SHORT_SUPPORTIVE, MIXED_DIRECTIONAL, WEAK_BOTH_SIDES. '
            || 'Allowed primary_reason_code: HISTORY_LONG_SUPPORTIVE, HISTORY_SHORT_SUPPORTIVE, HISTORY_MIXED_DIRECTIONAL, HISTORY_WEAK_BOTH_SIDES. '
            || 'Dossier JSON: ' || TO_JSON(DOSSIER_PAYLOAD_JSON) || ' Prior outputs: ' || COALESCE(TO_JSON(PRIOR_OUTPUTS), '[]') AS PROMPT_TEXT
        FROM ctx
    ),
    cortexed AS (
        SELECT
            p.*,
            SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            c.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(c.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT
        FROM cortexed c
    )
    SELECT
        RUN_ID,
        DOSSIER_ID,
        AGENT_NAME,
        PROMPT_TEXT,
        RAW_TEXT,
        CLEANED_TEXT,
        TRY_PARSE_JSON(CLEANED_TEXT) AS PARSED_JSON
    FROM cleaned;

    INSERT INTO TMP_PHASE4_AGENT_RAW
    WITH ctx AS (
        SELECT
            s.RUN_ID,
            s.DOSSIER_ID,
            s.DOSSIER_PAYLOAD_JSON,
            (SELECT ARRAY_AGG(STRUCTURED_OUTPUT_JSON) FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 o WHERE o.RUN_ID = s.RUN_ID AND o.DOSSIER_ID = s.DOSSIER_ID) AS PRIOR_OUTPUTS
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
        WHERE s.RUN_ID = :v_run_id
    ),
    prompted AS (
        SELECT
            RUN_ID,
            DOSSIER_ID,
            'RISK_EXECUTION_FEASIBILITY_AGENT' AS AGENT_NAME,
            'You are the Risk / Execution Feasibility Agent on the active MIP Phase 4 Agentic Proposal Board. '
            || 'Evaluate the agentic thesis, not a pre-labelled setup. If thesis is short and short_live_enabled=false, return RESEARCH_ONLY and SHORT_LIVE_DISABLED or RISK_RESEARCH_ONLY. '
            || 'Return ONLY JSON with keys: verdict, primary_reason_code, secondary_reason_code, confidence, rationale_text, long_score, short_score, no_trade_score, live_direction_allowed, sizing_treatment, risk_treatment. '
            || 'Allowed verdict: ACTIONABLE, RESEARCH_ONLY, WAIT_CONFIRMATION, NO_TRADE, HARD_BLOCK. '
            || 'Allowed primary_reason_code: RISK_ACTIONABLE, RISK_RESEARCH_ONLY, RISK_WAIT_CONFIRMATION, RISK_NO_TRADE, RISK_HARD_BLOCK, SHORT_LIVE_DISABLED. '
            || 'Dossier JSON: ' || TO_JSON(DOSSIER_PAYLOAD_JSON) || ' Prior outputs: ' || COALESCE(TO_JSON(PRIOR_OUTPUTS), '[]') AS PROMPT_TEXT
        FROM ctx
    ),
    cortexed AS (
        SELECT
            p.*,
            SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            c.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(c.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT
        FROM cortexed c
    )
    SELECT
        RUN_ID,
        DOSSIER_ID,
        AGENT_NAME,
        PROMPT_TEXT,
        RAW_TEXT,
        CLEANED_TEXT,
        TRY_PARSE_JSON(CLEANED_TEXT) AS PARSED_JSON
    FROM cleaned;

    INSERT INTO TMP_PHASE4_AGENT_RAW
    WITH ctx AS (
        SELECT
            s.RUN_ID,
            s.DOSSIER_ID,
            s.DOSSIER_PAYLOAD_JSON,
            (SELECT ARRAY_AGG(STRUCTURED_OUTPUT_JSON) FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 o WHERE o.RUN_ID = s.RUN_ID AND o.DOSSIER_ID = s.DOSSIER_ID) AS PRIOR_OUTPUTS
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
        WHERE s.RUN_ID = :v_run_id
    ),
    prompted AS (
        SELECT
            RUN_ID,
            DOSSIER_ID,
            'CHAIR_PORTFOLIO_PM' AS AGENT_NAME,
            'You are the Chair / Portfolio PM on the active MIP Phase 4 Agentic Proposal Board. '
            || 'You make the final decision from the symbol dossier and prior agent outputs. Direction must come only from your final_direction. '
            || 'Use only AGENTIC_* thesis labels, never copy old deterministic setup families as the final setup_family. '
            || 'If final_action is PROPOSE_SHORT while short_live_enabled=false, convert to WATCH_SHORT or REJECT; executable short publication is not allowed. '
            || 'Return ONLY JSON with keys: final_action, final_direction, primary_reason_code, secondary_reason_code, confidence, final_thesis, why_not_opposite, why_not_no_trade, risk_treatment, rationale_text, proposed_trade_config, committee_payload, long_score, short_score, no_trade_score. '
            || 'Allowed final_action: PROPOSE_LONG, PROPOSE_SHORT, WATCH_LONG, WATCH_SHORT, NO_TRADE, REJECT, WAIT_FOR_CONFIRMATION. '
            || 'Allowed final_direction: LONG, SHORT, NONE. '
            || 'Allowed primary_reason_code: CHAIR_PROPOSE_LONG, CHAIR_PROPOSE_SHORT, CHAIR_WATCH_LONG, CHAIR_WATCH_SHORT, CHAIR_NO_TRADE, CHAIR_REJECT, CHAIR_WAIT_FOR_CONFIRMATION, SHORT_RESEARCH_ONLY. '
            || 'proposed_trade_config must include thesis_label starting with AGENTIC_, entry_zone_low, entry_zone_high, invalidation_level, invalidation_rule, target_policy, trailing_policy, size_treatment, time_horizon, primary_evidence_setup_event_id. '
            || 'Dossier JSON: ' || TO_JSON(DOSSIER_PAYLOAD_JSON) || ' Prior outputs: ' || COALESCE(TO_JSON(PRIOR_OUTPUTS), '[]') AS PROMPT_TEXT
        FROM ctx
    ),
    cortexed AS (
        SELECT
            p.*,
            SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            c.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(c.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT
        FROM cortexed c
    )
    SELECT
        RUN_ID,
        DOSSIER_ID,
        AGENT_NAME,
        PROMPT_TEXT,
        RAW_TEXT,
        CLEANED_TEXT,
        TRY_PARSE_JSON(CLEANED_TEXT) AS PARSED_JSON
    FROM cleaned;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, ERROR_TYPE, RAW_OUTPUT_JSON, ERROR_MESSAGE
    )
    SELECT
        r.RUN_ID,
        r.DOSSIER_ID,
        r.AGENT_NAME,
        'AGENT_OUTPUT_INVALID',
        OBJECT_CONSTRUCT(
            'prompt_text', r.PROMPT_TEXT,
            'raw_text', r.RAW_TEXT,
            'cleaned_text', r.CLEANED_TEXT,
            'parsed_json', r.PARSED_JSON
        ),
        'Phase 4 symbol-dossier board requires valid JSON with allowed enum and reason code; deterministic fallback is forbidden.'
    FROM TMP_PHASE4_AGENT_RAW r
    WHERE NOT (
        r.PARSED_JSON IS NOT NULL
        AND NOT IS_NULL_VALUE(r.PARSED_JSON:confidence)
        AND TRY_TO_DOUBLE(r.PARSED_JSON:confidence::STRING) BETWEEN 0.0 AND 1.0
        AND (
            (r.AGENT_NAME = 'MARKET_STRUCTURE_AGENT'
             AND r.PARSED_JSON:verdict::STRING IN ('TREND_UP','TREND_DOWN','RANGE','BREAKOUT_ATTEMPT','FAILED_BREAKOUT','RESISTANCE_REJECTION','SUPPORT_BOUNCE','EXHAUSTION','REVERSAL_FORMING','CHOP_NO_EDGE')
             AND r.PARSED_JSON:primary_reason_code::STRING IN ('STRUCTURE_TREND_UP','STRUCTURE_TREND_DOWN','STRUCTURE_RANGE','STRUCTURE_CHOP_NO_EDGE','STRUCTURE_REVERSAL_FORMING','STRUCTURE_FAILED_BREAKOUT'))
            OR (r.AGENT_NAME = 'LEVEL_PRICE_ACTION_AGENT'
             AND r.PARSED_JSON:verdict::STRING IN ('LONG_LOCATION','SHORT_LOCATION','BOTH_SIDES','WAIT_CONFIRMATION','NO_EDGE')
             AND r.PARSED_JSON:primary_reason_code::STRING IN ('LEVEL_LONG_LOCATION','LEVEL_SHORT_LOCATION','LEVEL_WAIT_CONFIRMATION','LEVEL_NO_EDGE'))
            OR (r.AGENT_NAME = 'THESIS_AGENT'
             AND r.PARSED_JSON:verdict::STRING IN ('LONG_THESIS','SHORT_THESIS','WATCH_LONG','WATCH_SHORT','NO_TRADE','CONFLICTED')
             AND r.PARSED_JSON:primary_reason_code::STRING IN ('THESIS_LONG','THESIS_SHORT','THESIS_WATCH_LONG','THESIS_WATCH_SHORT','THESIS_NO_TRADE','THESIS_CONFLICTED'))
            OR (r.AGENT_NAME = 'HISTORICAL_EVIDENCE_AGENT'
             AND r.PARSED_JSON:verdict::STRING IN ('LONG_SUPPORTIVE','SHORT_SUPPORTIVE','MIXED_DIRECTIONAL','WEAK_BOTH_SIDES')
             AND r.PARSED_JSON:primary_reason_code::STRING IN ('HISTORY_LONG_SUPPORTIVE','HISTORY_SHORT_SUPPORTIVE','HISTORY_MIXED_DIRECTIONAL','HISTORY_WEAK_BOTH_SIDES'))
            OR (r.AGENT_NAME = 'RISK_EXECUTION_FEASIBILITY_AGENT'
             AND r.PARSED_JSON:verdict::STRING IN ('ACTIONABLE','RESEARCH_ONLY','WAIT_CONFIRMATION','NO_TRADE','HARD_BLOCK')
             AND r.PARSED_JSON:primary_reason_code::STRING IN ('RISK_ACTIONABLE','RISK_RESEARCH_ONLY','RISK_WAIT_CONFIRMATION','RISK_NO_TRADE','RISK_HARD_BLOCK','SHORT_LIVE_DISABLED'))
            OR (r.AGENT_NAME = 'CHAIR_PORTFOLIO_PM'
             AND r.PARSED_JSON:final_action::STRING IN ('PROPOSE_LONG','PROPOSE_SHORT','WATCH_LONG','WATCH_SHORT','NO_TRADE','REJECT','WAIT_FOR_CONFIRMATION')
             AND r.PARSED_JSON:final_direction::STRING IN ('LONG','SHORT','NONE')
             AND r.PARSED_JSON:primary_reason_code::STRING IN ('CHAIR_PROPOSE_LONG','CHAIR_PROPOSE_SHORT','CHAIR_WATCH_LONG','CHAIR_WATCH_SHORT','CHAIR_NO_TRADE','CHAIR_REJECT','CHAIR_WAIT_FOR_CONFIRMATION','SHORT_RESEARCH_ONLY')
             AND r.PARSED_JSON:proposed_trade_config:thesis_label::STRING ILIKE 'AGENTIC_%')
        )
    );

    SELECT COUNT(*) INTO :v_invalid_count
    FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR
    WHERE RUN_ID = :v_run_id;

    IF (v_invalid_count > 0) THEN
        UPDATE MIP.APP.PROPOSAL_BOARD_RUN
           SET RUN_STATUS = 'FAILED',
               FINISHED_AT = CURRENT_TIMESTAMP(),
               ERROR_JSON = OBJECT_CONSTRUCT(
                   'message', 'AGENT_OUTPUT_INVALID_FAIL_CLOSED',
                   'invalid_count', :v_invalid_count,
                   'reason_code', 'AGENT_OUTPUT_INVALID'
               )
         WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'invalid_count', :v_invalid_count,
            'reason_code', 'AGENT_OUTPUT_INVALID'
        );
    END IF;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 (
        RUN_ID, DOSSIER_ID, AGENT_NAME, VERDICT, PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE, CONFIDENCE, LONG_SCORE, SHORT_SCORE,
        NO_TRADE_SCORE, RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
    )
    SELECT
        RUN_ID,
        DOSSIER_ID,
        AGENT_NAME,
        IFF(AGENT_NAME = 'CHAIR_PORTFOLIO_PM', PARSED_JSON:final_action::STRING, PARSED_JSON:verdict::STRING),
        PARSED_JSON:primary_reason_code::STRING,
        IFF(IS_NULL_VALUE(PARSED_JSON:secondary_reason_code), NULL, PARSED_JSON:secondary_reason_code::STRING),
        TRY_TO_DOUBLE(PARSED_JSON:confidence::STRING),
        TRY_TO_DOUBLE(PARSED_JSON:long_score::STRING),
        TRY_TO_DOUBLE(PARSED_JSON:short_score::STRING),
        TRY_TO_DOUBLE(PARSED_JSON:no_trade_score::STRING),
        COALESCE(PARSED_JSON:rationale_text::STRING, PARSED_JSON:final_thesis::STRING),
        OBJECT_INSERT(
            OBJECT_INSERT(
                OBJECT_INSERT(
                    OBJECT_INSERT(PARSED_JSON, 'mode', 'cortex', TRUE),
                    'model', :v_model_name, TRUE
                ),
                'cortex_raw_text', RAW_TEXT, TRUE
            ),
            'prompt_text', PROMPT_TEXT, TRUE
        )
    FROM TMP_PHASE4_AGENT_RAW;

    SELECT COUNT(*) INTO :v_agent_count
    FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2
    WHERE RUN_ID = :v_run_id;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_INTERACTION_V2 (
        RUN_ID, DOSSIER_ID, SOURCE_AGENT, TARGET_AGENT, TOPIC,
        DISAGREEMENT_TYPE, DISAGREEMENT_TEXT, RESPONSE_TEXT, RESOLVED_FLAG
    )
    SELECT
        s.RUN_ID,
        s.DOSSIER_ID,
        'THESIS_AGENT',
        'CHAIR_PORTFOLIO_PM',
        'LONG_SHORT_NO_TRADE_TENSION',
        'MIXED_DIRECTIONAL_EVIDENCE',
        'Both long and short evidence were visible in the symbol dossier.',
        'Chair final action: ' || c.STRUCTURED_OUTPUT_JSON:final_action::STRING,
        TRUE
    FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
    JOIN MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 c
      ON c.RUN_ID = s.RUN_ID
     AND c.DOSSIER_ID = s.DOSSIER_ID
     AND c.AGENT_NAME = 'CHAIR_PORTFOLIO_PM'
    WHERE s.RUN_ID = :v_run_id
      AND ARRAY_CONTAINS('BOTH_LONG_AND_SHORT_EVIDENCE_VISIBLE'::VARIANT, s.BOARD_WARNING_FLAGS);

    INSERT INTO MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT (
        RUN_ID, DOSSIER_ID, SYMBOL, MARKET_TYPE, FINAL_RANK,
        THESIS_VERDICT, THESIS_DIRECTION, FINAL_ACTION, FINAL_DIRECTION,
        FINAL_THESIS, WHY_NOT_OPPOSITE, WHY_NOT_NO_TRADE,
        PRIMARY_REASON_CODE, SECONDARY_REASON_CODE,
        PROPOSED_TRADE_CONFIG_JSON, RISK_TREATMENT, COMMITTEE_PAYLOAD, CHAIR_OUTPUT_JSON
    )
    WITH chair AS (
        SELECT
            s.RUN_ID,
            s.DOSSIER_ID,
            s.SYMBOL,
            s.MARKET_TYPE,
            s.DOSSIER_PAYLOAD_JSON,
            s.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
            s.SHORT_LIVE_ENABLED,
            c.STRUCTURED_OUTPUT_JSON AS CJ,
            c.PRIMARY_REASON_CODE,
            c.SECONDARY_REASON_CODE,
            c.CONFIDENCE,
            ROW_NUMBER() OVER (
                ORDER BY
                    CASE c.STRUCTURED_OUTPUT_JSON:final_action::STRING
                        WHEN 'PROPOSE_LONG' THEN 1
                        WHEN 'PROPOSE_SHORT' THEN 2
                        WHEN 'WATCH_LONG' THEN 3
                        WHEN 'WATCH_SHORT' THEN 4
                        WHEN 'WAIT_FOR_CONFIRMATION' THEN 5
                        WHEN 'NO_TRADE' THEN 6
                        ELSE 7
                    END,
                    c.CONFIDENCE DESC,
                    s.SYMBOL
            ) AS RN
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
        JOIN MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 c
          ON c.RUN_ID = s.RUN_ID
         AND c.DOSSIER_ID = s.DOSSIER_ID
         AND c.AGENT_NAME = 'CHAIR_PORTFOLIO_PM'
        WHERE s.RUN_ID = :v_run_id
    )
    SELECT
        RUN_ID,
        DOSSIER_ID,
        SYMBOL,
        MARKET_TYPE,
        RN,
        CJ:final_action::STRING,
        CJ:final_direction::STRING,
        CJ:final_action::STRING,
        CJ:final_direction::STRING,
        CJ:final_thesis::STRING,
        CJ:why_not_opposite::STRING,
        CJ:why_not_no_trade::STRING,
        PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE,
        OBJECT_INSERT(
            OBJECT_INSERT(
                COALESCE(CJ:proposed_trade_config, OBJECT_CONSTRUCT()),
                'direction_source', 'CHAIR_PORTFOLIO_PM.final_direction', TRUE
            ),
            'primary_evidence_setup_event_id_role', 'EVIDENCE_ONLY_NOT_DIRECTION_SOURCE', TRUE
        ),
        COALESCE(CJ:risk_treatment::STRING, CJ:proposed_trade_config:size_treatment::STRING),
        OBJECT_INSERT(
            OBJECT_INSERT(
                OBJECT_INSERT(
                    COALESCE(CJ:committee_payload, OBJECT_CONSTRUCT()),
                    'dossier_payload', DOSSIER_PAYLOAD_JSON, TRUE
                ),
                'primary_evidence_setup_event_id', PRIMARY_EVIDENCE_SETUP_EVENT_ID, TRUE
            ),
            'primary_evidence_setup_event_id_role', 'EVIDENCE_ONLY_NOT_DIRECTION_SOURCE', TRUE
        ),
        OBJECT_INSERT(
            OBJECT_INSERT(
                OBJECT_INSERT(CJ, 'direction_source', 'CHAIR_PORTFOLIO_PM.final_direction', TRUE),
                'setup_family_source', 'CHAIR_PORTFOLIO_PM.proposed_trade_config.thesis_label', TRUE
            ),
            'agentic_board_cutover', TRUE, TRUE
        )
    FROM chair;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 (
        RUN_ID, DOSSIER_ID, SYMBOL, MARKET_TYPE, RANK, FINAL_ACTION,
        FINAL_DIRECTION, THESIS_LABEL, SIZING_TREATMENT, FINAL_RATIONALE_SUMMARY,
        DOWNSTREAM_PAYLOAD_POINTER, PUBLICATION_STATUS, PUBLICATION_ERROR_JSON
    )
    SELECT
        v.RUN_ID,
        v.DOSSIER_ID,
        v.SYMBOL,
        v.MARKET_TYPE,
        v.FINAL_RANK,
        v.FINAL_ACTION,
        v.FINAL_DIRECTION,
        v.PROPOSED_TRADE_CONFIG_JSON:thesis_label::STRING,
        COALESCE(v.PROPOSED_TRADE_CONFIG_JSON:size_treatment::STRING, v.RISK_TREATMENT),
        v.FINAL_THESIS,
        'MIP.APP.STRUCTURAL_TRADE_PROPOSALS',
        CASE
            WHEN v.FINAL_ACTION = 'PROPOSE_LONG' AND v.FINAL_RANK <= :P_MAX_PROPOSALS THEN 'PENDING'
            WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND s.SHORT_LIVE_ENABLED AND v.FINAL_RANK <= :P_MAX_PROPOSALS THEN 'PENDING'
            WHEN v.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT s.SHORT_LIVE_ENABLED THEN 'SHORT_RESEARCH_ONLY'
            ELSE 'NOT_PUBLISHABLE'
        END,
        IFF(v.FINAL_ACTION = 'PROPOSE_SHORT' AND NOT s.SHORT_LIVE_ENABLED,
            OBJECT_CONSTRUCT('reason', 'SHORT_LIVE_ENABLED_FALSE', 'retained_as', 'SHORT_RESEARCH_ONLY'),
            NULL)
    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT v
    JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
      ON s.RUN_ID = v.RUN_ID
     AND s.DOSSIER_ID = v.DOSSIER_ID
    WHERE v.RUN_ID = :v_run_id;

    INSERT INTO MIP.APP.STRUCTURAL_TRADE_PROPOSALS (
        SETUP_EVENT_ID, PORTFOLIO_ID, SYMBOL, DIRECTION, SETUP_FAMILY,
        ENTRY_ZONE_LOW, ENTRY_ZONE_HIGH,
        PRICE_INVALIDATION_LEVEL, INVALIDATION_RULE,
        TRAIL_STYLE, TRAIL_PARAMS, EXIT_STYLE,
        STRUCTURE_CONFIDENCE, LEVEL_SIGNIFICANCE, REGIME_COMPAT,
        MEANINGFUL_HIT_RATE, PATH_SURVIVAL_HIT_RATE, MFE_MAE_RATIO,
        RISK_CLASS, CONFLICT_RESOLUTION, RATIONALE_TEXT,
        COMMITTEE_PAYLOAD, STATUS,
        BOARD_RUN_ID, BOARD_CANDIDATE_ID, BOARD_DOSSIER_ID,
        PRIMARY_EVIDENCE_SETUP_EVENT_ID,
        BOARD_FINAL_RANK, BOARD_FINAL_VERDICT,
        BOARD_PRIMARY_REASON_CODE, BOARD_REASON_CODES, BOARD_RATIONALE, BOARD_PAYLOAD_JSON
    )
    SELECT
        s.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
        s.PORTFOLIO_ID,
        s.SYMBOL,
        v.FINAL_DIRECTION,
        v.PROPOSED_TRADE_CONFIG_JSON:thesis_label::STRING,
        TRY_TO_DOUBLE(v.PROPOSED_TRADE_CONFIG_JSON:entry_zone_low::STRING),
        TRY_TO_DOUBLE(v.PROPOSED_TRADE_CONFIG_JSON:entry_zone_high::STRING),
        TRY_TO_DOUBLE(v.PROPOSED_TRADE_CONFIG_JSON:invalidation_level::STRING),
        COALESCE(v.PROPOSED_TRADE_CONFIG_JSON:invalidation_rule::STRING, 'AGENTIC_INVALIDATION'),
        COALESCE(v.PROPOSED_TRADE_CONFIG_JSON:trailing_policy:trail_style::STRING, 'PCT'),
        COALESCE(v.PROPOSED_TRADE_CONFIG_JSON:trailing_policy:trail_params, PARSE_JSON('{"policy_version":"phase4_agentic","profile":"TRAIL_STANDARD","reference":"ENTRY_FILL","tp_mode":"LIMIT","trail_mode":"PCT","trail_value":2.5}')),
        COALESCE(v.PROPOSED_TRADE_CONFIG_JSON:target_policy:exit_style::STRING, 'STAGED_PARTIAL'),
        TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:structure:state_confidence::STRING),
        TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:levels:nearest_resistance:level_significance::STRING),
        COALESCE(s.DOSSIER_PAYLOAD_JSON:regime:tags:trend_regime::STRING, 'AGENTIC'),
        TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:long_history[0]:meaningful_hit_rate::STRING),
        TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:long_history[0]:path_survival_hit_rate::STRING),
        TRY_TO_DOUBLE(s.DOSSIER_PAYLOAD_JSON:history:long_history[0]:mfe_mae_ratio::STRING),
        COALESCE(v.PROPOSED_TRADE_CONFIG_JSON:risk_class::STRING, 'MEDIUM'),
        NULL,
        'Phase 4 agentic board rank ' || fs.RANK || ' | ' || v.FINAL_ACTION || ' | ' || v.PRIMARY_REASON_CODE || ' | ' || v.FINAL_THESIS,
        v.COMMITTEE_PAYLOAD,
        'PROPOSED',
        :v_run_id,
        NULL,
        v.DOSSIER_ID,
        s.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
        fs.RANK,
        v.FINAL_ACTION,
        v.PRIMARY_REASON_CODE,
        ARRAY_CONSTRUCT(v.PRIMARY_REASON_CODE, v.SECONDARY_REASON_CODE),
        v.FINAL_THESIS,
        OBJECT_CONSTRUCT(
            'agentic_board_cutover', TRUE,
            'direction_source', 'CHAIR_PORTFOLIO_PM.final_direction',
            'setup_family_source', 'CHAIR_PORTFOLIO_PM.proposed_trade_config.thesis_label',
            'primary_evidence_setup_event_id', s.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
            'primary_evidence_setup_event_id_role', 'EVIDENCE_ONLY_NOT_DIRECTION_SOURCE',
            'dossier_id', v.DOSSIER_ID,
            'dossier_payload', s.DOSSIER_PAYLOAD_JSON,
            'chair_output', v.CHAIR_OUTPUT_JSON,
            'proposed_trade_config', v.PROPOSED_TRADE_CONFIG_JSON
        )
    FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
    JOIN MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT v
      ON v.RUN_ID = fs.RUN_ID
     AND v.DOSSIER_ID = fs.DOSSIER_ID
    JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
      ON s.RUN_ID = fs.RUN_ID
     AND s.DOSSIER_ID = fs.DOSSIER_ID
    WHERE fs.RUN_ID = :v_run_id
      AND fs.PUBLICATION_STATUS = 'PENDING'
      AND fs.RANK <= :P_MAX_PROPOSALS
      AND s.PRIMARY_EVIDENCE_SETUP_EVENT_ID IS NOT NULL
      AND v.PROPOSED_TRADE_CONFIG_JSON:thesis_label::STRING ILIKE 'AGENTIC_%'
      AND (
            v.FINAL_ACTION = 'PROPOSE_LONG'
            OR (v.FINAL_ACTION = 'PROPOSE_SHORT' AND s.SHORT_LIVE_ENABLED)
          )
      AND NOT EXISTS (
          SELECT 1
          FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
          WHERE p.STATUS = 'PROPOSED'
            AND p.SYMBOL = s.SYMBOL
            AND (
                 :P_PORTFOLIO_ID IS NULL
                 OR p.PORTFOLIO_ID = :P_PORTFOLIO_ID
                 OR p.PORTFOLIO_ID IS NULL
            )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM MIP.LIVE.LIVE_ACTIONS la
          WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
            AND la.SYMBOL = s.SYMBOL
            AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION','OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')
            AND (
                 :P_PORTFOLIO_ID IS NULL
                 OR la.PORTFOLIO_ID = :P_PORTFOLIO_ID
                 OR la.PORTFOLIO_ID IS NULL
            )
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
     WHERE RUN_ID = :v_run_id
       AND PUBLICATION_STATUS = 'PENDING';

    SELECT COUNT(*) INTO :v_published_count
    FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2
    WHERE RUN_ID = :v_run_id
      AND PUBLICATION_STATUS = 'PUBLISHED';

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
        p.PROPOSAL_ID,
        p.CREATED_AT,
        p.SYMBOL,
        p.DIRECTION,
        p.SETUP_FAMILY,
        p.COMMITTEE_PAYLOAD:dossier_payload:structure:structural_state::STRING,
        p.COMMITTEE_PAYLOAD:dossier_payload:regime:tags:trend_regime::STRING,
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
    WHERE p.BOARD_RUN_ID = :v_run_id
      AND NOT EXISTS (
          SELECT 1
          FROM MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT s
          WHERE s.PROPOSAL_ID = p.PROPOSAL_ID
      );

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

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'sqlcode', :SQLCODE,
            'sqlerrm', :SQLERRM,
            'sqlstate', :SQLSTATE
        );
END;
$$;

GRANT USAGE ON PROCEDURE MIP.APP.SP_RUN_PROPOSAL_BOARD(NUMBER, INTEGER, DATE, INTEGER) TO ROLE MIP_ADMIN_ROLE;

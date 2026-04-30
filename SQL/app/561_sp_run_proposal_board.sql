/* ================================================================
   561_sp_run_proposal_board.sql
   Agentic Proposal Board v1 production selector.

   V1 uses deterministic local specialist functions to preserve the
   board contract while Cortex Agent execution is stabilized. It does
   not use the retired deterministic weighted composite selector.
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
    v_candidate_count   NUMBER := 0;
    v_final_count       NUMBER := 0;
    v_published_count   NUMBER := 0;
    v_invalid_count     NUMBER := 0;
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
        OBJECT_CONSTRUCT('mode', 'deterministic_local_specialists', 'target', 'snowflake_cortex_agents'),
        'proposal_board_v1',
        'proposal_board_policy_v1';

    -- Risk policy seeding remains deterministic evidence/policy substrate,
    -- not proposal selection.
    MERGE INTO MIP.APP.STRUCTURAL_RISK_POLICY tgt
    USING (
        SELECT * FROM (VALUES
            ('BREAKOUT_RETEST_LONG','LONG','CONFIRMED_CLOSE_BEYOND',0.3,5.0,'MFE_RISK_MULTIPLE',1.0,'STRUCTURAL','{}','OPEN_RUNNER',20,TRUE,'1.0'),
            ('SUPPORT_WICK_LONG','LONG','SINGLE_CLOSE_BEYOND',0.0,4.0,'MFE_RISK_MULTIPLE',1.5,'PROGRESS_BASED','{"breakeven_at":1.0,"lock_50_at":2.0}','STAGED_PARTIAL',15,TRUE,'1.0'),
            ('THREE_BAR_REVERSAL_LONG','LONG','SINGLE_CLOSE_BEYOND',0.0,4.0,'STRUCTURAL_LEVEL_BREAK',NULL,'HYBRID','{"switch_at":2.0}','STAGED_PARTIAL',20,TRUE,'1.0'),
            ('TREND_PULLBACK_LONG','LONG','SINGLE_CLOSE_BEYOND',0.0,3.0,'STRUCTURAL_LEVEL_BREAK',NULL,'STRUCTURAL','{}','OPEN_RUNNER',20,TRUE,'1.0'),
            ('BREAKDOWN_RETEST_SHORT','SHORT','CONFIRMED_CLOSE_BEYOND',0.3,5.0,'MFE_RISK_MULTIPLE',1.0,'PROGRESS_BASED','{"breakeven_at":1.0,"lock_50_at":2.0,"lock_60_at":3.0}','STAGED_PARTIAL',20,TRUE,'1.0'),
            ('RESISTANCE_WICK_SHORT','SHORT','SINGLE_CLOSE_BEYOND',0.0,4.0,'MFE_RISK_MULTIPLE',1.5,'PROGRESS_BASED','{"breakeven_at":1.0,"lock_50_at":2.0}','STAGED_PARTIAL',15,TRUE,'1.0'),
            ('THREE_BAR_REVERSAL_SHORT','SHORT','SINGLE_CLOSE_BEYOND',0.0,4.0,'STRUCTURAL_LEVEL_BREAK',NULL,'HYBRID','{"switch_at":1.5}','STAGED_PARTIAL',20,TRUE,'1.0'),
            ('FAILED_BREAKOUT_SHORT','SHORT','SINGLE_CLOSE_BEYOND',0.0,3.0,'IMMEDIATE',NULL,'STRUCTURAL','{"fast_escalation":true}','STAGED_PARTIAL',15,TRUE,'1.0')
        ) AS v(SF,DIR,PIR,IBUF,MIDP,TAT,TAP,TS,TP,ES,MHB,IA,PV)
    ) src
    ON  tgt.SETUP_FAMILY = src.SF
    AND tgt.DIRECTION = src.DIR
    AND tgt.POLICY_VERSION = src.PV
    WHEN NOT MATCHED THEN INSERT (
        SETUP_FAMILY, DIRECTION, PRICE_INVALIDATION_RULE, INVALIDATION_BUFFER_ATR,
        MAX_INVALIDATION_DISTANCE_PCT, TRAIL_ACTIVATION_TYPE, TRAIL_ACTIVATION_PARAM,
        TRAIL_STYLE, TRAIL_PARAMS, EXIT_STYLE, MAX_HOLD_BARS, IS_ACTIVE, POLICY_VERSION
    ) VALUES (
        src.SF, src.DIR, src.PIR, src.IBUF, src.MIDP, src.TAT, src.TAP,
        src.TS, PARSE_JSON(src.TP), src.ES, src.MHB, src.IA, src.PV
    );

    UPDATE MIP.APP.STRUCTURAL_RISK_POLICY
       SET TRAIL_STYLE  = 'PROGRESS_BASED',
           TRAIL_PARAMS = PARSE_JSON('{"breakeven_at":1.0,"lock_50_at":2.0}'),
           EXIT_STYLE   = 'STAGED_PARTIAL'
     WHERE SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'
       AND DIRECTION = 'LONG'
       AND IS_ACTIVE = TRUE;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT (
        RUN_ID, SETUP_EVENT_ID, SOURCE_CANDIDATE_KEY, AS_OF_DATE, PORTFOLIO_ID,
        SYMBOL, MARKET_TYPE, FAMILY, DIRECTION, SETUP_DATE, SETUP_STATUS,
        STRUCTURAL_STATE, REGIME_COMPAT, TRUST_LABEL,
        ENTRY_ZONE_LOW, ENTRY_ZONE_HIGH, PRICE_INVALIDATION_LEVEL,
        STRUCTURE_CONFIDENCE, LEVEL_SIGNIFICANCE, MEANINGFUL_HIT_RATE,
        PATH_SURVIVAL_HIT_RATE, MFE_MAE_RATIO, RISK_CLASS,
        ABSOLUTE_EXCLUSION_REASONS, BOARD_WARNING_FLAGS,
        FRESHNESS_FLAGS_JSON, RECENT_TRADE_FLAGS_JSON,
        EVIDENCE_PAYLOAD_JSON, PAYLOAD_HASH
    )
    SELECT
        :v_run_id,
        e.SETUP_EVENT_ID,
        e.SOURCE_CANDIDATE_KEY,
        :v_as_of,
        :P_PORTFOLIO_ID,
        e.SYMBOL,
        e.MARKET_TYPE,
        e.SETUP_FAMILY,
        e.DIRECTION,
        e.SETUP_DATE,
        e.SETUP_STATUS,
        e.STRUCTURAL_STATE,
        e.REGIME_COMPAT,
        e.TRUST_LABEL,
        e.ENTRY_ZONE_LOW,
        e.ENTRY_ZONE_HIGH,
        e.PRICE_INVALIDATION_LEVEL,
        e.STRUCTURE_CONFIDENCE,
        e.LEVEL_SIGNIFICANCE,
        e.MEANINGFUL_HIT_RATE,
        e.PATH_SURVIVAL_HIT_RATE,
        e.MFE_MAE_RATIO,
        e.RISK_CLASS,
        e.ABSOLUTE_EXCLUSION_REASONS,
        e.BOARD_WARNING_FLAGS,
        OBJECT_CONSTRUCT(
            'same_day', e.SETUP_DATE = :v_as_of,
            'bars_since_detection', e.BARS_SINCE_DETECTION,
            'setup_status', e.SETUP_STATUS
        ),
        OBJECT_CONSTRUCT(
            'recent_failed_trade_30d', e.RECENT_FAILED_TRADE_30D,
            'recent_terminal_action_14d', e.RECENT_TERMINAL_ACTION_14D,
            'recent_outcome_class', e.RECENT_OUTCOME_CLASS,
            'last_proposed_at', e.LAST_PROPOSED_AT
        ),
        e.EVIDENCE_PAYLOAD_JSON,
        SHA2(TO_JSON(e.EVIDENCE_PAYLOAD_JSON), 256)
    FROM MIP.MART.V_PROPOSAL_BOARD_CANDIDATE_EVIDENCE e
    WHERE e.SETUP_DATE BETWEEN DATEADD('day', -5, :v_as_of) AND :v_as_of
      AND NOT e.ABSOLUTE_EXCLUDED
      -- Active-proposal dedup. When P_PORTFOLIO_ID is NULL the board
      -- is producing portfolio-agnostic candidates that, if approved,
      -- can spawn live actions on any portfolio; in that mode we must
      -- block on ANY portfolio's existing active proposal for the same
      -- setup event. When a specific portfolio is requested we block
      -- on either that portfolio or a portfolio-agnostic active row.
      AND NOT EXISTS (
          SELECT 1
          FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
          WHERE p.SETUP_EVENT_ID = e.SETUP_EVENT_ID
            AND p.STATUS = 'PROPOSED'
            AND (
                 :P_PORTFOLIO_ID IS NULL
                 OR p.PORTFOLIO_ID = :P_PORTFOLIO_ID
                 OR p.PORTFOLIO_ID IS NULL
            )
      )
      -- In-flight live-actions dedup. Same NULL-aware semantics: a
      -- portfolio-agnostic board run must respect every portfolio's
      -- in-flight execution to avoid emitting a duplicate proposal
      -- against a setup event already being acted on.
      AND NOT EXISTS (
          SELECT 1
          FROM MIP.LIVE.LIVE_ACTIONS la
          JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
            ON p.PROPOSAL_ID = la.PROPOSAL_ID
          WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
            AND p.SETUP_EVENT_ID = e.SETUP_EVENT_ID
            AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION','OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')
            AND (
                 :P_PORTFOLIO_ID IS NULL
                 OR la.PORTFOLIO_ID = :P_PORTFOLIO_ID
                 OR la.PORTFOLIO_ID IS NULL
            )
      );

    SELECT COUNT(*) INTO :v_candidate_count
    FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT
    WHERE RUN_ID = :v_run_id;

    UPDATE MIP.APP.PROPOSAL_BOARD_RUN
       SET CANDIDATE_COUNT = :v_candidate_count
     WHERE RUN_ID = :v_run_id;

    IF (v_candidate_count = 0) THEN
        UPDATE MIP.APP.PROPOSAL_BOARD_RUN
           SET RUN_STATUS = 'COMPLETE',
               FINAL_PROPOSAL_COUNT = 0,
               FINISHED_AT = CURRENT_TIMESTAMP(),
               ERROR_JSON = OBJECT_CONSTRUCT('message', 'NO_GOOD_IDEAS_TODAY', 'reason_code', 'NO_GOOD_IDEAS_TODAY')
         WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'COMPLETE',
            'run_id', :v_run_id,
            'as_of_date', :v_as_of,
            'candidate_count', 0,
            'published_count', 0
        );
    END IF;

    -- Structure Agent
    INSERT INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, VERDICT, PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE, CONFIDENCE, RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
    )
    SELECT
        RUN_ID,
        CANDIDATE_ID,
        'STRUCTURE_AGENT',
        CASE
            WHEN COALESCE(STRUCTURE_CONFIDENCE, 0) < 0.35 THEN 'reject'
            WHEN SETUP_STATUS IN ('STALE', 'WAITING')
              OR ARRAY_CONTAINS('OPPOSING_SETUP'::VARIANT, BOARD_WARNING_FLAGS) THEN 'weak'
            ELSE 'approve'
        END,
        CASE
            WHEN COALESCE(STRUCTURE_CONFIDENCE, 0) < 0.35 THEN 'STRUCTURE_WEAK'
            WHEN ARRAY_CONTAINS('OPPOSING_SETUP'::VARIANT, BOARD_WARNING_FLAGS) THEN 'CONFLICTING_STRUCTURE'
            WHEN SETUP_STATUS IN ('STALE', 'WAITING') THEN 'STRUCTURE_NOT_FRESH'
            ELSE 'STRUCTURE_APPROVED'
        END,
        IFF(COALESCE(LEVEL_SIGNIFICANCE, 0) < 0.30, 'FAMILY_INTERPRETATION_WEAK', NULL),
        LEAST(0.95, GREATEST(0.35, COALESCE(STRUCTURE_CONFIDENCE, 0.5))),
        'Structure review based on setup family, state, level geometry, freshness, and conflicts.',
        OBJECT_CONSTRUCT(
            'verdict_schema', 'proposal_board_agent_v1',
            'supporting_evidence_keys', ARRAY_CONSTRUCT('STRUCTURE_CONFIDENCE','STRUCTURAL_STATE','LEVEL_SIGNIFICANCE','SETUP_STATUS','BOARD_WARNING_FLAGS'),
            'concern_flags', BOARD_WARNING_FLAGS
        )
    FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT
    WHERE RUN_ID = :v_run_id;

    -- Opportunity Quality Agent
    INSERT INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, VERDICT, PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE, CONFIDENCE, RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
    )
    SELECT
        RUN_ID,
        CANDIDATE_ID,
        'OPPORTUNITY_QUALITY_AGENT',
        CASE
            WHEN ARRAY_CONTAINS('NOISY_CHOPPY_PRICE_ACTION'::VARIANT, BOARD_WARNING_FLAGS) THEN 'weak'
            WHEN ARRAY_CONTAINS('PRICE_DISTANCE_FROM_ENTRY_ZONE'::VARIANT, BOARD_WARNING_FLAGS) THEN 'watch'
            WHEN SETUP_STATUS IN ('STALE', 'WAITING') THEN 'watch'
            ELSE 'attractive'
        END,
        CASE
            WHEN ARRAY_CONTAINS('NOISY_CHOPPY_PRICE_ACTION'::VARIANT, BOARD_WARNING_FLAGS) THEN 'NOISY_CHOPPY_PRICE_ACTION'
            WHEN ARRAY_CONTAINS('PRICE_DISTANCE_FROM_ENTRY_ZONE'::VARIANT, BOARD_WARNING_FLAGS) THEN 'TOO_EXTENDED'
            WHEN SETUP_STATUS IN ('STALE', 'WAITING') THEN 'TREND_STALE'
            ELSE 'OPPORTUNITY_ATTRACTIVE'
        END,
        NULL,
        CASE
            WHEN SETUP_STATUS IN ('STALE', 'WAITING') THEN 0.55
            WHEN ARRAY_CONTAINS('PRICE_DISTANCE_FROM_ENTRY_ZONE'::VARIANT, BOARD_WARNING_FLAGS) THEN 0.50
            ELSE 0.75
        END,
        'Opportunity review based on setup freshness, entry distance, and bar-quality warning flags.',
        OBJECT_CONSTRUCT(
            'verdict_schema', 'proposal_board_agent_v1',
            'supporting_evidence_keys', ARRAY_CONSTRUCT('SETUP_STATUS','DISTANCE_FROM_ENTRY_ZONE','BOARD_WARNING_FLAGS'),
            'concern_flags', BOARD_WARNING_FLAGS
        )
    FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT
    WHERE RUN_ID = :v_run_id;

    -- Historical Evidence Agent
    INSERT INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, VERDICT, PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE, CONFIDENCE, RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
    )
    SELECT
        RUN_ID,
        CANDIDATE_ID,
        'HISTORICAL_EVIDENCE_AGENT',
        CASE
            WHEN TRUST_LABEL = 'REJECTED' THEN 'reject'
            WHEN ARRAY_CONTAINS('RECENT_FAILED_SYMBOL'::VARIANT, BOARD_WARNING_FLAGS)
              OR ARRAY_CONTAINS('REPEATED_REPITCH'::VARIANT, BOARD_WARNING_FLAGS)
              OR ARRAY_CONTAINS('SPARSE_HISTORY'::VARIANT, BOARD_WARNING_FLAGS) THEN 'mixed'
            WHEN COALESCE(MEANINGFUL_HIT_RATE, 0) < 0.45 THEN 'weak'
            ELSE 'evidence_supported'
        END,
        CASE
            WHEN TRUST_LABEL = 'REJECTED' THEN 'EVIDENCE_WEAK'
            WHEN ARRAY_CONTAINS('RECENT_FAILED_SYMBOL'::VARIANT, BOARD_WARNING_FLAGS) THEN 'RECENT_FAILED_SYMBOL'
            WHEN ARRAY_CONTAINS('REPEATED_REPITCH'::VARIANT, BOARD_WARNING_FLAGS) THEN 'REPEATED_REPITCH'
            WHEN ARRAY_CONTAINS('SPARSE_HISTORY'::VARIANT, BOARD_WARNING_FLAGS) THEN 'SAMPLE_SIZE_LOW'
            WHEN COALESCE(PATH_SURVIVAL_HIT_RATE, 0) < 0.35 THEN 'PATH_SURVIVAL_WEAK'
            WHEN COALESCE(MEANINGFUL_HIT_RATE, 0) < 0.45 THEN 'EVIDENCE_WEAK'
            ELSE 'EVIDENCE_SUPPORTED'
        END,
        IFF(COALESCE(MFE_MAE_RATIO, 0) < 1.0, 'EVIDENCE_MIXED', NULL),
        CASE
            WHEN TRUST_LABEL = 'TRUSTED' THEN 0.80
            WHEN TRUST_LABEL = 'PROVISIONAL' THEN 0.68
            ELSE 0.55
        END,
        'Historical review based on family trust, path stats, sample size, recent symbol memory, and re-pitch warnings.',
        OBJECT_CONSTRUCT(
            'verdict_schema', 'proposal_board_agent_v1',
            'supporting_evidence_keys', ARRAY_CONSTRUCT('TRUST_LABEL','MEANINGFUL_HIT_RATE','PATH_SURVIVAL_HIT_RATE','MFE_MAE_RATIO','RECENT_TRADE_FLAGS_JSON'),
            'concern_flags', BOARD_WARNING_FLAGS
        )
    FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT
    WHERE RUN_ID = :v_run_id;

    -- Risk / Execution Feasibility Agent
    INSERT INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, VERDICT, PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE, CONFIDENCE, RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
    )
    SELECT
        RUN_ID,
        CANDIDATE_ID,
        'RISK_EXECUTION_FEASIBILITY_AGENT',
        CASE
            WHEN ARRAY_CONTAINS('DIRECTION_NOT_EXECUTABLE'::VARIANT, BOARD_WARNING_FLAGS) THEN 'reject'
            WHEN RISK_CLASS IN ('HIGH', 'GAP_AWARE')
              OR COALESCE(TRY_TO_DOUBLE(EVIDENCE_PAYLOAD_JSON:history:gap_risk_contribution::STRING), 0) >= 0.35 THEN 'constrained'
            ELSE 'executable'
        END,
        CASE
            WHEN ARRAY_CONTAINS('DIRECTION_NOT_EXECUTABLE'::VARIANT, BOARD_WARNING_FLAGS) THEN 'DIRECTION_NOT_EXECUTABLE'
            WHEN DIRECTION = 'SHORT' THEN 'SHORT_HISTORY_NOT_OPERATIONAL'
            WHEN RISK_CLASS = 'GAP_AWARE'
              OR COALESCE(TRY_TO_DOUBLE(EVIDENCE_PAYLOAD_JSON:history:gap_risk_contribution::STRING), 0) >= 0.35 THEN 'GAP_RISK_HIGH'
            WHEN RISK_CLASS = 'HIGH' THEN 'SIZE_REDUCE_REQUIRED'
            ELSE 'EXECUTABLE'
        END,
        IFF(RISK_CLASS IN ('HIGH', 'GAP_AWARE'), 'EXECUTION_CONSTRAINED', NULL),
        CASE
            WHEN ARRAY_CONTAINS('DIRECTION_NOT_EXECUTABLE'::VARIANT, BOARD_WARNING_FLAGS) THEN 0.90
            WHEN RISK_CLASS IN ('HIGH', 'GAP_AWARE') THEN 0.72
            ELSE 0.80
        END,
        'Proposal-time feasibility review. Downstream committee/live execution remains the final execution authority.',
        OBJECT_CONSTRUCT(
            'verdict_schema', 'proposal_board_agent_v1',
            'supporting_evidence_keys', ARRAY_CONSTRUCT('ENTRY_ZONE_LOW','ENTRY_ZONE_HIGH','PRICE_INVALIDATION_LEVEL','RISK_CLASS','BOARD_WARNING_FLAGS'),
            'concern_flags', BOARD_WARNING_FLAGS
        )
    FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT
    WHERE RUN_ID = :v_run_id;

    -- Reason-code validation. Unknown codes are not allowed to slip through.
    INSERT INTO MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, ERROR_TYPE, RAW_OUTPUT_JSON, ERROR_MESSAGE
    )
    SELECT
        o.RUN_ID,
        o.CANDIDATE_ID,
        o.AGENT_NAME,
        'UNKNOWN_REASON_CODE',
        o.STRUCTURED_OUTPUT_JSON,
        'Agent output used inactive or unknown primary reason code: ' || o.PRIMARY_REASON_CODE
    FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME o
    LEFT JOIN MIP.APP.PROPOSAL_BOARD_REASON_CODE rc
      ON rc.REASON_CODE = o.PRIMARY_REASON_CODE
     AND rc.IS_ACTIVE = TRUE
    WHERE o.RUN_ID = :v_run_id
      AND rc.REASON_CODE IS NULL;

    SELECT COUNT(*) INTO :v_invalid_count
    FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR
    WHERE RUN_ID = :v_run_id;

    IF (v_invalid_count > 0) THEN
        UPDATE MIP.APP.PROPOSAL_BOARD_RUN
           SET RUN_STATUS = 'FAILED',
               FINISHED_AT = CURRENT_TIMESTAMP(),
               ERROR_JSON = OBJECT_CONSTRUCT('reason_code', 'SYSTEM_VALIDATION_FAILED', 'invalid_output_count', :v_invalid_count)
         WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'reason_code', 'SYSTEM_VALIDATION_FAILED',
            'invalid_output_count', :v_invalid_count
        );
    END IF;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_INTERACTION (
        RUN_ID, CANDIDATE_ID, SOURCE_AGENT, TARGET_AGENT, TOPIC,
        DISAGREEMENT_TYPE, DISAGREEMENT_TEXT, RESPONSE_TEXT, RESOLVED_FLAG
    )
    SELECT
        s.RUN_ID,
        s.CANDIDATE_ID,
        'ORCHESTRATOR',
        'SPECIALISTS',
        'candidate_disagreement',
        'MIXED_AGENT_VERDICTS',
        'At least one specialist supported the candidate while another rejected or constrained it.',
        'Chair resolves through final verdict and persisted reason codes.',
        TRUE
    FROM (
        SELECT
            RUN_ID,
            CANDIDATE_ID,
            COUNT_IF(VERDICT IN ('approve','attractive','evidence_supported','executable')) AS SUPPORTIVE_COUNT,
            COUNT_IF(VERDICT IN ('reject','weak','constrained','mixed','watch')) AS CONCERN_COUNT
        FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME
        WHERE RUN_ID = :v_run_id
        GROUP BY RUN_ID, CANDIDATE_ID
    ) s
    WHERE s.SUPPORTIVE_COUNT > 0
      AND s.CONCERN_COUNT > 0;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT (
        RUN_ID, CANDIDATE_ID, FINAL_RANK, FINAL_VERDICT,
        PRIMARY_REASON_CODE, SECONDARY_REASON_CODE, FINAL_RATIONALE,
        WHY_SELECTED_OR_REJECTED, COMPARATIVE_REASONING_JSON
    )
    WITH agent_summary AS (
        SELECT
            RUN_ID,
            CANDIDATE_ID,
            COUNT_IF(VERDICT = 'reject') AS REJECT_COUNT,
            COUNT_IF(VERDICT IN ('weak','mixed','watch','constrained')) AS CONCERN_COUNT,
            COUNT_IF(VERDICT IN ('approve','attractive','evidence_supported','executable')) AS SUPPORT_COUNT,
            AVG(CONFIDENCE) AS AVG_CONFIDENCE,
            ARRAY_AGG(PRIMARY_REASON_CODE) WITHIN GROUP (ORDER BY AGENT_NAME) AS REASON_CODES
        FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME
        WHERE RUN_ID = :v_run_id
        GROUP BY RUN_ID, CANDIDATE_ID
    ),
    decisions AS (
        SELECT
            c.RUN_ID,
            c.CANDIDATE_ID,
            c.SETUP_EVENT_ID,
            c.SYMBOL,
            c.DIRECTION,
            c.SETUP_DATE,
            c.STRUCTURE_CONFIDENCE,
            c.LEVEL_SIGNIFICANCE,
            c.BOARD_WARNING_FLAGS,
            a.REJECT_COUNT,
            a.CONCERN_COUNT,
            a.SUPPORT_COUNT,
            a.AVG_CONFIDENCE,
            a.REASON_CODES,
            CASE
                WHEN ARRAY_CONTAINS('DIRECTION_NOT_EXECUTABLE'::VARIANT, c.BOARD_WARNING_FLAGS) THEN 'WATCH'
                WHEN a.REJECT_COUNT > 0 THEN 'REJECT'
                WHEN a.CONCERN_COUNT >= 2 THEN 'APPROVE_REDUCED'
                ELSE 'APPROVE'
            END AS FINAL_VERDICT,
            CASE
                WHEN ARRAY_CONTAINS('DIRECTION_NOT_EXECUTABLE'::VARIANT, c.BOARD_WARNING_FLAGS) THEN 'WATCHLIST_ONLY'
                WHEN a.REJECT_COUNT > 0 THEN COALESCE(a.REASON_CODES[0]::STRING, 'EVIDENCE_WEAK')
                WHEN a.CONCERN_COUNT >= 2 THEN 'APPROVED_REDUCED_BY_BOARD'
                ELSE 'APPROVED_BY_BOARD'
            END AS PRIMARY_REASON_CODE,
            CASE
                WHEN ARRAY_CONTAINS('RECENT_FAILED_SYMBOL'::VARIANT, c.BOARD_WARNING_FLAGS) THEN 'RECENT_FAILED_SYMBOL'
                WHEN ARRAY_CONTAINS('REPEATED_REPITCH'::VARIANT, c.BOARD_WARNING_FLAGS) THEN 'REPEATED_REPITCH'
                WHEN ARRAY_CONTAINS('OPPOSING_SETUP'::VARIANT, c.BOARD_WARNING_FLAGS) THEN 'CONFLICTING_STRUCTURE'
                ELSE NULL
            END AS SECONDARY_REASON_CODE
        FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT c
        JOIN agent_summary a
          ON a.RUN_ID = c.RUN_ID
         AND a.CANDIDATE_ID = c.CANDIDATE_ID
        WHERE c.RUN_ID = :v_run_id
    )
    SELECT
        RUN_ID,
        CANDIDATE_ID,
        ROW_NUMBER() OVER (
            ORDER BY
                CASE FINAL_VERDICT
                    WHEN 'APPROVE' THEN 1
                    WHEN 'APPROVE_REDUCED' THEN 2
                    WHEN 'WATCH' THEN 3
                    ELSE 4
                END,
                SUPPORT_COUNT DESC,
                CONCERN_COUNT ASC,
                SETUP_DATE DESC,
                COALESCE(STRUCTURE_CONFIDENCE, 0) DESC,
                COALESCE(LEVEL_SIGNIFICANCE, 0) DESC,
                SYMBOL
        ) AS FINAL_RANK,
        FINAL_VERDICT,
        PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE,
        'Chair synthesized four specialist reviews and selected a final board verdict.',
        CASE
            WHEN FINAL_VERDICT IN ('APPROVE','APPROVE_REDUCED') THEN 'Selected for publication by board verdict.'
            WHEN FINAL_VERDICT = 'WATCH' THEN 'Kept as board-visible watchlist evidence but not published as actionable.'
            ELSE 'Rejected by board due to specialist objections or weak evidence.'
        END,
        OBJECT_CONSTRUCT(
            'support_count', SUPPORT_COUNT,
            'concern_count', CONCERN_COUNT,
            'reject_count', REJECT_COUNT,
            'avg_confidence', AVG_CONFIDENCE,
            'reason_codes', REASON_CODES,
            'warning_flags', BOARD_WARNING_FLAGS
        )
    FROM decisions;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_FINAL_SLATE (
        RUN_ID, CANDIDATE_ID, SETUP_EVENT_ID, SYMBOL, RANK, VERDICT,
        SIZING_TREATMENT, RISK_CLASS, FINAL_RATIONALE_SUMMARY,
        DOWNSTREAM_PAYLOAD_POINTER, PUBLICATION_STATUS
    )
    SELECT
        v.RUN_ID,
        c.CANDIDATE_ID,
        c.SETUP_EVENT_ID,
        c.SYMBOL,
        v.FINAL_RANK,
        v.FINAL_VERDICT,
        IFF(v.FINAL_VERDICT = 'APPROVE_REDUCED' OR c.RISK_CLASS IN ('HIGH','GAP_AWARE'), 'REDUCED_OR_CONSTRAINED', 'STANDARD'),
        c.RISK_CLASS,
        v.FINAL_RATIONALE,
        'MIP.APP.STRUCTURAL_TRADE_PROPOSALS',
        'PENDING'
    FROM MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT v
    JOIN MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT c
      ON c.RUN_ID = v.RUN_ID
     AND c.CANDIDATE_ID = v.CANDIDATE_ID
    WHERE v.RUN_ID = :v_run_id
      AND v.FINAL_VERDICT IN ('APPROVE', 'APPROVE_REDUCED')
      AND v.FINAL_RANK <= :P_MAX_PROPOSALS;

    SELECT COUNT(*) INTO :v_final_count
    FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE
    WHERE RUN_ID = :v_run_id;

    IF (v_final_count = 0) THEN
        UPDATE MIP.APP.PROPOSAL_BOARD_RUN
           SET RUN_STATUS = 'COMPLETE',
               FINAL_PROPOSAL_COUNT = 0,
               FINISHED_AT = CURRENT_TIMESTAMP(),
               ERROR_JSON = OBJECT_CONSTRUCT('message', 'NO_GOOD_IDEAS_TODAY', 'reason_code', 'NO_GOOD_IDEAS_TODAY')
         WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'COMPLETE',
            'run_id', :v_run_id,
            'as_of_date', :v_as_of,
            'candidate_count', :v_candidate_count,
            'published_count', 0,
            'reason_code', 'NO_GOOD_IDEAS_TODAY'
        );
    END IF;

    INSERT INTO MIP.APP.STRUCTURAL_TRADE_PROPOSALS (
        SETUP_EVENT_ID, PORTFOLIO_ID, SYMBOL, DIRECTION, SETUP_FAMILY,
        ENTRY_ZONE_LOW, ENTRY_ZONE_HIGH,
        PRICE_INVALIDATION_LEVEL, INVALIDATION_RULE,
        TRAIL_STYLE, TRAIL_PARAMS, EXIT_STYLE,
        STRUCTURE_CONFIDENCE, LEVEL_SIGNIFICANCE, REGIME_COMPAT,
        MEANINGFUL_HIT_RATE, PATH_SURVIVAL_HIT_RATE, MFE_MAE_RATIO,
        RISK_CLASS, CONFLICT_RESOLUTION, RATIONALE_TEXT,
        COMMITTEE_PAYLOAD, STATUS,
        BOARD_RUN_ID, BOARD_CANDIDATE_ID, BOARD_FINAL_RANK, BOARD_FINAL_VERDICT,
        BOARD_PRIMARY_REASON_CODE, BOARD_REASON_CODES, BOARD_RATIONALE, BOARD_PAYLOAD_JSON
    )
    SELECT
        c.SETUP_EVENT_ID,
        c.PORTFOLIO_ID,
        c.SYMBOL,
        c.DIRECTION,
        c.FAMILY,
        c.ENTRY_ZONE_LOW,
        c.ENTRY_ZONE_HIGH,
        c.PRICE_INVALIDATION_LEVEL,
        c.EVIDENCE_PAYLOAD_JSON:execution:invalidation_rule::STRING,
        c.EVIDENCE_PAYLOAD_JSON:execution:trail_style::STRING,
        c.EVIDENCE_PAYLOAD_JSON:execution:trail_params,
        c.EVIDENCE_PAYLOAD_JSON:execution:exit_style::STRING,
        c.STRUCTURE_CONFIDENCE,
        c.LEVEL_SIGNIFICANCE,
        c.REGIME_COMPAT,
        c.MEANINGFUL_HIT_RATE,
        c.PATH_SURVIVAL_HIT_RATE,
        c.MFE_MAE_RATIO,
        c.RISK_CLASS,
        NULL,
        'Board rank ' || fs.RANK || ' | ' || v.FINAL_VERDICT || ' | ' || v.PRIMARY_REASON_CODE || ' | ' || v.FINAL_RATIONALE,
        OBJECT_INSERT(
            OBJECT_INSERT(
                OBJECT_INSERT(c.EVIDENCE_PAYLOAD_JSON, 'board_run_id', :v_run_id, TRUE),
                'board_candidate_id', c.CANDIDATE_ID, TRUE
            ),
            'board_verdict',
            OBJECT_CONSTRUCT(
                'final_rank', fs.RANK,
                'final_verdict', v.FINAL_VERDICT,
                'primary_reason_code', v.PRIMARY_REASON_CODE,
                'secondary_reason_code', v.SECONDARY_REASON_CODE,
                'reason_codes', v.COMPARATIVE_REASONING_JSON:reason_codes,
                'rationale', v.FINAL_RATIONALE
            ),
            TRUE
        ),
        'PROPOSED',
        :v_run_id,
        c.CANDIDATE_ID,
        fs.RANK,
        v.FINAL_VERDICT,
        v.PRIMARY_REASON_CODE,
        v.COMPARATIVE_REASONING_JSON:reason_codes,
        v.FINAL_RATIONALE,
        OBJECT_CONSTRUCT(
            'candidate_snapshot', c.EVIDENCE_PAYLOAD_JSON,
            'chair_verdict', v.COMPARATIVE_REASONING_JSON,
            'publication_rank', fs.RANK
        )
    FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE fs
    JOIN MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT c
      ON c.RUN_ID = fs.RUN_ID
     AND c.CANDIDATE_ID = fs.CANDIDATE_ID
    JOIN MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT v
      ON v.RUN_ID = fs.RUN_ID
     AND v.CANDIDATE_ID = fs.CANDIDATE_ID
    WHERE fs.RUN_ID = :v_run_id
      AND fs.PUBLICATION_STATUS = 'PENDING'
      -- Publication-time safety net. Symmetric NULL-aware semantics to
      -- the snapshot dedup above: a portfolio-agnostic candidate
      -- (c.PORTFOLIO_ID IS NULL) must not be published if any active
      -- proposal exists for the same setup event on any portfolio.
      AND NOT EXISTS (
          SELECT 1
          FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
          WHERE p.SETUP_EVENT_ID = c.SETUP_EVENT_ID
            AND p.STATUS = 'PROPOSED'
            AND (
                 c.PORTFOLIO_ID IS NULL
                 OR p.PORTFOLIO_ID = c.PORTFOLIO_ID
                 OR p.PORTFOLIO_ID IS NULL
            )
      );

    UPDATE MIP.APP.PROPOSAL_BOARD_FINAL_SLATE fs
       SET PUBLICATION_STATUS = 'PUBLISHED',
           PUBLISHED_PROPOSAL_ID = p.PROPOSAL_ID,
           PUBLISHED_AT = p.CREATED_AT
      FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
     WHERE fs.RUN_ID = :v_run_id
       AND p.BOARD_RUN_ID = fs.RUN_ID
       AND p.BOARD_CANDIDATE_ID = fs.CANDIDATE_ID
       AND fs.PUBLICATION_STATUS = 'PENDING';

    UPDATE MIP.APP.PROPOSAL_BOARD_FINAL_SLATE
       SET PUBLICATION_STATUS = 'SKIPPED_DUPLICATE',
           PUBLICATION_ERROR_JSON = OBJECT_CONSTRUCT('reason', 'duplicate_or_not_inserted')
     WHERE RUN_ID = :v_run_id
       AND PUBLICATION_STATUS = 'PENDING';

    SELECT COUNT(*) INTO :v_published_count
    FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE
    WHERE RUN_ID = :v_run_id
      AND PUBLICATION_STATUS = 'PUBLISHED';

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
        p.COMMITTEE_PAYLOAD:structure:structural_state::STRING,
        p.REGIME_COMPAT,
        COALESCE(p.COMMITTEE_PAYLOAD:history:trust_label::STRING, 'UNKNOWN'),
        OBJECT_CONSTRUCT('low', p.ENTRY_ZONE_LOW, 'high', p.ENTRY_ZONE_HIGH),
        OBJECT_CONSTRUCT('level', p.PRICE_INVALIDATION_LEVEL, 'rule', p.INVALIDATION_RULE),
        OBJECT_CONSTRUCT(
            'meaningful_hit_rate', p.MEANINGFUL_HIT_RATE,
            'path_survival_hit_rate', p.PATH_SURVIVAL_HIT_RATE,
            'mfe_mae_ratio', p.MFE_MAE_RATIO,
            'median_mfe', p.COMMITTEE_PAYLOAD:history:median_mfe,
            'median_mae', p.COMMITTEE_PAYLOAD:history:median_mae,
            'gap_risk', p.COMMITTEE_PAYLOAD:history:gap_risk_contribution
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
        'candidate_count', :v_candidate_count,
        'final_slate_count', :v_final_count,
        'published_count', :v_published_count,
        'elapsed_sec', DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE MIP.APP.PROPOSAL_BOARD_RUN
           SET RUN_STATUS = 'FAILED',
               FINISHED_AT = CURRENT_TIMESTAMP(),
               ERROR_JSON = OBJECT_CONSTRUCT('sqlcode', SQLCODE, 'sqlerrm', SQLERRM, 'sqlstate', SQLSTATE)
         WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'sqlcode', SQLCODE,
            'sqlerrm', SQLERRM,
            'sqlstate', SQLSTATE
        );
END;
$$;

GRANT USAGE ON PROCEDURE MIP.APP.SP_RUN_PROPOSAL_BOARD(NUMBER, INTEGER, DATE, INTEGER) TO ROLE MIP_ADMIN_ROLE;

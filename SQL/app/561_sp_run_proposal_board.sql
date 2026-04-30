/* ================================================================
   561_sp_run_proposal_board.sql
   Agentic Proposal Board v2 production selector.

   Sprint 1 of Phase 2: the four specialist agents are Cortex-backed
   via snowflake.cortex.complete(), with the deterministic v1 logic
   preserved per row as the fallback path when a Cortex response is
   missing, malformed, or fails the per-agent verdict / reason-code
   contract. The chair (orchestrator), final-slate selection, and
   publication path remain deterministic and Phase-1-compatible.

   Hard rules carried forward from Phase 1:
   - The board is the sole proposal-time selector. STRUCTURAL_TRADE_PROPOSALS
     is the single publication target.
   - No old deterministic-composite selector path. No UI toggle. No
     structural_priority.py revival.
   - Reason-code validation hard-fails the run if any specialist emits
     an inactive or unknown PRIMARY_REASON_CODE.
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
        OBJECT_CONSTRUCT(
            'mode',           'cortex_specialists_with_deterministic_fallback',
            'specialists',    ARRAY_CONSTRUCT(
                                'STRUCTURE_AGENT',
                                'OPPORTUNITY_QUALITY_AGENT',
                                'HISTORICAL_EVIDENCE_AGENT',
                                'RISK_EXECUTION_FEASIBILITY_AGENT'
                              ),
            'specialist_model', :v_model_name,
            'specialist_versions', OBJECT_CONSTRUCT(
                                       'structure_agent',                 'phase3_step3_structure_refined_v1',
                                       'opportunity_quality_agent',       'phase3_step3_opportunity_refined_v1',
                                       'historical_evidence_agent',       'phase3_step3_historical_refined_v1',
                                       'risk_execution_feasibility_agent','phase3_step2_risk_refined_v1'
                                   ),
            'chair_mode',     'deterministic_templated_chair',
            'chair_template_version', 'phase3_step4_chair_matrix_v1',
            'fallback_policy','per_row_deterministic_when_cortex_invalid'
        ),
        'proposal_board_v2_phase3_step4',
        'proposal_board_policy_v2_phase3_step5';

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

    -- ============================================================
    -- Specialist 1: STRUCTURE_AGENT (Cortex with deterministic fallback)
    -- ============================================================
    INSERT INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, VERDICT, PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE, CONFIDENCE, RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
    )
    WITH prompted AS (
        SELECT
            c.RUN_ID,
            c.CANDIDATE_ID,
            c.SYMBOL,
            c.DIRECTION,
            c.FAMILY,
            c.SETUP_DATE,
            c.SETUP_STATUS,
            c.STRUCTURAL_STATE,
            c.REGIME_COMPAT,
            c.STRUCTURE_CONFIDENCE,
            c.LEVEL_SIGNIFICANCE,
            c.ENTRY_ZONE_LOW,
            c.ENTRY_ZONE_HIGH,
            c.PRICE_INVALIDATION_LEVEL,
            c.BOARD_WARNING_FLAGS,
            c.ABSOLUTE_EXCLUSION_REASONS,
'You are the Structure Specialist on the MIP Agentic Proposal Board. Your job is to review the structural coherence of a single trade-setup candidate. Focus only on structure (family, state, level geometry, freshness, conflicts). Other specialists handle opportunity timing, history, and execution.

CANDIDATE EVIDENCE:
- symbol: ' || COALESCE(c.SYMBOL, 'NULL') ||
'
- direction: ' || COALESCE(c.DIRECTION, 'NULL') ||
'
- setup_family: ' || COALESCE(c.FAMILY, 'NULL') ||
'
- setup_date: ' || COALESCE(TO_VARCHAR(c.SETUP_DATE), 'NULL') ||
'
- setup_status: ' || COALESCE(c.SETUP_STATUS, 'NULL') ||
'
- structural_state: ' || COALESCE(c.STRUCTURAL_STATE, 'NULL') ||
'
- regime_compat: ' || COALESCE(c.REGIME_COMPAT, 'NULL') ||
'
- structure_confidence (0..1): ' || COALESCE(TO_VARCHAR(c.STRUCTURE_CONFIDENCE), 'NULL') ||
'
- level_significance (0..1): ' || COALESCE(TO_VARCHAR(c.LEVEL_SIGNIFICANCE), 'NULL') ||
'
- entry_zone: [' || COALESCE(TO_VARCHAR(c.ENTRY_ZONE_LOW), 'NULL') || ', ' || COALESCE(TO_VARCHAR(c.ENTRY_ZONE_HIGH), 'NULL') || ']
- price_invalidation_level: ' || COALESCE(TO_VARCHAR(c.PRICE_INVALIDATION_LEVEL), 'NULL') ||
'
- board_warning_flags: ' || COALESCE(TO_VARCHAR(c.BOARD_WARNING_FLAGS), '[]') ||
'
- absolute_exclusion_reasons: ' || COALESCE(TO_VARCHAR(c.ABSOLUTE_EXCLUSION_REASONS), '[]') ||
'

DECISION TASK:
Decide whether the structure is coherent enough for board consideration. Use the FULL range of verdicts. weak is NOT the default: most candidates whose structural geometry is intact should land in approve or acceptable, with weak reserved for material structural concerns and reject reserved for broken or disqualified structures.

Allowed verdict values (pick exactly ONE):
- approve    : Structure is clean and high-confidence. Use when structure_confidence is strong (typically >= 0.65), level_significance is reasonable, structural_state is FRESH or CONFIRMED, and there are no major structural conflicts.
- acceptable : Structure is recognizable and tradeable but imperfect. Use when structure_confidence is moderate (typically 0.45-0.65), or there are minor freshness or level concerns, but the structural geometry is intact. This is the MIDDLE band, distinct from approve and weak. Default to acceptable when in doubt rather than to weak.
- weak       : Structure is intact but has MATERIAL freshness, level, or conflict concerns. Use when structural_state is STALE/WAITING with real time decay, OPPOSING_SETUP creates a real ambiguity that would change the trade plan, or level_significance is too low to support the family interpretation.
- reject     : Structure is BROKEN, INVALIDATED, or DISQUALIFIED. Use when absolute_exclusion_reasons is non-empty, structural_state indicates breakdown, or structure_confidence is very low (< 0.30) AND family interpretation is incoherent.

Critical scoping rules (the structure agent owns ONLY structure):
- WEAK_TRUST_LABEL is a HISTORY concern, NOT a structure concern. Ignore it for your verdict.
- REPEATED_REPITCH is a HISTORY concern, NOT a structure concern. Ignore it.
- RECENT_TERMINAL_TRADE_OUTCOME is a HISTORY concern. Ignore it.
- NOISY_CHOPPY_PRICE_ACTION is an OPPORTUNITY concern. Ignore it.
- DIRECTION_NOT_EXECUTABLE is an EXECUTION concern. Ignore it.
- If absolute_exclusion_reasons is non-empty -> reject.

Allowed primary_reason_code per verdict:
- approve    -> STRUCTURE_APPROVED
- acceptable -> STRUCTURE_ACCEPTABLE (preferred), or STRUCTURE_NOT_FRESH when the only issue is mild staleness
- weak       -> STRUCTURE_NOT_FRESH (material staleness), CONFLICTING_STRUCTURE, FAMILY_INTERPRETATION_WEAK
- reject     -> STRUCTURE_WEAK

Allowed secondary_reason_code values: any of the codes above, or null.

Output requirements:
Return ONLY a single JSON object. Do NOT wrap in markdown fences. Do NOT include any other text. Start with { and end with }.

The JSON object must have exactly these keys:
{
  "verdict": "<one of: approve, acceptable, weak, reject>",
  "primary_reason_code": "<one of the allowed primary codes for that verdict>",
  "secondary_reason_code": "<allowed code or null>",
  "confidence": <float between 0.0 and 1.0>,
  "rationale_text": "<one or two sentences explaining your decision>",
  "concern_flags": [<optional array of short labels you noticed, may be empty>]
}'
            AS PROMPT_TEXT
        FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT c
        WHERE c.RUN_ID = :v_run_id
    ),
    cortexed AS (
        SELECT
            p.*,
            SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            cx.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(cx.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT,
            TRY_PARSE_JSON(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(cx.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', ''))) AS CORTEX_JSON
        FROM cortexed cx
    ),
    validated AS (
        SELECT
            cl.*,
            cl.CORTEX_JSON:verdict::STRING                AS C_VERDICT,
            cl.CORTEX_JSON:primary_reason_code::STRING    AS C_PRC,
            -- Soft-discard secondary_reason_code if it is JSON null or not in
            -- the allowed list. The primary code is the load-bearing audit
            -- field; the secondary is supplementary, so an invalid value
            -- should be nulled rather than invalidate the whole verdict.
            CASE
                WHEN cl.CORTEX_JSON:secondary_reason_code IS NULL
                  OR IS_NULL_VALUE(cl.CORTEX_JSON:secondary_reason_code)
                THEN NULL
                WHEN cl.CORTEX_JSON:secondary_reason_code::STRING IN (
                    'STRUCTURE_APPROVED','STRUCTURE_ACCEPTABLE','STRUCTURE_NOT_FRESH','STRUCTURE_WEAK',
                    'FAMILY_INTERPRETATION_WEAK','CONFLICTING_STRUCTURE'
                ) THEN cl.CORTEX_JSON:secondary_reason_code::STRING
                ELSE NULL
            END AS C_SRC,
            -- Soft-default confidence: if missing or out of range, fall back
            -- to a neutral 0.5 instead of invalidating the whole verdict.
            CASE
                WHEN TRY_TO_DOUBLE(cl.CORTEX_JSON:confidence::STRING) BETWEEN 0.0 AND 1.0
                THEN TRY_TO_DOUBLE(cl.CORTEX_JSON:confidence::STRING)
                ELSE 0.5
            END AS C_CONF,
            cl.CORTEX_JSON:rationale_text::STRING         AS C_RAT,
            cl.CORTEX_JSON:concern_flags                  AS C_FLAGS,
            -- Phase 3 Step 3: refined STRUCTURE vocabulary. 'acceptable' is the
            -- new middle band; old prompt's three-bin output is intentionally
            -- replaced. If Cortex emits a stale verbiage from the prior prompt
            -- (e.g. just "approve|weak|reject"), the new word "approve|reject"
            -- still validates and "acceptable" is added; legacy "weak" remains
            -- valid. Soft-validation: any drift falls back deterministically
            -- and surfaces via cortex_validation_passed=false.
            (cl.CORTEX_JSON IS NOT NULL
             AND NOT IS_NULL_VALUE(cl.CORTEX_JSON:verdict)
             AND NOT IS_NULL_VALUE(cl.CORTEX_JSON:primary_reason_code)
             AND cl.CORTEX_JSON:verdict::STRING IN ('approve','acceptable','weak','reject')
             AND cl.CORTEX_JSON:primary_reason_code::STRING IN (
                 'STRUCTURE_APPROVED','STRUCTURE_ACCEPTABLE','STRUCTURE_NOT_FRESH','STRUCTURE_WEAK',
                 'FAMILY_INTERPRETATION_WEAK','CONFLICTING_STRUCTURE'
             )
            ) AS CORTEX_USABLE
        FROM cleaned cl
    )
    SELECT
        v.RUN_ID,
        v.CANDIDATE_ID,
        'STRUCTURE_AGENT',
        IFF(v.CORTEX_USABLE,
            v.C_VERDICT,
            -- Phase 3 Step 3: deterministic fallback now uses the refined
            -- four-bin vocabulary with 'acceptable' as the middle band.
            -- Defaults are calibrated so weak is no longer the catch-all.
            CASE
                WHEN ARRAY_SIZE(COALESCE(v.ABSOLUTE_EXCLUSION_REASONS, ARRAY_CONSTRUCT())) > 0 THEN 'reject'
                WHEN COALESCE(v.STRUCTURE_CONFIDENCE, 0) < 0.30 THEN 'reject'
                WHEN COALESCE(v.STRUCTURE_CONFIDENCE, 0) < 0.45
                  OR (ARRAY_CONTAINS('OPPOSING_SETUP'::VARIANT, v.BOARD_WARNING_FLAGS)
                      AND COALESCE(v.STRUCTURE_CONFIDENCE, 0) < 0.55) THEN 'weak'
                WHEN v.SETUP_STATUS IN ('STALE','WAITING')
                  OR COALESCE(v.STRUCTURE_CONFIDENCE, 0) < 0.65
                  OR COALESCE(v.LEVEL_SIGNIFICANCE, 0) < 0.30
                  OR ARRAY_CONTAINS('OPPOSING_SETUP'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'acceptable'
                ELSE 'approve'
            END),
        IFF(v.CORTEX_USABLE,
            v.C_PRC,
            CASE
                WHEN ARRAY_SIZE(COALESCE(v.ABSOLUTE_EXCLUSION_REASONS, ARRAY_CONSTRUCT())) > 0 THEN 'STRUCTURE_WEAK'
                WHEN COALESCE(v.STRUCTURE_CONFIDENCE, 0) < 0.30 THEN 'STRUCTURE_WEAK'
                WHEN COALESCE(v.STRUCTURE_CONFIDENCE, 0) < 0.45 THEN 'FAMILY_INTERPRETATION_WEAK'
                WHEN ARRAY_CONTAINS('OPPOSING_SETUP'::VARIANT, v.BOARD_WARNING_FLAGS)
                  AND COALESCE(v.STRUCTURE_CONFIDENCE, 0) < 0.55 THEN 'CONFLICTING_STRUCTURE'
                WHEN v.SETUP_STATUS IN ('STALE','WAITING') THEN 'STRUCTURE_NOT_FRESH'
                WHEN COALESCE(v.STRUCTURE_CONFIDENCE, 0) < 0.65
                  OR COALESCE(v.LEVEL_SIGNIFICANCE, 0) < 0.30
                  OR ARRAY_CONTAINS('OPPOSING_SETUP'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'STRUCTURE_ACCEPTABLE'
                ELSE 'STRUCTURE_APPROVED'
            END),
        IFF(v.CORTEX_USABLE,
            v.C_SRC,
            IFF(COALESCE(v.LEVEL_SIGNIFICANCE, 0) < 0.30, 'FAMILY_INTERPRETATION_WEAK', NULL)),
        IFF(v.CORTEX_USABLE,
            COALESCE(v.C_CONF, LEAST(0.95, GREATEST(0.35, COALESCE(v.STRUCTURE_CONFIDENCE, 0.5)))),
            LEAST(0.95, GREATEST(0.35, COALESCE(v.STRUCTURE_CONFIDENCE, 0.5)))),
        IFF(v.CORTEX_USABLE,
            COALESCE(v.C_RAT, 'Cortex structured response.'),
            'Deterministic fallback after Cortex output was missing or invalid. Structure review based on setup family, state, level geometry, freshness, and conflicts.'),
        OBJECT_CONSTRUCT(
            'verdict_schema',          'proposal_board_agent_v2_cortex',
            'mode',                    IFF(v.CORTEX_USABLE, 'cortex', 'deterministic_fallback'),
            'model',                   IFF(v.CORTEX_USABLE, :v_model_name, 'DETERMINISTIC_FALLBACK'),
            'cortex_parse_succeeded',  (v.CORTEX_JSON IS NOT NULL),
            'cortex_validation_passed',v.CORTEX_USABLE,
            'cortex_raw_text',         v.RAW_TEXT,
            'cortex_cleaned_text',     v.CLEANED_TEXT,
            'cortex_parsed_json',      v.CORTEX_JSON,
            'concern_flags',           IFF(v.CORTEX_USABLE,
                                          COALESCE(v.C_FLAGS, v.BOARD_WARNING_FLAGS),
                                          ARRAY_INSERT(COALESCE(v.BOARD_WARNING_FLAGS, ARRAY_CONSTRUCT()), 0, 'MODEL_FALLBACK_USED')),
            'supporting_evidence_keys',ARRAY_CONSTRUCT('STRUCTURE_CONFIDENCE','STRUCTURAL_STATE','LEVEL_SIGNIFICANCE','SETUP_STATUS','BOARD_WARNING_FLAGS'),
            'prompt_text',             v.PROMPT_TEXT
        )
    FROM validated v;

    -- ============================================================
    -- Specialist 2: OPPORTUNITY_QUALITY_AGENT (Cortex with deterministic fallback)
    -- ============================================================
    INSERT INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, VERDICT, PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE, CONFIDENCE, RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
    )
    WITH prompted AS (
        SELECT
            c.RUN_ID,
            c.CANDIDATE_ID,
            c.SYMBOL,
            c.DIRECTION,
            c.FAMILY,
            c.SETUP_DATE,
            c.SETUP_STATUS,
            c.ENTRY_ZONE_LOW,
            c.ENTRY_ZONE_HIGH,
            c.BOARD_WARNING_FLAGS,
            c.EVIDENCE_PAYLOAD_JSON:execution:current_price::FLOAT AS CURRENT_PRICE,
            c.EVIDENCE_PAYLOAD_JSON:execution:distance_from_entry_zone_pct::FLOAT AS ENTRY_DISTANCE_PCT,
            c.EVIDENCE_PAYLOAD_JSON:opportunity AS OPPORTUNITY_PAYLOAD,
'You are the Opportunity Quality Specialist on the MIP Agentic Proposal Board. Your job is to assess whether THIS moment is a good time to act on this candidate (timing, entry distance, recent bar quality), independent of structure, history, or execution feasibility.

CANDIDATE EVIDENCE:
- symbol: ' || COALESCE(c.SYMBOL, 'NULL') ||
'
- direction: ' || COALESCE(c.DIRECTION, 'NULL') ||
'
- setup_family: ' || COALESCE(c.FAMILY, 'NULL') ||
'
- setup_date: ' || COALESCE(TO_VARCHAR(c.SETUP_DATE), 'NULL') ||
'
- setup_status: ' || COALESCE(c.SETUP_STATUS, 'NULL') ||
'
- entry_zone: [' || COALESCE(TO_VARCHAR(c.ENTRY_ZONE_LOW), 'NULL') || ', ' || COALESCE(TO_VARCHAR(c.ENTRY_ZONE_HIGH), 'NULL') || ']
- current_price: ' || COALESCE(TO_VARCHAR(c.EVIDENCE_PAYLOAD_JSON:execution:current_price), 'NULL') ||
'
- distance_from_entry_zone_pct: ' || COALESCE(TO_VARCHAR(c.EVIDENCE_PAYLOAD_JSON:execution:distance_from_entry_zone_pct), 'NULL') ||
'
- opportunity_payload: ' || COALESCE(TO_VARCHAR(c.EVIDENCE_PAYLOAD_JSON:opportunity), 'null') ||
'
- board_warning_flags: ' || COALESCE(TO_VARCHAR(c.BOARD_WARNING_FLAGS), '[]') ||
'

DECISION TASK:
Decide how attractive THIS opportunity is right now, given freshness, distance to entry zone, and recent bar quality. You are NOT judging the structure itself or the historical evidence; only the opportunity at the current moment. Use the FULL range of verdicts: attractive remains selective, acceptable is the new middle band, watch is for valid-but-not-actionable cases, weak is for marginal opportunities, and reject is for clearly unattractive or structurally overextended opportunities.

Allowed verdict values (pick exactly ONE):
- attractive : Opportunity is selectively strong RIGHT NOW. Price is in or near the entry zone, recent bars are constructive, and the timing supports action. Use selectively.
- acceptable : Opportunity is workable but not standout. Use for setups where freshness, entry distance, or bar quality is fine but nothing is exceptional. This is the MIDDLE band; default to acceptable rather than to weak when in doubt.
- watch      : Opportunity is valid but NOT actionable now. Typical cases: price is too far from the entry zone (waiting for pullback), setup is stale and needs a re-trigger, or the opportunity needs a clearer signal before acting.
- weak       : Opportunity is marginal. Poor expectancy, choppy price action that meaningfully degrades the entry, pullback too shallow, or reversal too early to act on.
- reject     : Opportunity is clearly unattractive or structurally overextended (price has run too far past the entry zone with no realistic re-entry plan, or the bar quality is so bad the opportunity should not be on the slate at all).

Critical scoping rules (the opportunity agent owns ONLY opportunity timing and quality):
- WEAK_TRUST_LABEL is a HISTORY concern. Ignore it.
- REPEATED_REPITCH and RECENT_TERMINAL_TRADE_OUTCOME are HISTORY concerns. Ignore them.
- GAP_RISK_HIGH and SIZE_REDUCE_REQUIRED are EXECUTION concerns. Ignore them.
- DIRECTION_NOT_EXECUTABLE is an EXECUTION concern. Ignore it.

Calibration rules to apply:
- If board_warning_flags contains NOISY_CHOPPY_PRICE_ACTION, DEFAULT to acceptable. Only emit attractive when price is currently in the entry zone AND there is strong confluence (e.g. multiple supportive cues in opportunity_payload). Emit weak when the noise materially degrades the entry.
- If price is far from the entry zone (PRICE_DISTANCE_FROM_ENTRY_ZONE flag, or distance_from_entry_zone_pct is large), prefer watch with OPPORTUNITY_WAIT_FOR_ENTRY rather than weak.
- If SETUP_STATUS is STALE or WAITING, prefer watch (TREND_STALE) over weak unless the opportunity is genuinely poor.

Allowed primary_reason_code per verdict:
- attractive -> OPPORTUNITY_ATTRACTIVE
- acceptable -> OPPORTUNITY_ACCEPTABLE
- watch      -> OPPORTUNITY_WATCH, OPPORTUNITY_WAIT_FOR_ENTRY (preferred when waiting for pullback), TREND_STALE, TOO_EXTENDED
- weak       -> OPPORTUNITY_WEAK (preferred), PULLBACK_TOO_WEAK, REVERSAL_TOO_EARLY, NOISY_CHOPPY_PRICE_ACTION
- reject     -> OPPORTUNITY_WEAK (strong form for clearly unattractive), or TOO_EXTENDED (for extreme overextension with no realistic re-entry)

Allowed secondary_reason_code values: any of the codes above, or null.

Output requirements:
Return ONLY a single JSON object. Do NOT wrap in markdown fences. Do NOT include any other text. Start with { and end with }.

The JSON object must have exactly these keys:
{
  "verdict": "<one of: attractive, acceptable, watch, weak, reject>",
  "primary_reason_code": "<one of the allowed primary codes for that verdict>",
  "secondary_reason_code": "<allowed code or null>",
  "confidence": <float between 0.0 and 1.0>,
  "rationale_text": "<one or two sentences explaining your decision>",
  "concern_flags": [<optional array of short labels you noticed, may be empty>]
}'
            AS PROMPT_TEXT
        FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT c
        WHERE c.RUN_ID = :v_run_id
    ),
    cortexed AS (
        SELECT p.*,
               SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            cx.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(cx.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT,
            TRY_PARSE_JSON(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(cx.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', ''))) AS CORTEX_JSON
        FROM cortexed cx
    ),
    validated AS (
        SELECT
            cl.*,
            cl.CORTEX_JSON:verdict::STRING                AS C_VERDICT,
            cl.CORTEX_JSON:primary_reason_code::STRING    AS C_PRC,
            CASE
                WHEN cl.CORTEX_JSON:secondary_reason_code IS NULL
                  OR IS_NULL_VALUE(cl.CORTEX_JSON:secondary_reason_code)
                THEN NULL
                WHEN cl.CORTEX_JSON:secondary_reason_code::STRING IN (
                    'OPPORTUNITY_ATTRACTIVE','OPPORTUNITY_ACCEPTABLE','OPPORTUNITY_WATCH','OPPORTUNITY_WAIT_FOR_ENTRY',
                    'OPPORTUNITY_WEAK','PULLBACK_TOO_WEAK',
                    'REVERSAL_TOO_EARLY','TREND_STALE','TOO_EXTENDED','NOISY_CHOPPY_PRICE_ACTION'
                ) THEN cl.CORTEX_JSON:secondary_reason_code::STRING
                ELSE NULL
            END AS C_SRC,
            CASE
                WHEN TRY_TO_DOUBLE(cl.CORTEX_JSON:confidence::STRING) BETWEEN 0.0 AND 1.0
                THEN TRY_TO_DOUBLE(cl.CORTEX_JSON:confidence::STRING)
                ELSE 0.5
            END AS C_CONF,
            cl.CORTEX_JSON:rationale_text::STRING         AS C_RAT,
            cl.CORTEX_JSON:concern_flags                  AS C_FLAGS,
            -- Phase 3 Step 3: refined OPPORTUNITY vocabulary with the new
            -- 'acceptable' middle band and an explicit 'reject' verdict.
            -- Soft-validation: drift falls back deterministically and is
            -- visible via cortex_validation_passed=false.
            (cl.CORTEX_JSON IS NOT NULL
             AND NOT IS_NULL_VALUE(cl.CORTEX_JSON:verdict)
             AND NOT IS_NULL_VALUE(cl.CORTEX_JSON:primary_reason_code)
             AND cl.CORTEX_JSON:verdict::STRING IN ('attractive','acceptable','watch','weak','reject')
             AND cl.CORTEX_JSON:primary_reason_code::STRING IN (
                 'OPPORTUNITY_ATTRACTIVE','OPPORTUNITY_ACCEPTABLE','OPPORTUNITY_WATCH','OPPORTUNITY_WAIT_FOR_ENTRY',
                 'OPPORTUNITY_WEAK','PULLBACK_TOO_WEAK',
                 'REVERSAL_TOO_EARLY','TREND_STALE','TOO_EXTENDED','NOISY_CHOPPY_PRICE_ACTION'
             )
            ) AS CORTEX_USABLE
        FROM cleaned cl
    )
    SELECT
        v.RUN_ID,
        v.CANDIDATE_ID,
        'OPPORTUNITY_QUALITY_AGENT',
        IFF(v.CORTEX_USABLE,
            v.C_VERDICT,
            -- Phase 3 Step 3: deterministic fallback emits the refined
            -- five-bin vocabulary. Per amendment 3, NOISY_CHOPPY_PRICE_ACTION
            -- now defaults to acceptable (not weak); Cortex can still upgrade
            -- to attractive when in-zone with strong confluence, or downgrade
            -- to weak/reject when noise is materially destructive.
            CASE
                WHEN ARRAY_CONTAINS('PRICE_DISTANCE_FROM_ENTRY_ZONE'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'watch'
                WHEN v.SETUP_STATUS IN ('STALE','WAITING') THEN 'watch'
                WHEN ARRAY_CONTAINS('NOISY_CHOPPY_PRICE_ACTION'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'acceptable'
                ELSE 'attractive'
            END),
        IFF(v.CORTEX_USABLE,
            v.C_PRC,
            CASE
                WHEN ARRAY_CONTAINS('PRICE_DISTANCE_FROM_ENTRY_ZONE'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'OPPORTUNITY_WAIT_FOR_ENTRY'
                WHEN v.SETUP_STATUS IN ('STALE','WAITING') THEN 'TREND_STALE'
                WHEN ARRAY_CONTAINS('NOISY_CHOPPY_PRICE_ACTION'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'OPPORTUNITY_ACCEPTABLE'
                ELSE 'OPPORTUNITY_ATTRACTIVE'
            END),
        IFF(v.CORTEX_USABLE, v.C_SRC, NULL),
        IFF(v.CORTEX_USABLE,
            COALESCE(v.C_CONF, 0.65),
            CASE
                WHEN v.SETUP_STATUS IN ('STALE','WAITING') THEN 0.55
                WHEN ARRAY_CONTAINS('PRICE_DISTANCE_FROM_ENTRY_ZONE'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 0.50
                ELSE 0.75
            END),
        IFF(v.CORTEX_USABLE,
            COALESCE(v.C_RAT, 'Cortex structured response.'),
            'Deterministic fallback after Cortex output was missing or invalid. Opportunity review based on setup freshness, entry distance, and bar-quality warning flags.'),
        OBJECT_CONSTRUCT(
            'verdict_schema',          'proposal_board_agent_v2_cortex',
            'mode',                    IFF(v.CORTEX_USABLE, 'cortex', 'deterministic_fallback'),
            'model',                   IFF(v.CORTEX_USABLE, :v_model_name, 'DETERMINISTIC_FALLBACK'),
            'cortex_parse_succeeded',  (v.CORTEX_JSON IS NOT NULL),
            'cortex_validation_passed',v.CORTEX_USABLE,
            'cortex_raw_text',         v.RAW_TEXT,
            'cortex_cleaned_text',     v.CLEANED_TEXT,
            'cortex_parsed_json',      v.CORTEX_JSON,
            'concern_flags',           IFF(v.CORTEX_USABLE,
                                          COALESCE(v.C_FLAGS, v.BOARD_WARNING_FLAGS),
                                          ARRAY_INSERT(COALESCE(v.BOARD_WARNING_FLAGS, ARRAY_CONSTRUCT()), 0, 'MODEL_FALLBACK_USED')),
            'supporting_evidence_keys',ARRAY_CONSTRUCT('SETUP_STATUS','DISTANCE_FROM_ENTRY_ZONE','BOARD_WARNING_FLAGS'),
            'prompt_text',             v.PROMPT_TEXT
        )
    FROM validated v;

    -- ============================================================
    -- Specialist 3: HISTORICAL_EVIDENCE_AGENT (Cortex with deterministic fallback)
    -- ============================================================
    INSERT INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, VERDICT, PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE, CONFIDENCE, RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
    )
    WITH prompted AS (
        SELECT
            c.RUN_ID,
            c.CANDIDATE_ID,
            c.SYMBOL,
            c.FAMILY,
            c.TRUST_LABEL,
            c.MEANINGFUL_HIT_RATE,
            c.PATH_SURVIVAL_HIT_RATE,
            c.MFE_MAE_RATIO,
            c.BOARD_WARNING_FLAGS,
            c.RECENT_TRADE_FLAGS_JSON,
            c.EVIDENCE_PAYLOAD_JSON:history AS HISTORY_PAYLOAD,
'You are the Historical Evidence Specialist on the MIP Agentic Proposal Board. Your job is to assess whether the historical track record (family-level path stats, sample size, recent symbol memory) supports acting on this candidate. You do NOT judge structure, opportunity timing, or execution feasibility.

CANDIDATE EVIDENCE:
- symbol: ' || COALESCE(c.SYMBOL, 'NULL') ||
'
- setup_family: ' || COALESCE(c.FAMILY, 'NULL') ||
'
- trust_label: ' || COALESCE(c.TRUST_LABEL, 'NULL') ||
'
- meaningful_hit_rate (0..1): ' || COALESCE(TO_VARCHAR(c.MEANINGFUL_HIT_RATE), 'NULL') ||
'
- path_survival_hit_rate (0..1): ' || COALESCE(TO_VARCHAR(c.PATH_SURVIVAL_HIT_RATE), 'NULL') ||
'
- mfe_mae_ratio: ' || COALESCE(TO_VARCHAR(c.MFE_MAE_RATIO), 'NULL') ||
'
- board_warning_flags: ' || COALESCE(TO_VARCHAR(c.BOARD_WARNING_FLAGS), '[]') ||
'
- recent_trade_flags: ' || COALESCE(TO_VARCHAR(c.RECENT_TRADE_FLAGS_JSON), 'null') ||
'
- history_payload: ' || COALESCE(TO_VARCHAR(c.EVIDENCE_PAYLOAD_JSON:history), 'null') ||
'

DECISION TASK:
Decide whether the historical evidence supports acting on this candidate. Use the FULL range of verdicts. mixed must NOT be the default non-answer: a verdict of mixed REQUIRES you to name BOTH a positive and a negative side in your rationale. Low-sample but non-negative evidence belongs in sparse_but_acceptable, NOT in mixed.

Allowed verdict values (pick exactly ONE):
- evidence_supported   : Historical evidence supports the setup. Hit rates are reasonable, MFE/MAE is constructive, and there is no recent failure pattern.
- sparse_but_acceptable: Sample size is low (e.g. board_warning_flags includes SPARSE_HISTORY, or history_payload shows few prior occurrences) BUT the available evidence is not negative. This is the proper home for "we have not seen enough to be confident, but nothing in what we have seen is a red flag."
- mixed                : Evidence has BOTH positive and negative signals that meaningfully tension. You MUST name both sides in your rationale (e.g. "trusted family AND solid hit rate, BUT a recent failed re-pitch on this exact symbol"). If you cannot name a clear negative, the verdict should be evidence_supported or sparse_but_acceptable.
- weak                 : Historical evidence is materially weak. Poor path survival, low meaningful hit rate, repeated failures on the symbol, or path stats clearly below a workable bar.
- reject               : History is strongly negative or disqualifying. Use ONLY when TRUST_LABEL is REJECTED, or when there is a recent failure pattern with NO measured improvement and the family-level stats are also weak.

Critical scoping rules (the historical agent owns ONLY history; ignore unrelated flags):
- WEAK_TRUST_LABEL is your concern, BUT it ALONE should not destroy your verdict if measured evidence (hit rates, path survival, MFE/MAE) is otherwise acceptable. Combine WEAK_TRUST_LABEL with measured evidence; do not override measured evidence with the label alone.
- REPEATED_REPITCH is your concern. Use it as a real negative signal (typically pushes toward mixed or weak), but pair it with measured evidence rather than using it as a one-shot disqualifier.
- RECENT_FAILED_SYMBOL is your concern. A single recent failure with no improvement is a strong push toward mixed/weak; chronic recent failures are a push toward reject.
- DIRECTION_NOT_EXECUTABLE, NOISY_CHOPPY_PRICE_ACTION, GAP_RISK_HIGH, OPPOSING_SETUP are NOT history concerns. Ignore them.

Calibration rules to apply:
- Default to evidence_supported when measured stats are reasonable AND no recent failure is visible.
- Default to sparse_but_acceptable when sample is small AND nothing in the available evidence is negative. This replaces the prior pattern of defaulting to mixed.
- Use mixed only when you can name BOTH a clear positive and a clear negative.
- Use weak when measured stats are clearly below a workable bar.
- Use reject only for strong/disqualifying negatives.

Allowed primary_reason_code per verdict:
- evidence_supported    -> EVIDENCE_SUPPORTED
- sparse_but_acceptable -> EVIDENCE_SPARSE_BUT_ACCEPTABLE (preferred), or SAMPLE_SIZE_LOW
- mixed                 -> EVIDENCE_MIXED (preferred), RECENT_FAILED_SYMBOL, REPEATED_REPITCH, SAMPLE_SIZE_LOW
- weak                  -> EVIDENCE_WEAK (preferred), PATH_SURVIVAL_WEAK
- reject                -> EVIDENCE_WEAK

Allowed secondary_reason_code values: any of the codes above, or null.

Output requirements:
Return ONLY a single JSON object. Do NOT wrap in markdown fences. Do NOT include any other text. Start with { and end with }.

The JSON object must have exactly these keys:
{
  "verdict": "<one of: evidence_supported, sparse_but_acceptable, mixed, weak, reject>",
  "primary_reason_code": "<one of the allowed primary codes for that verdict>",
  "secondary_reason_code": "<allowed code or null>",
  "confidence": <float between 0.0 and 1.0>,
  "rationale_text": "<one or two sentences. If verdict is mixed, name BOTH a positive and a negative.>",
  "concern_flags": [<optional array of short labels you noticed, may be empty>]
}'
            AS PROMPT_TEXT
        FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT c
        WHERE c.RUN_ID = :v_run_id
    ),
    cortexed AS (
        SELECT p.*,
               SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            cx.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(cx.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT,
            TRY_PARSE_JSON(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(cx.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', ''))) AS CORTEX_JSON
        FROM cortexed cx
    ),
    validated AS (
        SELECT
            cl.*,
            cl.CORTEX_JSON:verdict::STRING                AS C_VERDICT,
            cl.CORTEX_JSON:primary_reason_code::STRING    AS C_PRC,
            CASE
                WHEN cl.CORTEX_JSON:secondary_reason_code IS NULL
                  OR IS_NULL_VALUE(cl.CORTEX_JSON:secondary_reason_code)
                THEN NULL
                WHEN cl.CORTEX_JSON:secondary_reason_code::STRING IN (
                    'EVIDENCE_SUPPORTED','EVIDENCE_SPARSE_BUT_ACCEPTABLE','EVIDENCE_MIXED','EVIDENCE_WEAK',
                    'RECENT_FAILED_SYMBOL','REPEATED_REPITCH','PATH_SURVIVAL_WEAK','SAMPLE_SIZE_LOW'
                ) THEN cl.CORTEX_JSON:secondary_reason_code::STRING
                ELSE NULL
            END AS C_SRC,
            CASE
                WHEN TRY_TO_DOUBLE(cl.CORTEX_JSON:confidence::STRING) BETWEEN 0.0 AND 1.0
                THEN TRY_TO_DOUBLE(cl.CORTEX_JSON:confidence::STRING)
                ELSE 0.5
            END AS C_CONF,
            cl.CORTEX_JSON:rationale_text::STRING         AS C_RAT,
            cl.CORTEX_JSON:concern_flags                  AS C_FLAGS,
            -- Phase 3 Step 3: refined HISTORICAL vocabulary. The new
            -- 'sparse_but_acceptable' verdict absorbs low-sample-but-not-
            -- negative evidence so it stops collapsing into 'mixed'.
            -- Soft-validation: drift falls back deterministically and is
            -- visible via cortex_validation_passed=false.
            (cl.CORTEX_JSON IS NOT NULL
             AND NOT IS_NULL_VALUE(cl.CORTEX_JSON:verdict)
             AND NOT IS_NULL_VALUE(cl.CORTEX_JSON:primary_reason_code)
             AND cl.CORTEX_JSON:verdict::STRING IN ('evidence_supported','sparse_but_acceptable','mixed','weak','reject')
             AND cl.CORTEX_JSON:primary_reason_code::STRING IN (
                 'EVIDENCE_SUPPORTED','EVIDENCE_SPARSE_BUT_ACCEPTABLE','EVIDENCE_MIXED','EVIDENCE_WEAK',
                 'RECENT_FAILED_SYMBOL','REPEATED_REPITCH','PATH_SURVIVAL_WEAK','SAMPLE_SIZE_LOW'
             )
            ) AS CORTEX_USABLE
        FROM cleaned cl
    )
    SELECT
        v.RUN_ID,
        v.CANDIDATE_ID,
        'HISTORICAL_EVIDENCE_AGENT',
        IFF(v.CORTEX_USABLE,
            v.C_VERDICT,
            -- Phase 3 Step 3: deterministic fallback now uses the refined
            -- five-bin vocabulary. SPARSE_HISTORY is now sparse_but_acceptable
            -- (was 'mixed'); WEAK_TRUST_LABEL is intentionally NOT a fallback
            -- override -- only TRUST_LABEL='REJECTED' triggers reject. Per
            -- amendment 2, label-only weakness should not destroy measured
            -- evidence.
            CASE
                WHEN v.TRUST_LABEL = 'REJECTED' THEN 'reject'
                WHEN COALESCE(v.MEANINGFUL_HIT_RATE, 0) > 0
                 AND COALESCE(v.MEANINGFUL_HIT_RATE, 1) < 0.30 THEN 'weak'
                WHEN ARRAY_CONTAINS('RECENT_FAILED_SYMBOL'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'mixed'
                WHEN ARRAY_CONTAINS('REPEATED_REPITCH'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'mixed'
                WHEN ARRAY_CONTAINS('SPARSE_HISTORY'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'sparse_but_acceptable'
                WHEN COALESCE(v.PATH_SURVIVAL_HIT_RATE, 1) < 0.30 THEN 'weak'
                WHEN COALESCE(v.MEANINGFUL_HIT_RATE, 1) < 0.45 THEN 'mixed'
                ELSE 'evidence_supported'
            END),
        IFF(v.CORTEX_USABLE,
            v.C_PRC,
            CASE
                WHEN v.TRUST_LABEL = 'REJECTED' THEN 'EVIDENCE_WEAK'
                WHEN COALESCE(v.MEANINGFUL_HIT_RATE, 0) > 0
                 AND COALESCE(v.MEANINGFUL_HIT_RATE, 1) < 0.30 THEN 'EVIDENCE_WEAK'
                WHEN ARRAY_CONTAINS('RECENT_FAILED_SYMBOL'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'RECENT_FAILED_SYMBOL'
                WHEN ARRAY_CONTAINS('REPEATED_REPITCH'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'REPEATED_REPITCH'
                WHEN ARRAY_CONTAINS('SPARSE_HISTORY'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'EVIDENCE_SPARSE_BUT_ACCEPTABLE'
                WHEN COALESCE(v.PATH_SURVIVAL_HIT_RATE, 1) < 0.30 THEN 'PATH_SURVIVAL_WEAK'
                WHEN COALESCE(v.MEANINGFUL_HIT_RATE, 1) < 0.45 THEN 'EVIDENCE_MIXED'
                ELSE 'EVIDENCE_SUPPORTED'
            END),
        IFF(v.CORTEX_USABLE,
            v.C_SRC,
            IFF(COALESCE(v.MFE_MAE_RATIO, 0) < 1.0, 'EVIDENCE_MIXED', NULL)),
        IFF(v.CORTEX_USABLE,
            COALESCE(v.C_CONF,
                CASE
                    WHEN v.TRUST_LABEL = 'TRUSTED'    THEN 0.80
                    WHEN v.TRUST_LABEL = 'PROVISIONAL' THEN 0.68
                    ELSE 0.55
                END),
            CASE
                WHEN v.TRUST_LABEL = 'TRUSTED'    THEN 0.80
                WHEN v.TRUST_LABEL = 'PROVISIONAL' THEN 0.68
                ELSE 0.55
            END),
        IFF(v.CORTEX_USABLE,
            COALESCE(v.C_RAT, 'Cortex structured response.'),
            'Deterministic fallback after Cortex output was missing or invalid. Historical review based on family trust, path stats, sample size, recent symbol memory, and re-pitch warnings.'),
        OBJECT_CONSTRUCT(
            'verdict_schema',          'proposal_board_agent_v2_cortex',
            'mode',                    IFF(v.CORTEX_USABLE, 'cortex', 'deterministic_fallback'),
            'model',                   IFF(v.CORTEX_USABLE, :v_model_name, 'DETERMINISTIC_FALLBACK'),
            'cortex_parse_succeeded',  (v.CORTEX_JSON IS NOT NULL),
            'cortex_validation_passed',v.CORTEX_USABLE,
            'cortex_raw_text',         v.RAW_TEXT,
            'cortex_cleaned_text',     v.CLEANED_TEXT,
            'cortex_parsed_json',      v.CORTEX_JSON,
            'concern_flags',           IFF(v.CORTEX_USABLE,
                                          COALESCE(v.C_FLAGS, v.BOARD_WARNING_FLAGS),
                                          ARRAY_INSERT(COALESCE(v.BOARD_WARNING_FLAGS, ARRAY_CONSTRUCT()), 0, 'MODEL_FALLBACK_USED')),
            'supporting_evidence_keys',ARRAY_CONSTRUCT('TRUST_LABEL','MEANINGFUL_HIT_RATE','PATH_SURVIVAL_HIT_RATE','MFE_MAE_RATIO','RECENT_TRADE_FLAGS_JSON'),
            'prompt_text',             v.PROMPT_TEXT
        )
    FROM validated v;

    -- ============================================================
    -- Specialist 4: RISK_EXECUTION_FEASIBILITY_AGENT (Cortex with deterministic fallback)
    -- ============================================================
    INSERT INTO MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, VERDICT, PRIMARY_REASON_CODE,
        SECONDARY_REASON_CODE, CONFIDENCE, RATIONALE_TEXT, STRUCTURED_OUTPUT_JSON
    )
    WITH prompted AS (
        SELECT
            c.RUN_ID,
            c.CANDIDATE_ID,
            c.SYMBOL,
            c.DIRECTION,
            c.ENTRY_ZONE_LOW,
            c.ENTRY_ZONE_HIGH,
            c.PRICE_INVALIDATION_LEVEL,
            c.RISK_CLASS,
            c.BOARD_WARNING_FLAGS,
            c.EVIDENCE_PAYLOAD_JSON:execution AS EXECUTION_PAYLOAD,
            c.EVIDENCE_PAYLOAD_JSON:history:gap_risk_contribution AS GAP_RISK_CONTRIB,
'You are the Risk and Execution Feasibility Specialist on the MIP Agentic Proposal Board. Your job is to assess whether the candidate is practically executable at proposal time given entry zone, invalidation distance, risk class, gap risk, and direction operability. Downstream live execution is the final execution authority; you only judge proposal-time feasibility.

CANDIDATE EVIDENCE:
- symbol: ' || COALESCE(c.SYMBOL, 'NULL') ||
'
- direction: ' || COALESCE(c.DIRECTION, 'NULL') ||
'
- entry_zone: [' || COALESCE(TO_VARCHAR(c.ENTRY_ZONE_LOW), 'NULL') || ', ' || COALESCE(TO_VARCHAR(c.ENTRY_ZONE_HIGH), 'NULL') || ']
- price_invalidation_level: ' || COALESCE(TO_VARCHAR(c.PRICE_INVALIDATION_LEVEL), 'NULL') ||
'
- risk_class: ' || COALESCE(c.RISK_CLASS, 'NULL') ||
'
- board_warning_flags: ' || COALESCE(TO_VARCHAR(c.BOARD_WARNING_FLAGS), '[]') ||
'
- execution_payload: ' || COALESCE(TO_VARCHAR(c.EVIDENCE_PAYLOAD_JSON:execution), 'null') ||
'
- gap_risk_contribution: ' || COALESCE(TO_VARCHAR(c.EVIDENCE_PAYLOAD_JSON:history:gap_risk_contribution), 'NULL') ||
'

DECISION TASK:
Decide whether this candidate is practically executable at proposal time.

Allowed verdict values (pick exactly ONE):
- executable        : Live-tradeable today at standard sizing. Default for clean longs and (when SHORT_LIVE_ENABLED is true) clean shorts.
- constrained       : Live-tradeable but only with sizing or gap-aware constraints. Use for HIGH or GAP_AWARE risk_class, INVALIDATION_TOO_NEAR/FAR, or any case where SIZE_REDUCE_REQUIRED applies.
- risk_unattractive : Tradeable but the risk/reward ratio is below threshold or invalidation is too tight relative to plausible target. Distinct from constrained: the structure is executable, the trade is just unappealing.
- research_only     : Direction is research-visible but not currently live-enabled by policy. Specifically: SHORT candidates when SHORT_LIVE_ENABLED is false, or any other direction-policy block. THIS IS NOT REJECT. The candidate is held for research, not failed.
- hard_block        : Truly impossible to execute. Reserved for instrument disabled, market halted, no liquidity, legal exclusion, or comparable hard-impossibility cases.

Critical distinctions you MUST get right:
- If board_warning_flags contains DIRECTION_NOT_EXECUTABLE because of a policy switch (e.g. SHORT_LIVE_ENABLED=false), the verdict is research_only. NOT reject. NOT hard_block.
- hard_block is reserved for OBJECTIVE execution impossibility (instrument disabled / halted / illiquid). It is NOT for poor risk/reward.
- risk_unattractive is for tradeable-but-bad-trade cases (poor R/R). It is NOT for policy blocks.

Allowed primary_reason_code values per verdict:
- executable        -> EXECUTABLE
- constrained       -> EXECUTION_CONSTRAINED, INVALIDATION_TOO_NEAR, INVALIDATION_TOO_FAR, GAP_RISK_HIGH, SIZE_REDUCE_REQUIRED, SHORT_HISTORY_NOT_OPERATIONAL
- risk_unattractive -> RISK_REWARD_UNATTRACTIVE (preferred), or INVALIDATION_TOO_NEAR if R/R is the driver
- research_only     -> EXECUTION_RESEARCH_ONLY (preferred), or DIRECTION_NOT_EXECUTABLE
- hard_block        -> EXECUTION_IMPRACTICAL

Allowed secondary_reason_code values: any of the codes above, or null.

Output requirements:
Return ONLY a single JSON object. Do NOT wrap in markdown fences. Do NOT include any other text. Start with { and end with }.

The JSON object must have exactly these keys:
{
  "verdict": "<one of: executable, constrained, risk_unattractive, research_only, hard_block>",
  "primary_reason_code": "<one of the allowed primary codes for that verdict>",
  "secondary_reason_code": "<allowed code or null>",
  "confidence": <float between 0.0 and 1.0>,
  "rationale_text": "<one or two sentences explaining your decision>",
  "concern_flags": [<optional array of short labels you noticed, may be empty>]
}'
            AS PROMPT_TEXT
        FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT c
        WHERE c.RUN_ID = :v_run_id
    ),
    cortexed AS (
        SELECT p.*,
               SNOWFLAKE.CORTEX.COMPLETE(:v_model_name, p.PROMPT_TEXT) AS RAW_TEXT
        FROM prompted p
    ),
    cleaned AS (
        SELECT
            cx.*,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(cx.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', '')) AS CLEANED_TEXT,
            TRY_PARSE_JSON(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(cx.RAW_TEXT, ''), '^[[:space:]]*```(json)?[[:space:]]*', ''), '[[:space:]]*```[[:space:]]*$', ''))) AS CORTEX_JSON
        FROM cortexed cx
    ),
    validated AS (
        SELECT
            cl.*,
            cl.CORTEX_JSON:verdict::STRING                AS C_VERDICT,
            cl.CORTEX_JSON:primary_reason_code::STRING    AS C_PRC,
            CASE
                WHEN cl.CORTEX_JSON:secondary_reason_code IS NULL
                  OR IS_NULL_VALUE(cl.CORTEX_JSON:secondary_reason_code)
                THEN NULL
                WHEN cl.CORTEX_JSON:secondary_reason_code::STRING IN (
                    'EXECUTABLE','EXECUTION_CONSTRAINED','EXECUTION_IMPRACTICAL',
                    'INVALIDATION_TOO_NEAR','INVALIDATION_TOO_FAR','GAP_RISK_HIGH',
                    'SIZE_REDUCE_REQUIRED','SHORT_HISTORY_NOT_OPERATIONAL','DIRECTION_NOT_EXECUTABLE',
                    'EXECUTION_RESEARCH_ONLY','RISK_REWARD_UNATTRACTIVE'
                ) THEN cl.CORTEX_JSON:secondary_reason_code::STRING
                ELSE NULL
            END AS C_SRC,
            CASE
                WHEN TRY_TO_DOUBLE(cl.CORTEX_JSON:confidence::STRING) BETWEEN 0.0 AND 1.0
                THEN TRY_TO_DOUBLE(cl.CORTEX_JSON:confidence::STRING)
                ELSE 0.5
            END AS C_CONF,
            cl.CORTEX_JSON:rationale_text::STRING         AS C_RAT,
            cl.CORTEX_JSON:concern_flags                  AS C_FLAGS,
            -- Phase 3 Step 2: refined RISK vocabulary. Old "reject" verdict
            -- is intentionally NOT in the new allowlist. If Cortex emits it
            -- (drift from prior prompt), CORTEX_USABLE goes false and the
            -- deterministic fallback re-derives a refined verdict. The
            -- cortex_validation_passed flag in STRUCTURED_OUTPUT_JSON makes
            -- this visible for diagnostics.
            (cl.CORTEX_JSON IS NOT NULL
             AND NOT IS_NULL_VALUE(cl.CORTEX_JSON:verdict)
             AND NOT IS_NULL_VALUE(cl.CORTEX_JSON:primary_reason_code)
             AND cl.CORTEX_JSON:verdict::STRING IN ('executable','constrained','risk_unattractive','research_only','hard_block')
             AND cl.CORTEX_JSON:primary_reason_code::STRING IN (
                 'EXECUTABLE','EXECUTION_CONSTRAINED','EXECUTION_IMPRACTICAL',
                 'INVALIDATION_TOO_NEAR','INVALIDATION_TOO_FAR','GAP_RISK_HIGH',
                 'SIZE_REDUCE_REQUIRED','SHORT_HISTORY_NOT_OPERATIONAL','DIRECTION_NOT_EXECUTABLE',
                 'EXECUTION_RESEARCH_ONLY','RISK_REWARD_UNATTRACTIVE'
             )
            ) AS CORTEX_USABLE
        FROM cleaned cl
    )
    SELECT
        v.RUN_ID,
        v.CANDIDATE_ID,
        'RISK_EXECUTION_FEASIBILITY_AGENT',
        IFF(v.CORTEX_USABLE,
            v.C_VERDICT,
            -- Phase 3 Step 2: deterministic fallback emits refined vocab.
            -- DIRECTION_NOT_EXECUTABLE is a POLICY block (e.g. SHORT with
            -- SHORT_LIVE_ENABLED=false), not an execution failure, so it
            -- maps to research_only -- not the legacy 'reject'.
            -- True hard_block is reserved for instrument halted / disabled,
            -- which the deterministic fallback does not currently surface
            -- (no upstream signal yet); Cortex is the path for hard_block.
            CASE
                WHEN ARRAY_CONTAINS('DIRECTION_NOT_EXECUTABLE'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'research_only'
                WHEN v.RISK_CLASS IN ('HIGH','GAP_AWARE')
                  OR COALESCE(TRY_TO_DOUBLE(v.GAP_RISK_CONTRIB::STRING), 0) >= 0.35 THEN 'constrained'
                ELSE 'executable'
            END),
        IFF(v.CORTEX_USABLE,
            v.C_PRC,
            CASE
                WHEN ARRAY_CONTAINS('DIRECTION_NOT_EXECUTABLE'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 'EXECUTION_RESEARCH_ONLY'
                WHEN v.RISK_CLASS = 'GAP_AWARE'
                  OR COALESCE(TRY_TO_DOUBLE(v.GAP_RISK_CONTRIB::STRING), 0) >= 0.35 THEN 'GAP_RISK_HIGH'
                WHEN v.RISK_CLASS = 'HIGH' THEN 'SIZE_REDUCE_REQUIRED'
                ELSE 'EXECUTABLE'
            END),
        IFF(v.CORTEX_USABLE,
            v.C_SRC,
            IFF(v.RISK_CLASS IN ('HIGH','GAP_AWARE'), 'EXECUTION_CONSTRAINED', NULL)),
        IFF(v.CORTEX_USABLE,
            COALESCE(v.C_CONF,
                CASE
                    WHEN ARRAY_CONTAINS('DIRECTION_NOT_EXECUTABLE'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 0.90
                    WHEN v.RISK_CLASS IN ('HIGH','GAP_AWARE') THEN 0.72
                    ELSE 0.80
                END),
            CASE
                WHEN ARRAY_CONTAINS('DIRECTION_NOT_EXECUTABLE'::VARIANT, v.BOARD_WARNING_FLAGS) THEN 0.90
                WHEN v.RISK_CLASS IN ('HIGH','GAP_AWARE') THEN 0.72
                ELSE 0.80
            END),
        IFF(v.CORTEX_USABLE,
            COALESCE(v.C_RAT, 'Cortex structured response.'),
            'Deterministic fallback after Cortex output was missing or invalid. Proposal-time feasibility review based on entry zone, invalidation, risk class, and direction.'),
        OBJECT_CONSTRUCT(
            'verdict_schema',          'proposal_board_agent_v2_cortex',
            'mode',                    IFF(v.CORTEX_USABLE, 'cortex', 'deterministic_fallback'),
            'model',                   IFF(v.CORTEX_USABLE, :v_model_name, 'DETERMINISTIC_FALLBACK'),
            'cortex_parse_succeeded',  (v.CORTEX_JSON IS NOT NULL),
            'cortex_validation_passed',v.CORTEX_USABLE,
            'cortex_raw_text',         v.RAW_TEXT,
            'cortex_cleaned_text',     v.CLEANED_TEXT,
            'cortex_parsed_json',      v.CORTEX_JSON,
            'concern_flags',           IFF(v.CORTEX_USABLE,
                                          COALESCE(v.C_FLAGS, v.BOARD_WARNING_FLAGS),
                                          ARRAY_INSERT(COALESCE(v.BOARD_WARNING_FLAGS, ARRAY_CONSTRUCT()), 0, 'MODEL_FALLBACK_USED')),
            'supporting_evidence_keys',ARRAY_CONSTRUCT('ENTRY_ZONE_LOW','ENTRY_ZONE_HIGH','PRICE_INVALIDATION_LEVEL','RISK_CLASS','BOARD_WARNING_FLAGS'),
            'prompt_text',             v.PROMPT_TEXT
        )
    FROM validated v;

    -- ============================================================
    -- Reason-code validation (specialists).
    --
    -- Phase 3 Step 5: hardens the existing IS_ACTIVE check by adding
    -- category-correct enforcement. Any specialist that emits a
    -- PRIMARY_REASON_CODE outside its expected REASON_CATEGORY now
    -- fails the run, not just unknown / inactive codes.
    --
    --   STRUCTURE_AGENT                  -> REASON_CATEGORY = 'STRUCTURE'
    --   OPPORTUNITY_QUALITY_AGENT        -> REASON_CATEGORY = 'OPPORTUNITY'
    --   HISTORICAL_EVIDENCE_AGENT        -> REASON_CATEGORY = 'HISTORY'
    --   RISK_EXECUTION_FEASIBILITY_AGENT -> REASON_CATEGORY = 'RISK_EXECUTION'
    --
    -- Both the unknown-code check and the category-mismatch check
    -- write into PROPOSAL_BOARD_OUTPUT_ERROR (with distinct ERROR_TYPE
    -- values). The single v_invalid_count guard catches both as
    -- already-hard failures.
    -- ============================================================
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

    -- Phase 3 Step 5: category-correct hard validation. Any active
    -- code in the wrong category for the emitting agent fails the run.
    INSERT INTO MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, ERROR_TYPE, RAW_OUTPUT_JSON, ERROR_MESSAGE
    )
    SELECT
        o.RUN_ID,
        o.CANDIDATE_ID,
        o.AGENT_NAME,
        'CATEGORY_MISMATCH',
        o.STRUCTURED_OUTPUT_JSON,
        'Agent output used a primary reason code from the wrong category: '
            || o.PRIMARY_REASON_CODE
            || ' (category=' || rc.REASON_CATEGORY
            || ', expected='
            || CASE o.AGENT_NAME
                   WHEN 'STRUCTURE_AGENT'                  THEN 'STRUCTURE'
                   WHEN 'OPPORTUNITY_QUALITY_AGENT'        THEN 'OPPORTUNITY'
                   WHEN 'HISTORICAL_EVIDENCE_AGENT'        THEN 'HISTORY'
                   WHEN 'RISK_EXECUTION_FEASIBILITY_AGENT' THEN 'RISK_EXECUTION'
                   ELSE '<UNKNOWN_AGENT>'
               END
            || ')'
    FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME o
    JOIN MIP.APP.PROPOSAL_BOARD_REASON_CODE rc
      ON rc.REASON_CODE = o.PRIMARY_REASON_CODE
     AND rc.IS_ACTIVE = TRUE
    WHERE o.RUN_ID = :v_run_id
      AND rc.REASON_CATEGORY != CASE o.AGENT_NAME
                                    WHEN 'STRUCTURE_AGENT'                  THEN 'STRUCTURE'
                                    WHEN 'OPPORTUNITY_QUALITY_AGENT'        THEN 'OPPORTUNITY'
                                    WHEN 'HISTORICAL_EVIDENCE_AGENT'        THEN 'HISTORY'
                                    WHEN 'RISK_EXECUTION_FEASIBILITY_AGENT' THEN 'RISK_EXECUTION'
                                    ELSE '<UNKNOWN_AGENT>'
                                END;

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
            -- Phase 3 Step 3 compat shim: extend SUPPORT/CONCERN lists for
            -- the refined STRUCTURE/HISTORICAL/OPPORTUNITY vocabularies.
            --   * 'acceptable' (STRUCTURE, OPPORTUNITY) and
            --     'sparse_but_acceptable' (HISTORICAL) are positive middle
            --     bands -- they join SUPPORTIVE_COUNT so the disagreement
            --     signal stays meaningful, but they are intentionally NOT in
            --     CONCERN_COUNT (they are the new "not-a-concern" bin).
            --   * The new OPPORTUNITY 'reject' verdict already falls into
            --     CONCERN_COUNT via the existing 'reject' string (and into
            --     REJECT_COUNT in the chair's agent_summary below).
            -- Chair decision logic remains untouched.
            COUNT_IF(VERDICT IN (
                'approve','attractive','evidence_supported','executable',
                'acceptable','sparse_but_acceptable'
            )) AS SUPPORTIVE_COUNT,
            COUNT_IF(VERDICT IN (
                'reject','hard_block',
                'weak','constrained','mixed','watch',
                'risk_unattractive','research_only'
            )) AS CONCERN_COUNT
        FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME
        WHERE RUN_ID = :v_run_id
        GROUP BY RUN_ID, CANDIDATE_ID
    ) s
    WHERE s.SUPPORTIVE_COUNT > 0
      AND s.CONCERN_COUNT > 0;

    -- ============================================================
    -- Chair (orchestrator) verdict.
    --
    -- Phase 3 Step 4: full chair-matrix rewrite.
    --   * The chair is no longer driven by REJECT_COUNT / CONCERN_COUNT
    --     alone (which made it ~100% Risk-correlated and made WATCHLIST_ONLY
    --     dominate the reason-code distribution). It now reads each
    --     specialist verdict and the BOARD_WARNING_FLAGS directly and
    --     emits a specific chair reason code per branch.
    --   * Re-pitch and stale warnings get real policy effect via the
    --     compound REJECTs at Layer 1 (REJECT_REPEATED_REPITCH,
    --     REJECT_RECENT_FAILURE_NO_IMPROVEMENT, REJECT_STALE_WEAK_STRUCTURE).
    --   * REJECT verdicts are still authored into the orchestrator
    --     audit table; the existing publication INSERT below already
    --     filters publication to APPROVE / APPROVE_REDUCED so REJECT
    --     candidates never reach STRUCTURAL_TRADE_PROPOSALS (fork F2).
    --   * Same-symbol policy is unchanged (warning flag only).
    --   * Live-execution guardrails are unchanged.
    --   * Amendments honoured:
    --       1. OPPOSING_SETUP demotes to APPROVE_REDUCED unless combined
    --          with weak / ambiguous specialists, in which case it forces
    --          WATCH (Layer 3.3 vs Layer 4.2).
    --       2. WEAK_TRUST_LABEL alone does NOT block APPROVE if HIST is
    --          evidence_supported or sparse_but_acceptable (Layer 4.7
    --          guards the demotion accordingly).
    --       3. NOISY_CHOPPY_PRICE_ACTION caps Opportunity at acceptable
    --          via Layer 4.4, but if the OPP agent emits "attractive"
    --          (in-zone with strong confluence), the chair allows it
    --          through to clean APPROVE (Layer 5).
    --       4. Target distributions are sanity checks; the chair does
    --          not enforce quotas mechanically.
    --
    -- The compatibility shim in agent_summary above keeps the legacy
    -- SUPPORT_COUNT / CONCERN_COUNT / REJECT_COUNT counters honest for
    -- the rationale text; the chair matrix itself no longer reads them.
    -- ============================================================
    INSERT INTO MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT (
        RUN_ID, CANDIDATE_ID, FINAL_RANK, FINAL_VERDICT,
        PRIMARY_REASON_CODE, SECONDARY_REASON_CODE, FINAL_RATIONALE,
        WHY_SELECTED_OR_REJECTED, COMPARATIVE_REASONING_JSON
    )
    WITH agent_summary AS (
        SELECT
            RUN_ID,
            CANDIDATE_ID,
            -- Phase 3 Step 2 + Step 3 compat shim:
            --   * 'hard_block' (RISK)             -> REJECT_COUNT
            --   * 'research_only' (RISK)          -> CONCERN_COUNT
            --   * 'risk_unattractive' (RISK)      -> CONCERN_COUNT
            --   * 'acceptable' (STRUCT/OPP)       -> SUPPORT_COUNT (middle-band)
            --   * 'sparse_but_acceptable' (HIST)  -> SUPPORT_COUNT (middle-band)
            --   * OPPORTUNITY 'reject' verdict    -> REJECT_COUNT via the
            --     existing 'reject' bucket. The chair REJECT branch becomes
            --     reachable for opportunity / structure / history rejects too.
            -- Chair decision CASE (further down) is unchanged in Step 3.
            -- Step 4 will fold these labels into the chair matrix proper.
            COUNT_IF(VERDICT IN ('reject','hard_block')) AS REJECT_COUNT,
            COUNT_IF(VERDICT IN (
                'weak','mixed','watch','constrained',
                'risk_unattractive','research_only'
            )) AS CONCERN_COUNT,
            COUNT_IF(VERDICT IN (
                'approve','attractive','evidence_supported','executable',
                'acceptable','sparse_but_acceptable'
            )) AS SUPPORT_COUNT,
            AVG(CONFIDENCE) AS AVG_CONFIDENCE,
            ARRAY_AGG(PRIMARY_REASON_CODE) WITHIN GROUP (ORDER BY AGENT_NAME) AS REASON_CODES
        FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME
        WHERE RUN_ID = :v_run_id
        GROUP BY RUN_ID, CANDIDATE_ID
    ),
    -- Per-candidate per-agent breakdown so the chair text can
    -- name each specialist's verdict and primary reason code.
    agent_breakdown AS (
        SELECT
            RUN_ID,
            CANDIDATE_ID,
            MAX(IFF(AGENT_NAME = 'STRUCTURE_AGENT',                  VERDICT,             NULL)) AS STRUCT_V,
            MAX(IFF(AGENT_NAME = 'STRUCTURE_AGENT',                  PRIMARY_REASON_CODE, NULL)) AS STRUCT_PRC,
            MAX(IFF(AGENT_NAME = 'STRUCTURE_AGENT',                  SECONDARY_REASON_CODE, NULL)) AS STRUCT_SRC,
            MAX(IFF(AGENT_NAME = 'STRUCTURE_AGENT',                  CONFIDENCE,          NULL)) AS STRUCT_CONF,
            MAX(IFF(AGENT_NAME = 'STRUCTURE_AGENT',                  STRUCTURED_OUTPUT_JSON:mode::STRING, NULL)) AS STRUCT_MODE,
            MAX(IFF(AGENT_NAME = 'OPPORTUNITY_QUALITY_AGENT',        VERDICT,             NULL)) AS OPP_V,
            MAX(IFF(AGENT_NAME = 'OPPORTUNITY_QUALITY_AGENT',        PRIMARY_REASON_CODE, NULL)) AS OPP_PRC,
            MAX(IFF(AGENT_NAME = 'OPPORTUNITY_QUALITY_AGENT',        SECONDARY_REASON_CODE, NULL)) AS OPP_SRC,
            MAX(IFF(AGENT_NAME = 'OPPORTUNITY_QUALITY_AGENT',        CONFIDENCE,          NULL)) AS OPP_CONF,
            MAX(IFF(AGENT_NAME = 'OPPORTUNITY_QUALITY_AGENT',        STRUCTURED_OUTPUT_JSON:mode::STRING, NULL)) AS OPP_MODE,
            MAX(IFF(AGENT_NAME = 'HISTORICAL_EVIDENCE_AGENT',        VERDICT,             NULL)) AS HIST_V,
            MAX(IFF(AGENT_NAME = 'HISTORICAL_EVIDENCE_AGENT',        PRIMARY_REASON_CODE, NULL)) AS HIST_PRC,
            MAX(IFF(AGENT_NAME = 'HISTORICAL_EVIDENCE_AGENT',        SECONDARY_REASON_CODE, NULL)) AS HIST_SRC,
            MAX(IFF(AGENT_NAME = 'HISTORICAL_EVIDENCE_AGENT',        CONFIDENCE,          NULL)) AS HIST_CONF,
            MAX(IFF(AGENT_NAME = 'HISTORICAL_EVIDENCE_AGENT',        STRUCTURED_OUTPUT_JSON:mode::STRING, NULL)) AS HIST_MODE,
            MAX(IFF(AGENT_NAME = 'RISK_EXECUTION_FEASIBILITY_AGENT', VERDICT,             NULL)) AS RISK_V,
            MAX(IFF(AGENT_NAME = 'RISK_EXECUTION_FEASIBILITY_AGENT', PRIMARY_REASON_CODE, NULL)) AS RISK_PRC,
            MAX(IFF(AGENT_NAME = 'RISK_EXECUTION_FEASIBILITY_AGENT', SECONDARY_REASON_CODE, NULL)) AS RISK_SRC,
            MAX(IFF(AGENT_NAME = 'RISK_EXECUTION_FEASIBILITY_AGENT', CONFIDENCE,          NULL)) AS RISK_CONF,
            MAX(IFF(AGENT_NAME = 'RISK_EXECUTION_FEASIBILITY_AGENT', STRUCTURED_OUTPUT_JSON:mode::STRING, NULL)) AS RISK_MODE
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
            c.FAMILY,
            c.SETUP_DATE,
            c.SETUP_STATUS,
            c.STRUCTURAL_STATE,
            c.STRUCTURE_CONFIDENCE,
            c.LEVEL_SIGNIFICANCE,
            c.RISK_CLASS,
            c.BOARD_WARNING_FLAGS,
            a.REJECT_COUNT,
            a.CONCERN_COUNT,
            a.SUPPORT_COUNT,
            a.AVG_CONFIDENCE,
            a.REASON_CODES,
            b.STRUCT_V, b.STRUCT_PRC, b.STRUCT_SRC, b.STRUCT_CONF, b.STRUCT_MODE,
            b.OPP_V,    b.OPP_PRC,    b.OPP_SRC,    b.OPP_CONF,    b.OPP_MODE,
            b.HIST_V,   b.HIST_PRC,   b.HIST_SRC,   b.HIST_CONF,   b.HIST_MODE,
            b.RISK_V,   b.RISK_PRC,   b.RISK_SRC,   b.RISK_CONF,   b.RISK_MODE,
            -- Phase 3 Step 4 chair decision matrix.
            -- Layer ordering is significant; the FIRST matching branch wins.
            --   Layer 0: hard execution block (RISK = hard_block).
            --   Layer 1: compound REJECTs (re-pitch policy).
            --   Layer 2: short / direction policy WATCH (research_only).
            --   Layer 3: specialist-driven WATCH.
            --   Layer 4: warning-flag-driven APPROVE_REDUCED.
            --   Layer 5: clean APPROVE.
            -- The branch returns 'VERDICT|REASON_CODE'; the chair CTE
            -- below splits it into FINAL_VERDICT and PRIMARY_REASON_CODE
            -- so the matrix is evaluated exactly once per candidate.
            CASE
                -- ===== Layer 0: hard execution block =====
                WHEN b.RISK_V = 'hard_block'
                    THEN 'REJECT|REJECT_EXECUTION_HARD_BLOCK'

                -- ===== Layer 1: compound REJECTs (re-pitch policy) =====
                -- 1.1 stale + structurally weak/broken
                WHEN ARRAY_CONTAINS('STALE_BUT_NOT_EXPIRED'::VARIANT, c.BOARD_WARNING_FLAGS)
                     AND b.STRUCT_V IN ('weak','reject')
                    THEN 'REJECT|REJECT_STALE_WEAK_STRUCTURE'
                -- 1.2 recent terminal failure on this symbol AND nothing today
                --     materially improved (STRUCT not approve, OPP not attractive,
                --     HIST not on the supportive side).
                WHEN ARRAY_CONTAINS('RECENT_TERMINAL_TRADE_OUTCOME'::VARIANT, c.BOARD_WARNING_FLAGS)
                     AND b.HIST_V IN ('mixed','weak','reject')
                     AND b.STRUCT_V NOT IN ('approve')
                     AND b.OPP_V NOT IN ('attractive')
                    THEN 'REJECT|REJECT_RECENT_FAILURE_NO_IMPROVEMENT'
                -- 1.3 setup re-pitched many times AND every axis points away
                --     from a real edge (history not OK, opportunity not actionable,
                --     structure actively weak/broken). Conservative on purpose so
                --     STRUCT=acceptable doesn't trigger REJECT.
                WHEN ARRAY_CONTAINS('REPEATED_REPITCH'::VARIANT, c.BOARD_WARNING_FLAGS)
                     AND b.HIST_V IN ('mixed','weak','reject')
                     AND b.OPP_V IN ('weak','watch','reject')
                     AND b.STRUCT_V IN ('weak','reject')
                    THEN 'REJECT|REJECT_REPEATED_REPITCH'
                -- 1.4 opportunity hard-rejects (TOO_EXTENDED, REVERSAL_TOO_EARLY)
                WHEN b.OPP_V = 'reject'
                    THEN 'REJECT|REJECT_WEAK_OPPORTUNITY'

                -- ===== Layer 2: short / direction policy WATCH =====
                -- research_only is the policy block; the SHORT case is
                -- almost always SHORT_LIVE_ENABLED=false, the non-SHORT
                -- branch is forward-looking (e.g. FX_LIVE_ENABLED=false).
                WHEN b.RISK_V = 'research_only' AND c.DIRECTION = 'SHORT'
                    THEN 'WATCH|WATCH_SHORT_RESEARCH_ONLY'
                WHEN b.RISK_V = 'research_only'
                    THEN 'WATCH|WATCH_DIRECTION_NOT_EXECUTABLE'

                -- ===== Layer 3: specialist-driven WATCH =====
                -- 3.1 risk says "tradeable but R:R below threshold"
                WHEN b.RISK_V = 'risk_unattractive'
                    THEN 'WATCH|WATCH_RISK_REWARD_UNATTRACTIVE'
                -- 3.2 opportunity is OK but price isn't in the entry zone yet
                WHEN b.OPP_V = 'watch' AND b.OPP_PRC = 'OPPORTUNITY_WAIT_FOR_ENTRY'
                    THEN 'WATCH|WATCH_WAITING_FOR_ENTRY'
                -- 3.3 OPPOSING_SETUP combined with weak/ambiguous specialists
                --     (amendment 1). 'mixed' on HIST is intentionally NOT a
                --     WATCH trigger here -- mixed is the new informative middle
                --     band, not "ambiguous" in the calibration sense.
                WHEN ARRAY_CONTAINS('OPPOSING_SETUP'::VARIANT, c.BOARD_WARNING_FLAGS)
                     AND (b.STRUCT_V = 'weak' OR b.HIST_V IN ('weak','reject'))
                    THEN 'WATCH|WATCH_OPPOSING_SETUP'
                -- 3.4 opportunity is in the watch / weak band for any other reason
                WHEN b.OPP_V IN ('watch','weak')
                    THEN 'WATCH|WATCH_OPPORTUNITY_NOT_RIPE'
                -- 3.5 structure or history hard-reject (rare; no specific code today)
                WHEN b.STRUCT_V = 'reject' THEN 'WATCH|WATCHLIST_ONLY'
                WHEN b.HIST_V = 'reject'   THEN 'WATCH|WATCHLIST_ONLY'

                -- ===== Layer 4: warning-flag-driven APPROVE_REDUCED =====
                -- 4.1 risk constrained -> reduced sizing for the right reason
                WHEN b.RISK_V = 'constrained'
                    THEN 'APPROVE_REDUCED|APPROVED_REDUCED_RISK_CONSTRAINED'
                -- 4.2 OPPOSING_SETUP without a compound WATCH trigger: demote
                --     rather than force WATCH (amendment 1).
                WHEN ARRAY_CONTAINS('OPPOSING_SETUP'::VARIANT, c.BOARD_WARNING_FLAGS)
                    THEN 'APPROVE_REDUCED|APPROVED_REDUCED_OPPOSING_SETUP'
                -- 4.3 REPEATED_REPITCH without a compound REJECT: demote so the
                --     warning has real policy effect.
                WHEN ARRAY_CONTAINS('REPEATED_REPITCH'::VARIANT, c.BOARD_WARNING_FLAGS)
                    THEN 'APPROVE_REDUCED|APPROVED_REDUCED_REPEATED_REPITCH'
                -- 4.4 NOISY_CHOPPY caps OPP unless OPP=attractive overrides it
                --     (amendment 3).
                WHEN ARRAY_CONTAINS('NOISY_CHOPPY_PRICE_ACTION'::VARIANT, c.BOARD_WARNING_FLAGS)
                     AND b.OPP_V != 'attractive'
                    THEN 'APPROVE_REDUCED|APPROVED_REDUCED_NOISY_CHOPPY_PRICE_ACTION'
                -- 4.5 history is mixed (or weak that didn't compound REJECT)
                WHEN b.HIST_V IN ('mixed','weak')
                    THEN 'APPROVE_REDUCED|APPROVED_REDUCED_HISTORY_MIXED'
                -- 4.6 structure is in the weak band
                WHEN b.STRUCT_V = 'weak'
                    THEN 'APPROVE_REDUCED|APPROVED_REDUCED_BY_BOARD'
                -- 4.7 WEAK_TRUST_LABEL only blocks APPROVE if HIST is not
                --     supported (amendment 2).
                WHEN ARRAY_CONTAINS('WEAK_TRUST_LABEL'::VARIANT, c.BOARD_WARNING_FLAGS)
                     AND b.HIST_V NOT IN ('evidence_supported','sparse_but_acceptable')
                    THEN 'APPROVE_REDUCED|APPROVED_REDUCED_HISTORY_MIXED'

                -- ===== Layer 5: clean APPROVE =====
                WHEN c.DIRECTION = 'LONG'  THEN 'APPROVE|APPROVED_CLEAN_LONG_STRUCTURE'
                WHEN c.DIRECTION = 'SHORT' THEN 'APPROVE|APPROVED_CLEAN_SHORT_STRUCTURE'
                ELSE                            'APPROVE|APPROVED_BY_BOARD'
            END AS DECISION_LABEL,
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
        JOIN agent_breakdown b
          ON b.RUN_ID = c.RUN_ID
         AND b.CANDIDATE_ID = c.CANDIDATE_ID
        WHERE c.RUN_ID = :v_run_id
    ),
    -- Phase 3 Step 4: split DECISION_LABEL into FINAL_VERDICT and
    -- PRIMARY_REASON_CODE so the rest of the pipeline keeps the column
    -- names it expects without re-evaluating the matrix CASE twice.
    chair AS (
        SELECT
            d.*,
            SPLIT_PART(d.DECISION_LABEL, '|', 1) AS FINAL_VERDICT,
            SPLIT_PART(d.DECISION_LABEL, '|', 2) AS PRIMARY_REASON_CODE
        FROM decisions d
    ),
    -- Templated chair synthesis. The text is fully derived from
    -- the candidate snapshot + the four specialist verdicts so
    -- two different candidates always produce different rationale
    -- strings (modulo a literal coincidence of every input).
    composed AS (
        SELECT
            d.*,
            -- Compact "specialists" line used inside both rationale fields.
            'structure='   || COALESCE(d.STRUCT_V, 'NA') || '/' || COALESCE(d.STRUCT_PRC, 'NA') ||
            ', opportunity=' || COALESCE(d.OPP_V,    'NA') || '/' || COALESCE(d.OPP_PRC,    'NA') ||
            ', history='     || COALESCE(d.HIST_V,   'NA') || '/' || COALESCE(d.HIST_PRC,   'NA') ||
            ', risk='        || COALESCE(d.RISK_V,   'NA') || '/' || COALESCE(d.RISK_PRC,   'NA')
                AS SPECIALIST_LINE,
            -- Compact warning summary; empty string when none.
            IFF(d.BOARD_WARNING_FLAGS IS NULL OR ARRAY_SIZE(d.BOARD_WARNING_FLAGS) = 0,
                '',
                ' Warnings: ' || ARRAY_TO_STRING(d.BOARD_WARNING_FLAGS, ', ') || '.')
                AS WARNING_LINE,
            -- Mode-mix summary so audit text reflects whether the
            -- chair was synthesizing Cortex or fallback specialists.
            'modes=structure:' || COALESCE(d.STRUCT_MODE, 'NA') ||
            ', opportunity:'   || COALESCE(d.OPP_MODE,    'NA') ||
            ', history:'       || COALESCE(d.HIST_MODE,   'NA') ||
            ', risk:'          || COALESCE(d.RISK_MODE,   'NA')
                AS MODE_LINE
        FROM chair d
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
        -- FINAL_RATIONALE: deterministic chair synthesis. One sentence
        -- naming the candidate, the four specialist verdicts/reasons,
        -- the chair's verdict and reason, and any warning flags.
        SUBSTR(
            COALESCE(SYMBOL, 'NA') || ' ' || COALESCE(DIRECTION, 'NA') || ' ' ||
            COALESCE(FAMILY, 'NA') || ' setup_date=' || COALESCE(TO_VARCHAR(SETUP_DATE), 'NA') ||
            ' status=' || COALESCE(SETUP_STATUS, 'NA') ||
            '. Specialists: ' || SPECIALIST_LINE ||
            ' (avg_conf=' || COALESCE(TO_VARCHAR(ROUND(AVG_CONFIDENCE, 2)), 'NA') ||
            ', support/concern/reject=' || SUPPORT_COUNT || '/' || CONCERN_COUNT || '/' || REJECT_COUNT || ').' ||
            ' Chair: ' || FINAL_VERDICT || ' (' || PRIMARY_REASON_CODE ||
            COALESCE(' / ' || SECONDARY_REASON_CODE, '') || ').' ||
            WARNING_LINE,
            1, 4000
        ),
        -- WHY_SELECTED_OR_REJECTED: per-verdict differentiated text.
        -- Phase 3 Step 4: each branch leads with the chair's specific
        -- PRIMARY_REASON_CODE (which now drives the verdict, instead of
        -- the legacy support/concern/reject vote count) and follows with
        -- the specialist line for context.
        SUBSTR(
            CASE FINAL_VERDICT
                WHEN 'APPROVE' THEN
                    'Approved for publication: chair reason ' || PRIMARY_REASON_CODE || '. ' ||
                    'All four specialists are on the supportive side and no warning flag is strong enough to demote. ' ||
                    'Specialist line: ' || SPECIALIST_LINE || '.' || WARNING_LINE
                WHEN 'APPROVE_REDUCED' THEN
                    'Approved with reduced sizing: chair reason ' || PRIMARY_REASON_CODE || '. ' ||
                    'Specialists are net supportive but a concern (specialist verdict or warning flag) caps sizing. ' ||
                    'Specialist line: ' || SPECIALIST_LINE || '.' || WARNING_LINE ||
                    IFF(RISK_CLASS IN ('HIGH','GAP_AWARE'),
                        ' Sizing also constrained because risk_class=' || RISK_CLASS || '.',
                        '')
                WHEN 'WATCH' THEN
                    'Held as watchlist evidence only: chair reason ' || PRIMARY_REASON_CODE || '. ' ||
                    'Candidate is informative but not actionable today (policy block, opportunity not ripe, or specialist disagreement). ' ||
                    'Specialist line: ' || SPECIALIST_LINE || '.' || WARNING_LINE
                ELSE
                    'Rejected by board: chair reason ' || PRIMARY_REASON_CODE || '. ' ||
                    'The compound rejection rule for this branch fired -- see catalog description for ' ||
                    PRIMARY_REASON_CODE || '. Specialist line: ' || SPECIALIST_LINE || '.' || WARNING_LINE
            END,
            1, 4000
        ),
        -- COMPARATIVE_REASONING_JSON: extended audit object.
        OBJECT_CONSTRUCT(
            'support_count',  SUPPORT_COUNT,
            'concern_count',  CONCERN_COUNT,
            'reject_count',   REJECT_COUNT,
            'avg_confidence', AVG_CONFIDENCE,
            'reason_codes',   REASON_CODES,
            'warning_flags',  BOARD_WARNING_FLAGS,
            'specialist_breakdown', OBJECT_CONSTRUCT(
                'structure',   OBJECT_CONSTRUCT('verdict', STRUCT_V, 'primary_reason_code', STRUCT_PRC, 'secondary_reason_code', STRUCT_SRC, 'confidence', STRUCT_CONF, 'mode', STRUCT_MODE),
                'opportunity', OBJECT_CONSTRUCT('verdict', OPP_V,    'primary_reason_code', OPP_PRC,    'secondary_reason_code', OPP_SRC,    'confidence', OPP_CONF,    'mode', OPP_MODE),
                'history',     OBJECT_CONSTRUCT('verdict', HIST_V,   'primary_reason_code', HIST_PRC,   'secondary_reason_code', HIST_SRC,   'confidence', HIST_CONF,   'mode', HIST_MODE),
                'risk',        OBJECT_CONSTRUCT('verdict', RISK_V,   'primary_reason_code', RISK_PRC,   'secondary_reason_code', RISK_SRC,   'confidence', RISK_CONF,   'mode', RISK_MODE)
            ),
            'chair_template_version', 'phase3_step4_chair_matrix_v1',
            'mode_line', MODE_LINE
        )
    FROM composed;

    -- ============================================================
    -- Phase 3 Step 5: chair-side reason-code validation (hard).
    --
    -- Mirrors the specialist validation above but for the chair's
    -- PRIMARY_REASON_CODE in PROPOSAL_BOARD_ORCHESTRATOR_VERDICT.
    -- Pre-Step-5 the chair was never validated; with the Step 4
    -- chair matrix emitting many specific codes this is now in scope.
    -- Both checks write into PROPOSAL_BOARD_OUTPUT_ERROR and the
    -- existing v_invalid_count guard treats them as hard failures.
    -- ============================================================
    INSERT INTO MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, ERROR_TYPE, RAW_OUTPUT_JSON, ERROR_MESSAGE
    )
    SELECT
        v.RUN_ID,
        v.CANDIDATE_ID,
        'ORCHESTRATOR',
        'UNKNOWN_REASON_CODE',
        v.COMPARATIVE_REASONING_JSON,
        'Chair output used inactive or unknown primary reason code: ' || v.PRIMARY_REASON_CODE
    FROM MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT v
    LEFT JOIN MIP.APP.PROPOSAL_BOARD_REASON_CODE rc
      ON rc.REASON_CODE = v.PRIMARY_REASON_CODE
     AND rc.IS_ACTIVE = TRUE
    WHERE v.RUN_ID = :v_run_id
      AND rc.REASON_CODE IS NULL;

    INSERT INTO MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR (
        RUN_ID, CANDIDATE_ID, AGENT_NAME, ERROR_TYPE, RAW_OUTPUT_JSON, ERROR_MESSAGE
    )
    SELECT
        v.RUN_ID,
        v.CANDIDATE_ID,
        'ORCHESTRATOR',
        'CATEGORY_MISMATCH',
        v.COMPARATIVE_REASONING_JSON,
        'Chair output used a primary reason code from the wrong category: '
            || v.PRIMARY_REASON_CODE
            || ' (category=' || rc.REASON_CATEGORY
            || ', expected=CHAIR)'
    FROM MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT v
    JOIN MIP.APP.PROPOSAL_BOARD_REASON_CODE rc
      ON rc.REASON_CODE = v.PRIMARY_REASON_CODE
     AND rc.IS_ACTIVE = TRUE
    WHERE v.RUN_ID = :v_run_id
      AND rc.REASON_CATEGORY != 'CHAIR';

    SELECT COUNT(*) INTO :v_invalid_count
    FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR
    WHERE RUN_ID = :v_run_id;

    IF (v_invalid_count > 0) THEN
        UPDATE MIP.APP.PROPOSAL_BOARD_RUN
           SET RUN_STATUS = 'FAILED',
               FINISHED_AT = CURRENT_TIMESTAMP(),
               ERROR_JSON = OBJECT_CONSTRUCT('reason_code', 'SYSTEM_VALIDATION_FAILED', 'invalid_output_count', :v_invalid_count, 'phase', 'chair')
         WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'reason_code', 'SYSTEM_VALIDATION_FAILED',
            'invalid_output_count', :v_invalid_count,
            'phase', 'chair'
        );
    END IF;

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
    WITH blocked_candidates AS (
        -- Phase 3 Step 3 fix: pre-compute candidates that already have an
        -- active PROPOSED row (NULL-aware on portfolio) instead of putting
        -- the same logic inside a correlated NOT EXISTS in the main INSERT.
        -- The original NOT EXISTS form (`c.PORTFOLIO_ID IS NULL OR
        -- p.PORTFOLIO_ID = c.PORTFOLIO_ID OR p.PORTFOLIO_ID IS NULL`) mixes
        -- a NULL-on-outer correlation with an equality correlation inside a
        -- single OR, which Snowflake's correlated-subquery analyzer rejects
        -- with "Unsupported subquery type cannot be evaluated" depending on
        -- table statistics. The semantics here are identical: a candidate
        -- is blocked if any active PROPOSED row matches the same setup
        -- event on the same portfolio (or either side is portfolio-agnostic).
        SELECT DISTINCT c.CANDIDATE_ID
        FROM MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT c
        JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
          ON p.SETUP_EVENT_ID = c.SETUP_EVENT_ID
         AND p.STATUS = 'PROPOSED'
         AND (
              c.PORTFOLIO_ID IS NULL
              OR p.PORTFOLIO_ID = c.PORTFOLIO_ID
              OR p.PORTFOLIO_ID IS NULL
         )
        WHERE c.RUN_ID = :v_run_id
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
      -- Publication-time safety net. Same NULL-aware semantics as the
      -- snapshot dedup at line ~171, but materialised through the
      -- non-correlated `blocked_candidates` CTE above to avoid the
      -- "Unsupported subquery type" Snowflake compilation failure.
      AND fs.CANDIDATE_ID NOT IN (SELECT CANDIDATE_ID FROM blocked_candidates);

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
        -- Phase 3 Step 3 fix: SQLCODE/SQLERRM/SQLSTATE are Snowflake Scripting
        -- variables and must be referenced with the bind-variable colon prefix
        -- when used inside SQL DML. Without the colon, Snowflake interprets
        -- them as column identifiers and the EXCEPTION handler itself raises
        -- a fresh "invalid identifier" error, which masks the original error
        -- and leaves the run stuck in RUN_STATUS='RUNNING'.
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

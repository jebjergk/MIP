/* ================================================================
   v_board_calibration.sql
   Phase 2 / item 5 — Agentic proposal board calibration analytics.

   Read-only views over the board persistence tables and the
   structural setup outcomes table. Designed to accumulate evidence
   in the background as more board runs land, so we can detect
   Cortex regression vs the prior deterministic baseline and
   recalibrate verdict thresholds + reason-code semantics in later
   sprints (with a bumped POLICY_VERSION).

   No procedure, no writes. The board procedure (SP_RUN_PROPOSAL_BOARD)
   and the verdict thresholds it enforces are unaffected by these
   views; they exist to inform the *next* threshold revision.

   Lineage:
     PROPOSAL_BOARD_RUN              — MODEL_CONFIG_JSON.mode is the
                                       cortex vs deterministic-baseline
                                       slice key.
     PROPOSAL_BOARD_FINAL_SLATE      — one row per published candidate;
                                       carries the chair verdict.
     PROPOSAL_BOARD_CANDIDATE_SNAPSHOT — links to SETUP_EVENT_ID so we
                                       can join realised outcomes.
     PROPOSAL_BOARD_AGENT_OUTCOME    — per-agent verdict + Cortex audit
                                       fields (for the Cortex health
                                       view).
     PROPOSAL_BOARD_ORCHESTRATOR_VERDICT — chair primary_reason_code.
     STRUCTURAL_SETUP_OUTCOMES       — realised quality
                                       (DIRECTIONAL_SUCCESS,
                                        MEANINGFUL_MOVE_SUCCESS,
                                        MFE_MAE_RATIO,
                                        ADVERSE_BEFORE_FAVORABLE,
                                        INVALIDATION_HIT) per
                                       (SETUP_EVENT_ID, EVAL_WINDOW).
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA MART;


/* ----------------------------------------------------------------
   1. V_BOARD_VERDICT_OUTCOMES — base view
   One row per (board candidate × eval_window). Carries everything
   downstream views aggregate. Failed runs are excluded so calibration
   never reads partial slates.
   ---------------------------------------------------------------- */
CREATE OR REPLACE VIEW MIP.MART.V_BOARD_VERDICT_OUTCOMES AS
SELECT
    s.RUN_ID,
    r.AS_OF_DATE                            AS RUN_AS_OF_DATE,
    r.RUN_STATUS,
    r.PROMPT_VERSION,
    r.POLICY_VERSION,
    -- Cortex-vs-deterministic slice key. NULL for runs predating the
    -- Sprint-1 model-config bump; downstream views coalesce.
    r.MODEL_CONFIG_JSON:mode::STRING        AS BOARD_MODE,
    r.MODEL_CONFIG_JSON:specialist_model::STRING AS SPECIALIST_MODEL,
    r.MODEL_CONFIG_JSON:chair_mode::STRING  AS CHAIR_MODE,
    s.CANDIDATE_ID,
    s.SETUP_EVENT_ID,
    s.SYMBOL,
    s.RANK                                  AS BOARD_FINAL_RANK,
    s.VERDICT                               AS BOARD_FINAL_VERDICT,
    s.SIZING_TREATMENT,
    s.RISK_CLASS,
    s.PUBLICATION_STATUS,
    s.PUBLISHED_PROPOSAL_ID,
    cs.FAMILY,
    cs.DIRECTION,
    cs.SETUP_DATE,
    cs.STRUCTURE_CONFIDENCE,
    cs.LEVEL_SIGNIFICANCE,
    cs.MEANINGFUL_HIT_RATE,
    cs.PATH_SURVIVAL_HIT_RATE,
    cs.MFE_MAE_RATIO                        AS PRIOR_MFE_MAE_RATIO,
    o.PRIMARY_REASON_CODE                   AS CHAIR_PRIMARY_REASON_CODE,
    o.SECONDARY_REASON_CODE                 AS CHAIR_SECONDARY_REASON_CODE,
    out.EVAL_WINDOW,
    out.EVAL_STATUS                         AS OUTCOME_EVAL_STATUS,
    out.DIRECTIONAL_SUCCESS,
    out.MEANINGFUL_MOVE_SUCCESS,
    out.INVALIDATION_HIT,
    out.ADVERSE_BEFORE_FAVORABLE,
    out.MFE_MAE_RATIO                       AS REALIZED_MFE_MAE_RATIO,
    out.MAX_ADVERSE_EXCURSION,
    out.MAX_FAVORABLE_EXCURSION,
    out.BARS_TO_INVALIDATION,
    out.BARS_TO_FAVORABLE_THRESHOLD,
    out.FAILURE_MODE,
    out.EVALUATED_AT,
    r.STARTED_AT                            AS RUN_STARTED_AT,
    r.FINISHED_AT                           AS RUN_FINISHED_AT
FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE s
JOIN MIP.APP.PROPOSAL_BOARD_RUN r
  ON r.RUN_ID = s.RUN_ID
LEFT JOIN MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT cs
  ON cs.RUN_ID = s.RUN_ID
 AND cs.CANDIDATE_ID = s.CANDIDATE_ID
LEFT JOIN MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT o
  ON o.RUN_ID = s.RUN_ID
 AND o.CANDIDATE_ID = s.CANDIDATE_ID
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_OUTCOMES out
  ON out.SETUP_EVENT_ID = s.SETUP_EVENT_ID
WHERE r.RUN_STATUS = 'COMPLETE';


/* ----------------------------------------------------------------
   2. V_BOARD_CALIBRATION_BY_VERDICT
   Realised quality grouped by chair verdict × eval window × board mode.
   Use to answer: "for a given board mode, do APPROVE_REDUCED rows
   actually produce different realised outcomes than APPROVE rows?"
   ---------------------------------------------------------------- */
CREATE OR REPLACE VIEW MIP.MART.V_BOARD_CALIBRATION_BY_VERDICT AS
SELECT
    COALESCE(BOARD_MODE, 'unknown_mode')         AS BOARD_MODE,
    BOARD_FINAL_VERDICT,
    EVAL_WINDOW,
    COUNT(*)                                     AS CANDIDATE_COUNT,
    COUNT_IF(OUTCOME_EVAL_STATUS = 'COMPLETE')   AS EVALUATED_COUNT,
    AVG(IFF(DIRECTIONAL_SUCCESS, 1.0, 0.0))      AS DIRECTIONAL_SUCCESS_RATE,
    AVG(IFF(MEANINGFUL_MOVE_SUCCESS, 1.0, 0.0))  AS MEANINGFUL_MOVE_SUCCESS_RATE,
    AVG(IFF(INVALIDATION_HIT, 1.0, 0.0))         AS INVALIDATION_HIT_RATE,
    AVG(IFF(ADVERSE_BEFORE_FAVORABLE, 1.0, 0.0)) AS ADVERSE_BEFORE_FAVORABLE_RATE,
    AVG(REALIZED_MFE_MAE_RATIO)                  AS AVG_REALIZED_MFE_MAE_RATIO,
    MIN(RUN_AS_OF_DATE)                          AS FIRST_RUN_DATE,
    MAX(RUN_AS_OF_DATE)                          AS LAST_RUN_DATE
FROM MIP.MART.V_BOARD_VERDICT_OUTCOMES
WHERE EVAL_WINDOW IS NOT NULL
GROUP BY BOARD_MODE, BOARD_FINAL_VERDICT, EVAL_WINDOW;


/* ----------------------------------------------------------------
   3. V_BOARD_CALIBRATION_BY_PRIMARY_REASON
   Same as above, but the slice key is the chair's primary reason code.
   Use to answer: "are APPROVED_REDUCED_BY_BOARD rows that fired
   because of REPEATED_REPITCH actually realising worse outcomes than
   ones that fired because of EVIDENCE_MIXED?"
   ---------------------------------------------------------------- */
CREATE OR REPLACE VIEW MIP.MART.V_BOARD_CALIBRATION_BY_PRIMARY_REASON AS
SELECT
    COALESCE(BOARD_MODE, 'unknown_mode')         AS BOARD_MODE,
    CHAIR_PRIMARY_REASON_CODE,
    BOARD_FINAL_VERDICT,
    EVAL_WINDOW,
    COUNT(*)                                     AS CANDIDATE_COUNT,
    COUNT_IF(OUTCOME_EVAL_STATUS = 'COMPLETE')   AS EVALUATED_COUNT,
    AVG(IFF(DIRECTIONAL_SUCCESS, 1.0, 0.0))      AS DIRECTIONAL_SUCCESS_RATE,
    AVG(IFF(MEANINGFUL_MOVE_SUCCESS, 1.0, 0.0))  AS MEANINGFUL_MOVE_SUCCESS_RATE,
    AVG(IFF(INVALIDATION_HIT, 1.0, 0.0))         AS INVALIDATION_HIT_RATE,
    AVG(REALIZED_MFE_MAE_RATIO)                  AS AVG_REALIZED_MFE_MAE_RATIO,
    MIN(RUN_AS_OF_DATE)                          AS FIRST_RUN_DATE,
    MAX(RUN_AS_OF_DATE)                          AS LAST_RUN_DATE
FROM MIP.MART.V_BOARD_VERDICT_OUTCOMES
WHERE EVAL_WINDOW IS NOT NULL
  AND CHAIR_PRIMARY_REASON_CODE IS NOT NULL
GROUP BY BOARD_MODE, CHAIR_PRIMARY_REASON_CODE, BOARD_FINAL_VERDICT, EVAL_WINDOW;


/* ----------------------------------------------------------------
   4. V_BOARD_CORTEX_HEALTH
   Per-run, per-agent Cortex acceptance health. Read directly from
   PROPOSAL_BOARD_AGENT_OUTCOME so it works for runs of any size and
   without joining outcomes.

   Tracks:
     - PARSE_PASS_PCT          — % of agent rows where Cortex parsed.
     - VALIDATION_PASS_PCT     — % where Cortex passed reason-code +
                                 verdict allow-list validation
                                 (i.e. the cortex vote was used).
     - FALLBACK_PCT            — % where the deterministic specialist
                                 served as the safety net.
     - MODEL_FALLBACK_USED_PCT — % whose STRUCTURED_OUTPUT_JSON
                                 reason flag set explicitly (for
                                 cross-checking).

   Use to alarm if VALIDATION_PASS_PCT trends down meaningfully on
   a given agent over consecutive runs (a sustained drop is a
   stronger signal than a single noisy run).
   ---------------------------------------------------------------- */
CREATE OR REPLACE VIEW MIP.MART.V_BOARD_CORTEX_HEALTH AS
SELECT
    o.RUN_ID,
    r.AS_OF_DATE,
    COALESCE(r.MODEL_CONFIG_JSON:mode::STRING, 'unknown_mode') AS BOARD_MODE,
    o.AGENT_NAME,
    COUNT(*)                                                   AS AGENT_OUTCOME_COUNT,
    COUNT_IF(o.STRUCTURED_OUTPUT_JSON:cortex_parse_succeeded::BOOLEAN)
                                                               AS CORTEX_PARSED_COUNT,
    COUNT_IF(o.STRUCTURED_OUTPUT_JSON:cortex_validation_passed::BOOLEAN)
                                                               AS CORTEX_VALIDATED_COUNT,
    COUNT_IF(o.STRUCTURED_OUTPUT_JSON:mode::STRING = 'deterministic_fallback')
                                                               AS DETERMINISTIC_FALLBACK_COUNT,
    -- A row that hit fallback should also carry the explicit flag
    -- (set in the SP). If these two diverge it's a regression in the
    -- per-row audit fields, not in cortex itself.
    COUNT_IF(ARRAY_CONTAINS(
        'MODEL_FALLBACK_USED'::VARIANT,
        o.STRUCTURED_OUTPUT_JSON:concern_flags
    ))                                                          AS EXPLICIT_FALLBACK_FLAG_COUNT,
    DIV0(
        COUNT_IF(o.STRUCTURED_OUTPUT_JSON:cortex_parse_succeeded::BOOLEAN),
        COUNT(*)
    )                                                           AS PARSE_PASS_PCT,
    DIV0(
        COUNT_IF(o.STRUCTURED_OUTPUT_JSON:cortex_validation_passed::BOOLEAN),
        COUNT(*)
    )                                                           AS VALIDATION_PASS_PCT,
    DIV0(
        COUNT_IF(o.STRUCTURED_OUTPUT_JSON:mode::STRING = 'deterministic_fallback'),
        COUNT(*)
    )                                                           AS FALLBACK_PCT,
    r.STARTED_AT                                                AS RUN_STARTED_AT
FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME o
JOIN MIP.APP.PROPOSAL_BOARD_RUN r
  ON r.RUN_ID = o.RUN_ID
WHERE r.RUN_STATUS = 'COMPLETE'
GROUP BY
    o.RUN_ID,
    r.AS_OF_DATE,
    BOARD_MODE,
    o.AGENT_NAME,
    r.STARTED_AT;


/* ----------------------------------------------------------------
   Grants — read-only for the API role and to the admin role.
   ---------------------------------------------------------------- */
GRANT SELECT ON VIEW MIP.MART.V_BOARD_VERDICT_OUTCOMES            TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_BOARD_CALIBRATION_BY_VERDICT      TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_BOARD_CALIBRATION_BY_PRIMARY_REASON TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_BOARD_CORTEX_HEALTH               TO ROLE MIP_ADMIN_ROLE;

GRANT SELECT ON VIEW MIP.MART.V_BOARD_VERDICT_OUTCOMES            TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_BOARD_CALIBRATION_BY_VERDICT      TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_BOARD_CALIBRATION_BY_PRIMARY_REASON TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_BOARD_CORTEX_HEALTH               TO ROLE MIP_UI_API_ROLE;

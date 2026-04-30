/* ================================================================
   board_verdict_calibration.sql
   Phase 2 / item 5 — read-only smoke queries that exercise the
   board calibration views.

   Run after deploying:
     MIP/SQL/views/mart/v_board_calibration.sql

   These queries do not modify any table, procedure, or view. Use
   them to:
     1. Validate the views compile and return expected shapes.
     2. Spot-check current calibration before recommending threshold
        or prompt changes (to be done in a later sprint with bumped
        POLICY_VERSION).

   Reads are scoped to RUN_STATUS='COMPLETE' inside the views, so
   smoke runs and partial failures cannot pollute calibration.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA MART;


/* 1. View existence + row counts. Fast smoke — confirms grants and
   joins compile. */
SELECT 'V_BOARD_VERDICT_OUTCOMES'        AS VIEW_NAME, COUNT(*) AS ROWS_TOTAL FROM MIP.MART.V_BOARD_VERDICT_OUTCOMES
UNION ALL
SELECT 'V_BOARD_CALIBRATION_BY_VERDICT', COUNT(*)               FROM MIP.MART.V_BOARD_CALIBRATION_BY_VERDICT
UNION ALL
SELECT 'V_BOARD_CALIBRATION_BY_PRIMARY_REASON', COUNT(*)        FROM MIP.MART.V_BOARD_CALIBRATION_BY_PRIMARY_REASON
UNION ALL
SELECT 'V_BOARD_CORTEX_HEALTH',          COUNT(*)               FROM MIP.MART.V_BOARD_CORTEX_HEALTH;


/* 2. Distinct board modes seen so far. Sanity check — a single
   mode should dominate at any given time (one Sprint per row). If
   you see both 'cortex_specialists_with_deterministic_fallback'
   and 'deterministic_local_specialists', it just means historical
   runs are still in scope. */
SELECT
    BOARD_MODE,
    SPECIALIST_MODEL,
    CHAIR_MODE,
    COUNT(DISTINCT RUN_ID)        AS RUN_COUNT,
    COUNT(*)                       AS CANDIDATE_OUTCOME_ROWS,
    MIN(RUN_AS_OF_DATE)           AS FIRST_DATE,
    MAX(RUN_AS_OF_DATE)           AS LAST_DATE
FROM MIP.MART.V_BOARD_VERDICT_OUTCOMES
GROUP BY BOARD_MODE, SPECIALIST_MODEL, CHAIR_MODE
ORDER BY LAST_DATE DESC;


/* 3. Cortex acceptance health across every COMPLETE run. Look for
   VALIDATION_PASS_PCT below ~0.95 on any agent — that's the
   regression alarm. */
SELECT
    AGENT_NAME,
    BOARD_MODE,
    COUNT(*)                                                AS RUNS,
    SUM(AGENT_OUTCOME_COUNT)                                AS TOTAL_AGENT_ROWS,
    DIV0(SUM(CORTEX_PARSED_COUNT),    SUM(AGENT_OUTCOME_COUNT)) AS PARSE_PASS_PCT,
    DIV0(SUM(CORTEX_VALIDATED_COUNT), SUM(AGENT_OUTCOME_COUNT)) AS VALIDATION_PASS_PCT,
    DIV0(SUM(DETERMINISTIC_FALLBACK_COUNT), SUM(AGENT_OUTCOME_COUNT)) AS FALLBACK_PCT
FROM MIP.MART.V_BOARD_CORTEX_HEALTH
GROUP BY AGENT_NAME, BOARD_MODE
ORDER BY AGENT_NAME, BOARD_MODE;


/* 4. Realised quality by chair verdict, grouped by board mode and
   eval window. Expected directionally: APPROVE >= APPROVE_REDUCED
   for both DIRECTIONAL_SUCCESS_RATE and AVG_REALIZED_MFE_MAE_RATIO,
   and INVALIDATION_HIT_RATE should trend the other way. Empty
   result means no candidates have outcomes yet — wait for the
   next setup-outcomes batch. */
SELECT
    BOARD_MODE,
    BOARD_FINAL_VERDICT,
    EVAL_WINDOW,
    CANDIDATE_COUNT,
    EVALUATED_COUNT,
    DIRECTIONAL_SUCCESS_RATE,
    MEANINGFUL_MOVE_SUCCESS_RATE,
    INVALIDATION_HIT_RATE,
    AVG_REALIZED_MFE_MAE_RATIO,
    LAST_RUN_DATE
FROM MIP.MART.V_BOARD_CALIBRATION_BY_VERDICT
ORDER BY BOARD_MODE, BOARD_FINAL_VERDICT, EVAL_WINDOW;


/* 5. Realised quality by chair primary reason code, focused on the
   top reason codes by candidate count. Useful for detecting
   reason-code drift: e.g. APPROVED_REDUCED_BY_BOARD rows that
   fired because of REPEATED_REPITCH should produce systematically
   weaker outcomes than ones that fired because of EVIDENCE_MIXED. */
SELECT
    BOARD_MODE,
    CHAIR_PRIMARY_REASON_CODE,
    BOARD_FINAL_VERDICT,
    EVAL_WINDOW,
    CANDIDATE_COUNT,
    EVALUATED_COUNT,
    DIRECTIONAL_SUCCESS_RATE,
    MEANINGFUL_MOVE_SUCCESS_RATE,
    INVALIDATION_HIT_RATE,
    AVG_REALIZED_MFE_MAE_RATIO,
    LAST_RUN_DATE
FROM MIP.MART.V_BOARD_CALIBRATION_BY_PRIMARY_REASON
ORDER BY CANDIDATE_COUNT DESC, BOARD_MODE, BOARD_FINAL_VERDICT
LIMIT 50;

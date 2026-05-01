/* ================================================================
   phase4_eligibility_short_gate_smoke.sql
   Phase 4 — focused smoke for the eligibility filter, IBKR_ACCOUNT_MODE
   guard, ACTIONABILITY_ESCALATION audit code, and reason-code allowlist.

   Run with:
     cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py \
         -f MIP/SQL/smoke/phase4_eligibility_short_gate_smoke.sql
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

-- 1) IBKR_ACCOUNT_MODE column shape and backfill.
SELECT 'IBKR_ACCOUNT_MODE_SHAPE' AS CHECK_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
  FROM MIP.INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_SCHEMA = 'LIVE'
   AND TABLE_NAME   = 'LIVE_PORTFOLIO_CONFIG'
   AND COLUMN_NAME  = 'IBKR_ACCOUNT_MODE';

SELECT 'IBKR_ACCOUNT_MODE_VALUES' AS CHECK_NAME,
       PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, IBKR_ACCOUNT_MODE
  FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
 ORDER BY PORTFOLIO_ID;

-- 2) Eligibility table schema sanity.
SELECT 'ELIGIBILITY_TABLE_SHAPE' AS CHECK_NAME, COLUMN_NAME, DATA_TYPE
  FROM MIP.INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_SCHEMA = 'APP'
   AND TABLE_NAME   = 'PROPOSAL_BOARD_REVIEW_ELIGIBILITY'
 ORDER BY ORDINAL_POSITION;

-- 3) Reason-code allowlist (eligibility + new audit codes must be present).
SELECT 'REASON_CODES_ELIGIBILITY' AS CHECK_NAME, REASON_CODE, REASON_CATEGORY, SEVERITY
  FROM MIP.APP.PROPOSAL_BOARD_REASON_CODE
 WHERE REASON_CATEGORY = 'ELIGIBILITY'
    OR REASON_CODE IN ('ACTIONABILITY_ESCALATION', 'IBKR_ACCOUNT_MODE_NOT_PAPER')
 ORDER BY REASON_CATEGORY, REASON_CODE;

-- 4) Direct shape check — every eligibility row has a primary reason and the
--    primary reason is in the allowlist. Should always be 0.
SELECT 'ELIGIBILITY_ROWS_WITH_BAD_REASON' AS CHECK_NAME,
       PRIMARY_REASON_CODE, COUNT(*) AS N
  FROM MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY r
 WHERE NOT EXISTS (
        SELECT 1 FROM MIP.APP.PROPOSAL_BOARD_REASON_CODE rc
         WHERE rc.REASON_CODE = r.PRIMARY_REASON_CODE
           AND rc.IS_ACTIVE
       )
 GROUP BY PRIMARY_REASON_CODE;

-- 5) Most recent agentic run + eligibility breakdown. Bound the lookup to
--    the latest run so the smoke doesn't drift over time.
SET v_run_id = (
    SELECT RUN_ID
      FROM MIP.APP.PROPOSAL_BOARD_RUN
     WHERE MODEL_CONFIG_JSON:mode::STRING = 'symbol_dossier_cortex_agentic_board_multi_round'
     ORDER BY STARTED_AT DESC
     LIMIT 1
);

SELECT 'LATEST_AGENTIC_RUN_FOR_SMOKE' AS CHECK_NAME,
       $v_run_id AS RUN_ID,
       (SELECT COUNT(*) FROM MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY WHERE RUN_ID = $v_run_id) AS ELIGIBILITY_ROW_COUNT,
       (SELECT SUM(CASE WHEN ELIGIBLE THEN 1 ELSE 0 END) FROM MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY WHERE RUN_ID = $v_run_id) AS ELIGIBLE_ROW_COUNT,
       (SELECT SUM(CASE WHEN NOT ELIGIBLE THEN 1 ELSE 0 END) FROM MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY WHERE RUN_ID = $v_run_id) AS SKIPPED_ROW_COUNT;

SELECT 'ELIGIBILITY_BREAKDOWN' AS CHECK_NAME,
       ELIGIBLE, PRIMARY_REASON_CODE, COUNT(*) AS N
  FROM MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY
 WHERE RUN_ID = $v_run_id
 GROUP BY ELIGIBLE, PRIMARY_REASON_CODE
 ORDER BY ELIGIBLE DESC, N DESC;

-- 6) ACTIONABILITY_ESCALATION audit interactions for the latest run.
SELECT 'ACTIONABILITY_ESCALATIONS' AS CHECK_NAME,
       DOSSIER_ID, SOURCE_AGENT, TARGET_AGENT,
       LEFT(DISAGREEMENT_TEXT, 200) AS DISAGREEMENT_TEXT,
       LEFT(RESPONSE_TEXT, 200) AS RESPONSE_TEXT
  FROM MIP.APP.PROPOSAL_BOARD_INTERACTION_V2
 WHERE RUN_ID = $v_run_id
   AND DISAGREEMENT_TYPE = 'ACTIONABILITY_ESCALATION'
 ORDER BY DOSSIER_ID;

-- 7) Short-publication safety: for the latest run, any final slate row
--    blocked by IBKR_ACCOUNT_MODE_NOT_PAPER should carry that reason.
SELECT 'SHORT_PUBLICATION_BLOCKED' AS CHECK_NAME,
       fs.DOSSIER_ID, fs.SYMBOL, fs.FINAL_ACTION, fs.PUBLICATION_STATUS,
       fs.PUBLICATION_ERROR_JSON
  FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
 WHERE fs.RUN_ID = $v_run_id
   AND fs.PUBLICATION_STATUS = 'SKIPPED_GUARDRAIL'
   AND fs.PUBLICATION_ERROR_JSON:reason::STRING = 'IBKR_ACCOUNT_MODE_NOT_PAPER'
 ORDER BY fs.DOSSIER_ID;

-- 8) Hard rule: no PUBLISHED PROPOSE_SHORT row from a run where the
--    portfolio's IBKR_ACCOUNT_MODE is anything other than PAPER. Should be 0.
SELECT 'PUBLISHED_SHORT_WITH_NON_PAPER_PORTFOLIO' AS CHECK_NAME,
       fs.RUN_ID, fs.DOSSIER_ID, fs.SYMBOL, c.IBKR_ACCOUNT_MODE
  FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
  JOIN MIP.APP.PROPOSAL_BOARD_RUN r ON r.RUN_ID = fs.RUN_ID
  LEFT JOIN MIP.LIVE.LIVE_PORTFOLIO_CONFIG c ON c.PORTFOLIO_ID = r.PORTFOLIO_ID
 WHERE fs.PUBLICATION_STATUS = 'PUBLISHED'
   AND fs.FINAL_ACTION = 'PROPOSE_SHORT'
   AND COALESCE(c.IBKR_ACCOUNT_MODE, 'UNKNOWN') <> 'PAPER';

-- 9) Agentic-only structural-timeline guarantee: the proposals lane must not
--    surface any legacy deterministic-selector row (BOARD_RUN_ID NULL).
--    Should be 0 after the v_structural_timeline_views.sql cutover.
SELECT 'TIMELINE_PROPOSALS_LEGACY_LEAK' AS CHECK_NAME,
       COUNT(*) AS LEGACY_ROW_COUNT
  FROM MIP.MART.V_STRUCTURAL_TIMELINE_PROPOSALS
 WHERE BOARD_RUN_ID IS NULL;

-- 10) Agentic-only structural-timeline event rail: no PROPOSAL_CREATED event
--     should be missing PROPOSAL_ID (defensive) or stem from a legacy row.
--     Cross-check by joining back to STRUCTURAL_TRADE_PROPOSALS.
SELECT 'TIMELINE_EVENTS_LEGACY_PROPOSAL_LEAK' AS CHECK_NAME,
       COUNT(*) AS LEGACY_EVENT_COUNT
  FROM MIP.MART.V_STRUCTURAL_TIMELINE_EVENTS ev
  LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
    ON sp.PROPOSAL_ID = ev.PROPOSAL_ID
 WHERE ev.EVENT_TYPE = 'PROPOSAL_CREATED'
   AND COALESCE(sp.BOARD_RUN_ID, '') = '';

-- 11a) LPA decision-validation gate (HARD RULE):
--      No LIVE_ACTIONS row in a pre-decision/committee-validation status
--      may be linked to a parent proposal with BOARD_RUN_ID NULL (legacy).
--      "Pre-decision" = states where the operator/committee is still
--      being asked to validate or approve. Once a row reaches PM_ACCEPTED
--      or beyond, the decision has been made and the proposal-lineage gate
--      no longer applies. Should be 0.
SELECT 'LPA_PENDING_DECISION_LEGACY_PARENT' AS CHECK_NAME,
       COUNT(*) AS LEGACY_PENDING_DECISION_COUNT
  FROM MIP.LIVE.LIVE_ACTIONS la
  JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
    ON p.PROPOSAL_ID = la.PROPOSAL_ID
 WHERE p.BOARD_RUN_ID IS NULL
   AND la.STATUS IN (
        'RESEARCH_IMPORTED','PROPOSED','PENDING_OPEN_VALIDATION','OPEN_ELIGIBLE','OPEN_CAUTION',
        'OPEN_BLOCKED','PENDING_OPEN_STABILITY_REVIEW','READY_FOR_APPROVAL_FLOW'
       );

-- 11b) Historical/post-decision lineage (INFORMATIONAL ONLY):
--      Pre-cutover legacy-proposal LIVE_ACTIONS that already passed
--      decision-validation and are now post-approval (PM_ACCEPTED through
--      EXECUTION_REQUESTED). These rows are expected and not actionable
--      by the LPA decision flow. Reported for audit completeness only.
SELECT 'LPA_POST_DECISION_LEGACY_PARENT_INFO' AS CHECK_NAME,
       la.STATUS, COUNT(*) AS N
  FROM MIP.LIVE.LIVE_ACTIONS la
  JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
    ON p.PROPOSAL_ID = la.PROPOSAL_ID
 WHERE p.BOARD_RUN_ID IS NULL
   AND la.STATUS IN (
        'PM_ACCEPTED','COMPLIANCE_APPROVED','INTENT_SUBMITTED','INTENT_APPROVED',
        'REVALIDATED_PASS','REVALIDATED_FAIL','EXECUTION_REQUESTED'
       )
 GROUP BY la.STATUS
 ORDER BY N DESC;

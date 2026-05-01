/* ================================================================
   phase4_agentic_board_smoke.sql
   Phase 4 Cortex Agentic Proposal Board — post-run validation.

   Run after MIP/scripts/proposal_board_phase4/run_board.py finishes.
   Looks up the most recent run that uses
   MODEL_CONFIG_JSON.mode='symbol_dossier_cortex_agentic_board_multi_round'
   and validates ordering invariants, agent independence, interaction
   persistence, FX/short gating, and committee compatibility.

   No assertions are written; queries return rows for the operator to
   inspect. Empty result sets are fine where indicated.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

-- 1) Identify the latest agentic run.
SET v_run_id = (
    SELECT RUN_ID
      FROM MIP.APP.PROPOSAL_BOARD_RUN
     WHERE MODEL_CONFIG_JSON:mode::STRING = 'symbol_dossier_cortex_agentic_board_multi_round'
     ORDER BY STARTED_AT DESC
     LIMIT 1
);

SELECT 'LATEST_AGENTIC_RUN' AS CHECK_NAME,
       $v_run_id AS RUN_ID,
       (SELECT RUN_STATUS FROM MIP.APP.PROPOSAL_BOARD_RUN WHERE RUN_ID = $v_run_id) AS STATUS,
       (SELECT MODEL_CONFIG_JSON:mode::STRING FROM MIP.APP.PROPOSAL_BOARD_RUN WHERE RUN_ID = $v_run_id) AS MODE,
       (SELECT PROMPT_VERSION FROM MIP.APP.PROPOSAL_BOARD_RUN WHERE RUN_ID = $v_run_id) AS PROMPT_VERSION,
       (SELECT FINAL_PROPOSAL_COUNT FROM MIP.APP.PROPOSAL_BOARD_RUN WHERE RUN_ID = $v_run_id) AS FINAL_PROPOSAL_COUNT;

-- 2) Specialist count per dossier — should be exactly 5 for every dossier
--    that the chair acted on.
SELECT 'SPECIALIST_ROW_COUNTS' AS CHECK_NAME,
       DOSSIER_ID,
       COUNT(*) AS SPECIALIST_ROW_COUNT,
       LISTAGG(AGENT_NAME, ',') WITHIN GROUP (ORDER BY AGENT_NAME) AS AGENTS
  FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2
 WHERE RUN_ID = $v_run_id
 GROUP BY DOSSIER_ID
 ORDER BY DOSSIER_ID;

-- 3) Each persisted specialist must show real tool calls
--    (STRUCTURED_OUTPUT_JSON.evidence_used not empty). Empty means the
--    agent never called get_evidence_slice.
SELECT 'AGENTS_WITHOUT_TOOL_CALLS' AS CHECK_NAME,
       DOSSIER_ID, AGENT_NAME, VERDICT,
       STRUCTURED_OUTPUT_JSON:evidence_used AS EVIDENCE_USED
  FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2
 WHERE RUN_ID = $v_run_id
   AND (
       STRUCTURED_OUTPUT_JSON:evidence_used IS NULL
       OR ARRAY_SIZE(STRUCTURED_OUTPUT_JSON:evidence_used) = 0
   )
 ORDER BY DOSSIER_ID, AGENT_NAME;

-- 4) Interaction persistence — count interactions per dossier.
SELECT 'INTERACTION_COUNTS' AS CHECK_NAME,
       DOSSIER_ID,
       COUNT(*) AS INTERACTION_ROWS,
       SUM(CASE WHEN RESOLVED_FLAG THEN 1 ELSE 0 END) AS RESOLVED_ROWS,
       LISTAGG(DISTINCT TOPIC, ',') WITHIN GROUP (ORDER BY TOPIC) AS TOPICS,
       LISTAGG(DISTINCT DISAGREEMENT_TYPE, ',') WITHIN GROUP (ORDER BY DISAGREEMENT_TYPE) AS DISAGREEMENT_TYPES
  FROM MIP.APP.PROPOSAL_BOARD_INTERACTION_V2
 WHERE RUN_ID = $v_run_id
 GROUP BY DOSSIER_ID
 ORDER BY DOSSIER_ID;

-- 5) Chair verdicts — one per dossier the chair acted on.
SELECT 'CHAIR_VERDICTS' AS CHECK_NAME,
       DOSSIER_ID, SYMBOL, FINAL_ACTION, FINAL_DIRECTION,
       PRIMARY_REASON_CODE,
       PROPOSED_TRADE_CONFIG_JSON:thesis_label::STRING AS THESIS_LABEL,
       COMMITTEE_PAYLOAD:unresolved_disagreement::BOOLEAN AS UNRESOLVED_DISAGREEMENT
  FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT
 WHERE RUN_ID = $v_run_id
 ORDER BY DOSSIER_ID;

-- 6) Hard rule: no proposal published with unresolved direction disagreement.
SELECT 'PROPOSALS_WITH_UNRESOLVED_DISAGREEMENT' AS CHECK_NAME,
       v.DOSSIER_ID, v.SYMBOL, v.FINAL_ACTION,
       v.COMMITTEE_PAYLOAD:unresolved_disagreement::BOOLEAN AS UNRESOLVED
  FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT v
  JOIN MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs USING (RUN_ID, DOSSIER_ID)
 WHERE v.RUN_ID = $v_run_id
   AND fs.PUBLICATION_STATUS = 'PUBLISHED'
   AND v.FINAL_ACTION IN ('PROPOSE_LONG','PROPOSE_SHORT')
   AND v.COMMITTEE_PAYLOAD:unresolved_disagreement::BOOLEAN = TRUE;

-- 7) Final slate breakdown.
SELECT 'FINAL_SLATE_DISTRIBUTION' AS CHECK_NAME,
       FINAL_ACTION, PUBLICATION_STATUS, COUNT(*) AS N
  FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2
 WHERE RUN_ID = $v_run_id
 GROUP BY FINAL_ACTION, PUBLICATION_STATUS
 ORDER BY FINAL_ACTION, PUBLICATION_STATUS;

-- 8) Verdict distribution across ALL dossiers (zero-counts shown explicitly).
WITH categories AS (
    SELECT * FROM VALUES
        ('PROPOSE_LONG'),
        ('PROPOSE_SHORT'),
        ('WATCH_LONG'),
        ('WATCH_SHORT'),
        ('NO_TRADE'),
        ('REJECT'),
        ('WAIT_FOR_CONFIRMATION')
    AS t(FINAL_ACTION)
)
SELECT 'CHAIR_VERDICT_DISTRIBUTION' AS CHECK_NAME,
       c.FINAL_ACTION,
       COUNT(v.VERDICT_ID) AS N
  FROM categories c
  LEFT JOIN MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT v
    ON v.FINAL_ACTION = c.FINAL_ACTION
   AND v.RUN_ID = $v_run_id
 GROUP BY c.FINAL_ACTION
 ORDER BY c.FINAL_ACTION;

-- 9) STRUCTURAL_TRADE_PROPOSALS — must show the new direction_source and
--    AGENTIC_ thesis label.
SELECT 'PUBLISHED_PROPOSALS_AUDIT' AS CHECK_NAME,
       p.PROPOSAL_ID, p.SYMBOL, p.DIRECTION, p.SETUP_FAMILY, p.STATUS,
       p.BOARD_PAYLOAD_JSON:direction_source::STRING AS DIRECTION_SOURCE,
       p.BOARD_PAYLOAD_JSON:setup_family_source::STRING AS SETUP_FAMILY_SOURCE,
       p.BOARD_PAYLOAD_JSON:primary_evidence_setup_event_id_role::STRING AS EVIDENCE_ROLE,
       p.BOARD_PAYLOAD_JSON:mode::STRING AS BOARD_MODE
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
 WHERE p.BOARD_RUN_ID = $v_run_id
 ORDER BY p.PROPOSAL_ID;

-- 10) Hard rule: no published proposal with non-AGENTIC setup family.
SELECT 'PUBLISHED_PROPOSALS_NON_AGENTIC' AS CHECK_NAME,
       PROPOSAL_ID, SYMBOL, DIRECTION, SETUP_FAMILY
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
 WHERE BOARD_RUN_ID = $v_run_id
   AND SETUP_FAMILY NOT ILIKE 'AGENTIC_%';

-- 11) FX gating — published FX proposals must only exist if FX_LIVE_ENABLED.
SELECT 'PUBLISHED_FX_FLAG_CHECK' AS CHECK_NAME,
       p.PROPOSAL_ID, p.SYMBOL, s.MARKET_TYPE, s.FX_LIVE_ENABLED, p.DIRECTION
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
  JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
    ON s.RUN_ID = p.BOARD_RUN_ID AND s.DOSSIER_ID = p.BOARD_DOSSIER_ID
 WHERE p.BOARD_RUN_ID = $v_run_id
   AND s.MARKET_TYPE = 'FX'
   AND s.FX_LIVE_ENABLED = FALSE;

-- 12) Short gating — published shorts must only exist if SHORT_LIVE_ENABLED.
SELECT 'PUBLISHED_SHORT_FLAG_CHECK' AS CHECK_NAME,
       p.PROPOSAL_ID, p.SYMBOL, p.DIRECTION, s.SHORT_LIVE_ENABLED
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
  JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
    ON s.RUN_ID = p.BOARD_RUN_ID AND s.DOSSIER_ID = p.BOARD_DOSSIER_ID
 WHERE p.BOARD_RUN_ID = $v_run_id
   AND p.DIRECTION = 'SHORT'
   AND s.SHORT_LIVE_ENABLED = FALSE;

-- 13) Committee view compatibility — published rows must be visible.
SELECT 'COMMITTEE_VIEW_COMPATIBILITY' AS CHECK_NAME,
       PROPOSAL_ID, SYMBOL, SETUP_FAMILY, DIRECTION
  FROM MIP.MART.V_STRUCTURAL_PROPOSALS_FOR_COMMITTEE
 WHERE PROPOSAL_ID IN (
     SELECT PROPOSAL_ID FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS WHERE BOARD_RUN_ID = $v_run_id
 )
 ORDER BY PROPOSAL_ID;

-- 14) Output errors logged for this run (should be empty unless a specialist
--     produced invalid JSON or a non-allowlisted enum).
SELECT 'OUTPUT_ERRORS_FOR_RUN' AS CHECK_NAME,
       AGENT_NAME, ERROR_TYPE, COUNT(*) AS N,
       MAX(LEFT(ERROR_MESSAGE, 200)) AS SAMPLE_MESSAGE
  FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR
 WHERE RUN_ID = $v_run_id
 GROUP BY AGENT_NAME, ERROR_TYPE
 ORDER BY AGENT_NAME, ERROR_TYPE;

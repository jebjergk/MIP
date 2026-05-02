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
        ('WATCH_LONG_FAILURE'),
        ('WATCH_SHORT_FAILURE'),
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

-- ================================================================
-- Phase 4 evidence-hardening v1 smoke checks
-- ================================================================

-- 15) SNAPSHOT_NULL_STRUCTURAL_STATE: every published agentic proposal in this
--     run must have a non-NULL STRUCTURAL_STATE and REGIME_STATE. Should
--     return 0 rows.
SELECT 'SNAPSHOT_NULL_STRUCTURAL_STATE' AS CHECK_NAME,
       p.PROPOSAL_ID, p.SYMBOL, p.SETUP_FAMILY,
       s.STRUCTURAL_STATE, s.REGIME_STATE
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
  LEFT JOIN MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT s
    ON s.PROPOSAL_ID = p.PROPOSAL_ID
 WHERE p.BOARD_RUN_ID = $v_run_id
   AND p.SETUP_FAMILY ILIKE 'AGENTIC_%'
   AND (s.STRUCTURAL_STATE IS NULL OR s.REGIME_STATE IS NULL)
 ORDER BY p.PROPOSAL_ID;

-- 16) DOSSIER_MISSING_STRUCTURAL_TIMELINE: every dossier snapshot in this run
--     must contain the new structural_timeline_summary, candle_psychology,
--     and actionability_context fields. Should return 0 rows.
SELECT 'DOSSIER_MISSING_STRUCTURAL_TIMELINE' AS CHECK_NAME,
       DOSSIER_ID, SYMBOL,
       DOSSIER_PAYLOAD_JSON:evidence_contract_version::STRING AS CONTRACT_VERSION,
       (DOSSIER_PAYLOAD_JSON:structural_timeline_summary IS NOT NULL) AS HAS_TIMELINE_SUMMARY,
       (DOSSIER_PAYLOAD_JSON:candle_psychology IS NOT NULL) AS HAS_CANDLE_PSYCHOLOGY,
       (DOSSIER_PAYLOAD_JSON:actionability_context IS NOT NULL) AS HAS_ACTIONABILITY
  FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT
 WHERE RUN_ID = $v_run_id
   AND (
       DOSSIER_PAYLOAD_JSON:structural_timeline_summary IS NULL
       OR DOSSIER_PAYLOAD_JSON:candle_psychology IS NULL
       OR DOSSIER_PAYLOAD_JSON:actionability_context IS NULL
       OR DOSSIER_PAYLOAD_JSON:evidence_contract_version::STRING IS NULL
   )
 ORDER BY DOSSIER_ID;

-- 17) CHAIR_MISSING_STRUCTURAL_EVIDENCE: informational. Surfaces every Chair
--     diagnostic where the Chair returned a directional proposal without
--     listing the new structural slices in evidence_used. Non-blocking but
--     should be reviewed; ideally returns 0 rows once prompts have settled.
SELECT 'CHAIR_MISSING_STRUCTURAL_EVIDENCE' AS CHECK_NAME,
       DOSSIER_ID, SOURCE_AGENT, TOPIC, DISAGREEMENT_TYPE,
       LEFT(DISAGREEMENT_TEXT, 300) AS DISAGREEMENT_TEXT_SAMPLE
  FROM MIP.APP.PROPOSAL_BOARD_INTERACTION_V2
 WHERE RUN_ID = $v_run_id
   AND DISAGREEMENT_TYPE = 'MISSING_STRUCTURAL_EVIDENCE'
 ORDER BY DOSSIER_ID;

-- ================================================================
-- Phase 4 taxonomy v2 smoke checks
-- ================================================================

-- 18) THESIS_HEALTH_PRESENT: every WATCH_LONG_FAILURE / WATCH_SHORT_FAILURE
--     row must carry a non-null thesis_health and prior_thesis_reference.
--     Should return 0 rows.
SELECT 'THESIS_HEALTH_PRESENT' AS CHECK_NAME,
       v.DOSSIER_ID, v.SYMBOL, v.FINAL_ACTION,
       v.CHAIR_OUTPUT_JSON:thesis_health::STRING AS THESIS_HEALTH,
       v.CHAIR_OUTPUT_JSON:prior_thesis_reference AS PRIOR_THESIS_REFERENCE
  FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT v
 WHERE v.RUN_ID = $v_run_id
   AND v.FINAL_ACTION IN ('WATCH_LONG_FAILURE','WATCH_SHORT_FAILURE')
   AND (
       v.CHAIR_OUTPUT_JSON:thesis_health::STRING IS NULL
       OR v.CHAIR_OUTPUT_JSON:prior_thesis_reference IS NULL
       OR v.CHAIR_OUTPUT_JSON:prior_thesis_reference:proposal_id IS NULL
   )
 ORDER BY v.DOSSIER_ID;

-- 19) WATCH_SHORT_WITHOUT_DOMINANT_EVIDENCE: informational. Surfaces every
--     row where the Chair returned WATCH_SHORT or PROPOSE_SHORT (or LONG
--     symmetrically) but the DOMINANT EVIDENCE rule was not satisfied.
--     Ideally 0 rows once prompts settle; non-blocking.
SELECT 'WATCH_SHORT_WITHOUT_DOMINANT_EVIDENCE' AS CHECK_NAME,
       DOSSIER_ID, SOURCE_AGENT, TOPIC, DISAGREEMENT_TYPE,
       LEFT(DISAGREEMENT_TEXT, 400) AS DISAGREEMENT_TEXT_SAMPLE
  FROM MIP.APP.PROPOSAL_BOARD_INTERACTION_V2
 WHERE RUN_ID = $v_run_id
   AND DISAGREEMENT_TYPE = 'WEAK_SHORT_EVIDENCE'
 ORDER BY DOSSIER_ID;

-- 20) LEVEL_CITATION_MISSING_CONFIDENCE: informational. Surfaces every
--     short-direction Chair verdict that referenced a level without
--     citing its confidence value. Non-blocking.
SELECT 'LEVEL_CITATION_MISSING_CONFIDENCE' AS CHECK_NAME,
       DOSSIER_ID, SOURCE_AGENT, TOPIC, DISAGREEMENT_TYPE,
       LEFT(DISAGREEMENT_TEXT, 400) AS DISAGREEMENT_TEXT_SAMPLE
  FROM MIP.APP.PROPOSAL_BOARD_INTERACTION_V2
 WHERE RUN_ID = $v_run_id
   AND DISAGREEMENT_TYPE = 'MISSING_LEVEL_CONFIDENCE_CITATION'
 ORDER BY DOSSIER_ID;

-- 21) NON_STOCK_PUBLISH_ATTEMPT: hard guard. Should always return 0 rows.
--     Surfaces any final-slate row where the Phase 4 STOCK-only publication
--     guard had to block a non-STOCK proposal. Ideally never fires because
--     run_board.py defaults to --market-types STOCK; this is the belt-and-
--     braces audit on top of the orchestrator-level guard.
SELECT 'NON_STOCK_PUBLISH_ATTEMPT' AS CHECK_NAME,
       fs.DOSSIER_ID, fs.SYMBOL, fs.MARKET_TYPE,
       fs.FINAL_ACTION, fs.PUBLICATION_STATUS,
       fs.PUBLICATION_ERROR_JSON:reason::STRING AS REASON,
       fs.PUBLICATION_ERROR_JSON:market_type::STRING AS BLOCKED_MARKET_TYPE
  FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
 WHERE fs.RUN_ID = $v_run_id
   AND fs.PUBLICATION_ERROR_JSON:reason::STRING = 'BLOCKED_NON_STOCK_PUBLISH'
 ORDER BY fs.DOSSIER_ID;

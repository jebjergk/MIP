-- =============================================================================
-- 49_phase4_tier1_plumbing_smoke.sql
-- Tier 1 pipeline fixes — post-deploy validation (read-only)
--
-- Checks:
--   T1-A  View allows PARTIAL_FAILURE + published-proposal tolerance
--   T1-B  No recent finished run with PROPOSE_* slate stuck in PENDING
--   T1-C  Authority view includes runs with published proposals despite errors
--
-- Note: historical PENDING rows from before Tier 1 may fail T1-B until cleared
-- or superseded by a new board run. Re-run after the next bar load + board run.
-- =============================================================================

-- T1-A: View text includes Tier 1 semantics
SELECT 'T1A_VIEW_PARTIAL_FAILURE' AS CHECK_NAME,
       IFF(
           VIEW_DEFINITION ILIKE '%PARTIAL_FAILURE%'
           AND VIEW_DEFINITION ILIKE '%STRUCTURAL_TRADE_PROPOSALS%'
           AND VIEW_DEFINITION ILIKE '%PROPOSAL_BOARD_OUTPUT_ERROR%',
           'PASS',
           'FAIL'
       ) AS RESULT
  FROM MIP.INFORMATION_SCHEMA.VIEWS
 WHERE TABLE_SCHEMA = 'MART'
   AND TABLE_NAME = 'V_LATEST_AUTHORITATIVE_BOARD_RUN';

-- T1-B: Limbo detector — PROPOSE_* still PENDING on finished runs (last 7d)
SELECT 'T1B_LIMBO_PROPOSE_PENDING' AS CHECK_NAME,
       COUNT(*) AS LIMBO_COUNT,
       IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS RESULT
  FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
  JOIN MIP.APP.PROPOSAL_BOARD_RUN r ON r.RUN_ID = fs.RUN_ID
 WHERE r.RUN_STATUS IN ('COMPLETE', 'PARTIAL_FAILURE')
   AND r.FINISHED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())
   AND fs.FINAL_ACTION IN ('PROPOSE_LONG', 'PROPOSE_SHORT')
   AND fs.PUBLICATION_STATUS = 'PENDING';

-- T1-C: Published proposals from PARTIAL_FAILURE runs must be authoritative
SELECT 'T1C_PARTIAL_WITH_PUBLISH_IN_AUTH_VIEW' AS CHECK_NAME,
       COUNT(*) AS PARTIAL_PUBLISHED_NOT_AUTH,
       IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS RESULT
  FROM MIP.APP.PROPOSAL_BOARD_RUN r
  JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS stp
    ON stp.BOARD_RUN_ID = r.RUN_ID AND stp.STATUS = 'PROPOSED'
 WHERE r.RUN_STATUS = 'PARTIAL_FAILURE'
   AND r.FINISHED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())
   AND COALESCE(r.CANDIDATE_COUNT, 0) > 0
   AND NOT EXISTS (
       SELECT 1 FROM MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN auth
        WHERE auth.RUN_ID = r.RUN_ID
   );

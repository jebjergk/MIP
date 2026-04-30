-- v_latest_authoritative_board_run.sql
-- Single source of truth for "the latest authoritative Agentic Proposal
-- Board run" used by every actionability gate in the live cockpit.
--
-- A run is treated as authoritative ONLY when ALL of the Phase 2/3 guard
-- conditions hold (mirroring SP_EXPIRE_STALE_DAILY_PROPOSALS Rule 3):
--
--   (a) RUN_STATUS = 'COMPLETE'
--       — excludes RUNNING and FAILED (validation failures and SQL
--         exceptions both land in FAILED).
--
--   (b) COALESCE(CANDIDATE_COUNT, 0) > 0
--       — separates the legitimate chair verdict NO_GOOD_IDEAS_TODAY
--         (board actually evaluated N>0 candidates and rejected all)
--         from the upstream empty-evidence early-exit path that uses
--         the SAME reason_code with CANDIDATE_COUNT = 0.
--
--   (c) No rows in PROPOSAL_BOARD_OUTPUT_ERROR for the RUN_ID
--       — defensive belt-and-suspenders against any future SP change
--         that lets a half-broken run publish on top of validation
--         errors.
--
-- The view returns the single most-recently-started authoritative run
-- across the whole table (not partitioned by AS_OF_DATE) because the
-- cockpit's actionability gate is "is this proposal from THE latest
-- canonical board?", not "is it from today's latest". A run completed
-- minutes ago on AS_OF=today is the only one that can drive live
-- entries.
--
-- Returns at most ONE row. When zero authoritative runs exist (cold
-- start, full prod outage), the view is empty and downstream consumers
-- must fail-closed (no proposal is actionable).

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

CREATE OR REPLACE VIEW MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN AS
SELECT
    r.RUN_ID,
    r.AS_OF_DATE,
    r.STARTED_AT,
    r.FINISHED_AT,
    r.CANDIDATE_COUNT,
    r.FINAL_PROPOSAL_COUNT,
    r.PROMPT_VERSION,
    r.POLICY_VERSION
FROM MIP.APP.PROPOSAL_BOARD_RUN r
WHERE r.RUN_STATUS = 'COMPLETE'
  AND COALESCE(r.CANDIDATE_COUNT, 0) > 0
  AND NOT EXISTS (
        SELECT 1
          FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR e
         WHERE e.RUN_ID = r.RUN_ID
      )
QUALIFY ROW_NUMBER() OVER (
    ORDER BY r.STARTED_AT DESC, r.RUN_ID DESC
) = 1;

GRANT SELECT ON VIEW MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN TO ROLE MIP_APP_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN TO ROLE MIP_AGENT_READ_ROLE;

-- v_latest_authoritative_board_run.sql
-- Source of truth for "which board runs count as authoritative right now"
-- used by every actionability gate in the live cockpit.
--
-- Tier 1 (2026-06-25): tolerate PARTIAL_FAILURE when the run produced at least
-- one published structural proposal. A single dossier LLM/validation error must
-- not hide valid PROPOSE_* rows from LPA/Cockpit.
--
-- A run is treated as authoritative when ALL of the guard conditions hold:
--
--   (a) RUN_STATUS IN ('COMPLETE', 'PARTIAL_FAILURE')
--       — excludes RUNNING and FAILED. PARTIAL_FAILURE is allowed when the
--         board evaluated candidates and may have published some proposals
--         despite invalid dossiers elsewhere in the run.
--
--   (b) COALESCE(CANDIDATE_COUNT, 0) > 0 OR FINAL_PROPOSAL_COUNT > 0
--       — separates legitimate chair evaluation from empty-evidence early exit.
--         Auto-finalized CHAIR_DONE recovery rows may have CANDIDATE_COUNT=0
--         until healed; published proposal count is sufficient authority signal.
--
--   (c) OUTPUT_ERROR tolerance — a run with PROPOSAL_BOARD_OUTPUT_ERROR rows
--       is still authoritative when it published at least one PROPOSED row to
--       STRUCTURAL_TRADE_PROPOSALS (paid work must not be hidden). Runs with
--       errors AND zero published proposals remain non-authoritative.
--
-- Phase 5D semantic update (multi-run-per-day union):
--   Returns ONE ROW PER qualifying RUN whose AS_OF_DATE equals MAX(AS_OF_DATE)
--   across all qualifying runs. Same-day siblings are not adversaries.
--
-- Returns ZERO rows when no authoritative runs exist. Downstream consumers
-- must fail-closed in that case.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

CREATE OR REPLACE VIEW MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN AS
WITH qualifying_runs AS (
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
    WHERE r.RUN_STATUS IN ('COMPLETE', 'PARTIAL_FAILURE')
      AND (
            COALESCE(r.CANDIDATE_COUNT, 0) > 0
            OR COALESCE(r.FINAL_PROPOSAL_COUNT, 0) > 0
          )
      AND (
            NOT EXISTS (
                SELECT 1
                  FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR e
                 WHERE e.RUN_ID = r.RUN_ID
            )
            OR EXISTS (
                SELECT 1
                  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
                 WHERE p.BOARD_RUN_ID = r.RUN_ID
                   AND p.STATUS = 'PROPOSED'
            )
      )
),
latest_date AS (
    SELECT MAX(AS_OF_DATE) AS LATEST_AS_OF_DATE
    FROM qualifying_runs
)
SELECT
    q.RUN_ID,
    q.AS_OF_DATE,
    q.STARTED_AT,
    q.FINISHED_AT,
    q.CANDIDATE_COUNT,
    q.FINAL_PROPOSAL_COUNT,
    q.PROMPT_VERSION,
    q.POLICY_VERSION
FROM qualifying_runs q
JOIN latest_date l
  ON q.AS_OF_DATE = l.LATEST_AS_OF_DATE
ORDER BY q.STARTED_AT DESC, q.RUN_ID DESC;

GRANT SELECT ON VIEW MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN TO ROLE MIP_APP_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN TO ROLE MIP_AGENT_READ_ROLE;

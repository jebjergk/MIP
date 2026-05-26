-- v_latest_authoritative_board_run.sql
-- Source of truth for "which board runs count as authoritative right now"
-- used by every actionability gate in the live cockpit.
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
-- Phase 5D semantic update (multi-run-per-day union):
--   Previously this view returned exactly ONE row — the single
--   most-recently-started authoritative run across the whole table —
--   and any proposal whose BOARD_RUN_ID didn't match was treated as
--   "superseded". That broke when multiple board runs completed on
--   the same AS_OF_DATE producing disjoint symbol sets: the later run
--   silently invalidated the earlier run's perfectly valid proposals
--   on different symbols.
--
--   The view now returns ONE ROW PER COMPLETE+VALID RUN whose
--   AS_OF_DATE equals MAX(AS_OF_DATE) across all qualifying runs. All
--   completed runs for that latest trading session are equally
--   authoritative — they're treated as sibling components of the same
--   planning batch, not adversaries. A proposal is "current" iff its
--   BOARD_RUN_ID appears in this view.
--
--   When a board run for a newer AS_OF_DATE lands, the older
--   AS_OF_DATE drops out entirely (via the MAX filter), so the
--   trading day boundary still cleanly retires all prior days'
--   proposals. Cross-day staleness is handled here by the
--   AS_OF_DATE filter and by SP_EXPIRE_STALE_DAILY_PROPOSALS Rule 1
--   (CREATED_AT::DATE based). Same-day siblings are explicitly NOT
--   adversaries of each other.
--
-- Returns ZERO rows when no authoritative runs exist (cold start, full
-- prod outage). Downstream consumers must fail-closed (no proposal is
-- actionable) in that case.

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
    WHERE r.RUN_STATUS = 'COMPLETE'
      AND COALESCE(r.CANDIDATE_COUNT, 0) > 0
      AND NOT EXISTS (
            SELECT 1
              FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR e
             WHERE e.RUN_ID = r.RUN_ID
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

/* ================================================================
   supersede_stale_structural_pending_actions.sql
   LPA stale-proposal lifecycle cleanup — one-time backfill.

   Companion to the wider Stage A change in
   MIP/SQL/app/522_sp_expire_stale_daily_proposals.sql which expands
   the cascade status list to ALL pre-broker structural pending
   statuses (PROPOSED, PENDING_OPEN_VALIDATION, OPEN_ELIGIBLE,
   OPEN_CAUTION, OPEN_BLOCKED, PENDING_OPEN_STABILITY_REVIEW,
   READY_FOR_APPROVAL_FLOW, PM_ACCEPTED, COMPLIANCE_APPROVED,
   INTENT_SUBMITTED, INTENT_APPROVED, REVALIDATED_PASS,
   REVALIDATED_FAIL).

   Previously the cascade only covered four early statuses, so any
   structural ENTRY action that had progressed through committee to
   e.g. REVALIDATED_PASS or PM_ACCEPTED survived the daily expiry
   sweep and lingered in LPA pending decisions as a "stale" row that
   operators had to manually Reject.

   This script runs SP_EXPIRE_STALE_DAILY_PROPOSALS(CURRENT_DATE())
   under the new wider cascade so the existing zombie rows
   (DOW, PG, and any siblings) are terminalized in one pass.

   Safety:
     * EXECUTION_REQUESTED / EXECUTION_PARTIAL actions are NOT
       touched (broker race risk) — the SP only reports them.
     * EXIT actions are not gated by proposal freshness anywhere
       in the system; closing a live position remains available.
     * Open positions (e.g. JD short) are untouched — those rows
       are tracked by parent EXECUTED actions, not pre-broker pending.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

SELECT
    'BEFORE_CLEANUP' AS STEP,
    COUNT(*) AS STALE_STRUCTURAL_PRE_BROKER_PENDING_ROWS,
    ARRAY_AGG(DISTINCT la.SYMBOL) WITHIN GROUP (ORDER BY la.SYMBOL) AS STALE_SYMBOLS,
    ARRAY_AGG(la.ACTION_ID) WITHIN GROUP (ORDER BY la.ACTION_ID) AS STALE_ACTION_IDS
FROM MIP.LIVE.LIVE_ACTIONS la
LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
  ON p.PROPOSAL_ID = la.PROPOSAL_ID
LEFT JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN latest
  ON latest.RUN_ID = p.BOARD_RUN_ID
WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
  AND la.STATUS IN (
      'PROPOSED',
      'PENDING_OPEN_VALIDATION',
      'OPEN_ELIGIBLE',
      'OPEN_CAUTION',
      'OPEN_BLOCKED',
      'PENDING_OPEN_STABILITY_REVIEW',
      'READY_FOR_APPROVAL_FLOW',
      'PM_ACCEPTED',
      'COMPLIANCE_APPROVED',
      'INTENT_SUBMITTED',
      'INTENT_APPROVED',
      'REVALIDATED_PASS',
      'REVALIDATED_FAIL'
  )
  AND (
      p.STATUS IS NULL
      OR p.STATUS <> 'PROPOSED'
      OR latest.RUN_ID IS NULL
  );

CALL MIP.APP.SP_EXPIRE_STALE_DAILY_PROPOSALS(CURRENT_DATE());

SELECT
    'AFTER_CLEANUP' AS STEP,
    COUNT(*) AS REMAINING_STALE_PRE_BROKER_PENDING_ROWS,
    ARRAY_AGG(DISTINCT la.SYMBOL) WITHIN GROUP (ORDER BY la.SYMBOL) AS REMAINING_SYMBOLS,
    ARRAY_AGG(la.ACTION_ID) WITHIN GROUP (ORDER BY la.ACTION_ID) AS REMAINING_ACTION_IDS
FROM MIP.LIVE.LIVE_ACTIONS la
LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
  ON p.PROPOSAL_ID = la.PROPOSAL_ID
LEFT JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN latest
  ON latest.RUN_ID = p.BOARD_RUN_ID
WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
  AND la.STATUS IN (
      'PROPOSED',
      'PENDING_OPEN_VALIDATION',
      'OPEN_ELIGIBLE',
      'OPEN_CAUTION',
      'OPEN_BLOCKED',
      'PENDING_OPEN_STABILITY_REVIEW',
      'READY_FOR_APPROVAL_FLOW',
      'PM_ACCEPTED',
      'COMPLIANCE_APPROVED',
      'INTENT_SUBMITTED',
      'INTENT_APPROVED',
      'REVALIDATED_PASS',
      'REVALIDATED_FAIL'
  )
  AND (
      p.STATUS IS NULL
      OR p.STATUS <> 'PROPOSED'
      OR latest.RUN_ID IS NULL
  );

SELECT
    'DOW_PG_STATUS_AFTER' AS STEP,
    la.ACTION_ID,
    la.SYMBOL,
    la.STATUS,
    la.REASON_CODES,
    p.STATUS AS PROPOSAL_STATUS,
    p.BOARD_RUN_ID AS PROPOSAL_BOARD_RUN_ID,
    latest.RUN_ID AS MATCHED_AUTH_RUN_ID
FROM MIP.LIVE.LIVE_ACTIONS la
LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
  ON p.PROPOSAL_ID = la.PROPOSAL_ID
LEFT JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN latest
  ON latest.RUN_ID = p.BOARD_RUN_ID
WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
  AND la.SYMBOL IN ('DOW', 'PG')
ORDER BY la.SYMBOL, la.ACTION_ID;

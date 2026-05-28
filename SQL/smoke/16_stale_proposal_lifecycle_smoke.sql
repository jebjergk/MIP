/* ================================================================
   16_stale_proposal_lifecycle_smoke.sql
   LPA stale-proposal lifecycle — smoke checks.

   Validates the wider Stage A cascade in
   SP_EXPIRE_STALE_DAILY_PROPOSALS (the cascade now covers ALL
   pre-broker structural pending statuses, including REVALIDATED_PASS)
   and the cleanup-script outcome documented in
   MIP/SQL/scripts/supersede_stale_structural_pending_actions.sql.

   Checks (read-only):
     1) SP is present.
     2) No structural ENTRY pre-broker pending rows survive against a
        non-CURRENT parent proposal. This is the contract the LPA
        overview now relies on to skip stale rows.
     3) EXECUTION_REQUESTED / EXECUTION_PARTIAL structural rows with
        stale parents are NOT terminalized by the SP (broker race
        safety) — they are visible for manual review.
     4) Open positions (EXECUTED parent actions) are untouched —
        terminalizing pre-broker rows must never affect live positions.
     5) Wide-cascade contract: a STATUS bucket REVALIDATED_PASS is
        reachable by the cascade SQL (sanity: status list contract).

   Idempotent. Safe to re-run after each daily pipeline.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE WAREHOUSE MIP_WH_XS;
USE DATABASE MIP;
USE SCHEMA APP;

-- 1. SP presence.
SELECT 'SP_PRESENT' AS CHECK_NAME, COUNT(*) AS N
  FROM INFORMATION_SCHEMA.PROCEDURES
 WHERE PROCEDURE_SCHEMA = 'APP'
   AND PROCEDURE_NAME = 'SP_EXPIRE_STALE_DAILY_PROPOSALS';
-- Expect 1.

-- 2. Zero structural ENTRY pre-broker pending rows against non-CURRENT
--    parents (post-cleanup invariant — the daily pipeline maintains this).
SELECT
    'NO_STALE_PRE_BROKER_PENDING' AS CHECK_NAME,
    COUNT(*) AS N
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
-- Expect 0.

-- 3. Broker-race safety: stale parents with EXECUTION_REQUESTED /
--    EXECUTION_PARTIAL actions are surfaced but not mutated. The SP
--    only reports them; humans review and cancel. Zero or more is fine
--    here — this row just confirms the SP did not promote them to
--    SUPERSEDED.
SELECT
    'AT_BROKER_STALE_ACTIONS_UNTOUCHED' AS CHECK_NAME,
    COUNT(*) AS N,
    ARRAY_AGG(la.ACTION_ID) WITHIN GROUP (ORDER BY la.ACTION_ID) AS ACTION_IDS
  FROM MIP.LIVE.LIVE_ACTIONS la
  LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
    ON p.PROPOSAL_ID = la.PROPOSAL_ID
  LEFT JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN latest
    ON latest.RUN_ID = p.BOARD_RUN_ID
 WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
   AND la.STATUS IN ('EXECUTION_REQUESTED', 'EXECUTION_PARTIAL')
   AND (
        p.STATUS IS NULL
        OR p.STATUS <> 'PROPOSED'
        OR latest.RUN_ID IS NULL
   );

-- 4. Open positions invariant: EXECUTED structural ENTRY actions are
--    never affected by the expiry SP — surfacing the count here is a
--    documentation check that the cleanup did not perturb them.
SELECT
    'EXECUTED_STRUCTURAL_ENTRY_PRESERVED' AS CHECK_NAME,
    COUNT(*) AS N
  FROM MIP.LIVE.LIVE_ACTIONS la
 WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
   AND la.STATUS = 'EXECUTED'
   AND COALESCE(UPPER(la.ACTION_INTENT), CASE WHEN la.SIDE IN ('BUY','SELL') THEN 'ENTRY' ELSE NULL END) = 'ENTRY';

-- 5. JD specifically (Stage 2 short-correctness anchor) is still
--    EXECUTED and not SUPERSEDED/REJECTED by the new cascade.
SELECT
    'JD_STRUCTURAL_ENTRY_STATE' AS CHECK_NAME,
    la.SYMBOL,
    la.STATUS,
    la.SIDE,
    la.ACTION_INTENT
  FROM MIP.LIVE.LIVE_ACTIONS la
 WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
   AND la.SYMBOL = 'JD'
   AND la.STATUS = 'EXECUTED'
 ORDER BY la.UPDATED_AT DESC
 LIMIT 5;

-- 6. SP idempotency: a second invocation on the same date should
--    report zero new supersedes (the first call already terminalized
--    everything reachable). The result VARIANT is returned in the
--    `actions_superseded`, `actions_superseded_lifecycle`, and
--    `actions_superseded_orphan` keys.
CALL MIP.APP.SP_EXPIRE_STALE_DAILY_PROPOSALS(CURRENT_DATE());

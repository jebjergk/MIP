/* ================================================================
   restore_crm_proposal_3501_20260528.sql
   One-time recovery for CRM SHORT proposal 3501.

   Context:
     * Board run 2026-05-27 22:28 created proposal 3501 (CRM SHORT,
       EXECUTABLE) and the LPA importer correctly materialised
       LIVE_ACTION 76661906-4132-437d-9b82-740afcfa3a4f at status
       PENDING_OPEN_VALIDATION.
     * On 2026-05-28 15:13 the cleanup script
       supersede_stale_structural_pending_actions.sql (run as part of
       the LPA stale-proposal lifecycle work) called
       SP_EXPIRE_STALE_DAILY_PROPOSALS(CURRENT_DATE()). That SP's
       Rule 1 uses calendar-date comparison
       (CREATED_AT::DATE < CURRENT_DATE()) and therefore wrongly
       expired the CRM proposal even though no new evening pipeline
       has run yet. The action cascaded to STATUS=SUPERSEDED with
       reason code OLD_DAILY_PROPOSAL_EXPIRED.
     * The operator's intended lifecycle is "valid until the next
       evening pipeline produces fresh proposals" (board-run-based),
       not calendar-date-based.

   What this script does (and ONLY this):
     1) Restores STRUCTURAL_TRADE_PROPOSALS.STATUS to 'PROPOSED' for
        proposal 3501.
     2) Restores LIVE_ACTIONS.STATUS to 'PENDING_OPEN_VALIDATION' for
        action 76661906-4132-437d-9b82-740afcfa3a4f and strips the
        OLD_DAILY_PROPOSAL_EXPIRED reason code that the cascade added.
     3) Leaves ABBV (3502) and COIN (3503) alone — both are
        EXECUTION_POLICY_STATUS = GEOMETRY_INVALID research-only rows
        that were never import-eligible regardless.
     4) Verification SELECTs around the writes.

   Safe to re-run: each UPDATE has a tight predicate keyed on the
   exact ACTION_ID / PROPOSAL_ID + the SUPERSEDED/EXPIRED state, so
   running again is a no-op once the row has been restored.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

-- 1. BEFORE: confirm the row is in the expected SUPERSEDED/EXPIRED state.
SELECT
    'BEFORE_RESTORE_PROPOSAL' AS STEP,
    PROPOSAL_ID,
    SYMBOL,
    STATUS,
    BOARD_RUN_ID,
    CREATED_AT
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE PROPOSAL_ID = 3501;

SELECT
    'BEFORE_RESTORE_ACTION' AS STEP,
    ACTION_ID,
    SYMBOL,
    STATUS,
    COMMITTEE_STATUS,
    REASON_CODES
FROM MIP.LIVE.LIVE_ACTIONS
WHERE ACTION_ID = '76661906-4132-437d-9b82-740afcfa3a4f';

-- 2. Restore the proposal (only if still EXPIRED).
UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
   SET STATUS = 'PROPOSED'
 WHERE PROPOSAL_ID = 3501
   AND STATUS = 'EXPIRED';

-- 3. Restore the action and strip the cascade reason code.
--    ARRAY_REMOVE drops the OLD_DAILY_PROPOSAL_EXPIRED entry that
--    SP_EXPIRE appended. Any other reason codes the action accumulated
--    pre-supersede are preserved as-is.
UPDATE MIP.LIVE.LIVE_ACTIONS
   SET STATUS = 'PENDING_OPEN_VALIDATION',
       REASON_CODES = ARRAY_REMOVE(
                          COALESCE(REASON_CODES, ARRAY_CONSTRUCT()),
                          'OLD_DAILY_PROPOSAL_EXPIRED'::VARIANT
                      ),
       UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE ACTION_ID = '76661906-4132-437d-9b82-740afcfa3a4f'
   AND STATUS = 'SUPERSEDED';

-- 4. AFTER: confirm the row is back in actionable shape.
SELECT
    'AFTER_RESTORE_PROPOSAL' AS STEP,
    PROPOSAL_ID,
    SYMBOL,
    STATUS,
    BOARD_RUN_ID
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE PROPOSAL_ID = 3501;

SELECT
    'AFTER_RESTORE_ACTION' AS STEP,
    ACTION_ID,
    SYMBOL,
    STATUS,
    COMMITTEE_STATUS,
    REASON_CODES,
    UPDATED_AT
FROM MIP.LIVE.LIVE_ACTIONS
WHERE ACTION_ID = '76661906-4132-437d-9b82-740afcfa3a4f';

-- 5. Cross-check: confirm the restored row will be CURRENT per the
--    same lineage join LPA's overview uses, so it will show up in
--    pending_decisions immediately.
SELECT
    'POST_RESTORE_LPA_FRESHNESS' AS STEP,
    la.ACTION_ID,
    la.SYMBOL,
    la.STATUS,
    p.STATUS AS PROPOSAL_STATUS,
    latest.RUN_ID AS MATCHED_AUTH_RUN_ID,
    CASE
        WHEN p.STATUS = 'PROPOSED' AND latest.RUN_ID IS NOT NULL THEN 'CURRENT'
        WHEN p.STATUS = 'PROPOSED' THEN 'SUPERSEDED_BY_NEWER_RUN'
        ELSE 'EXPIRED'
    END AS PROPOSAL_FRESHNESS
FROM MIP.LIVE.LIVE_ACTIONS la
LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
  ON p.PROPOSAL_ID = la.PROPOSAL_ID
LEFT JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN latest
  ON latest.RUN_ID = p.BOARD_RUN_ID
WHERE la.ACTION_ID = '76661906-4132-437d-9b82-740afcfa3a4f';

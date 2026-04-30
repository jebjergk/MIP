/* ================================================================
   600_retire_pre_board_cutover_proposals.sql

   One-time cutover cleanup for the Agentic Proposal Board.

   Retires legacy structural trade proposals that were created by the
   pre-board deterministic selector and survived the cutover in
   STATUS='PROPOSED' without any board lineage. The board is now the
   sole production selector, so any active 'PROPOSED' row that was
   never written by SP_RUN_PROPOSAL_BOARD violates the contract that
   "every active proposal carries full board lineage" and also blocks
   its underlying setup event from being re-snapshotted by the board.

   Rules followed:
     * Identity preserved: PROPOSAL_ID, SETUP_EVENT_ID, SYMBOL,
       DIRECTION, SETUP_FAMILY, ENTRY_*, INVALIDATION_*, RATIONALE_TEXT,
       CREATED_AT are NOT modified.
     * COMMITTEE_PAYLOAD is augmented additively via OBJECT_INSERT
       (existing keys preserved). The cutover marker lives under a
       new 'cutover_retired' key.
     * STATUS is moved from 'PROPOSED' to 'EXPIRED' to match the
       existing status model (the only other observed status today).
     * BOARD_RATIONALE (currently NULL on these rows) records the
       human-readable reason: RETIRED_PRE_BOARD_CUTOVER_NO_BOARD_LINEAGE.
     * Live actions downstream of these proposals are NOT touched. The
       board snapshot dedupe logic checks LIVE_ACTIONS independently,
       so any setup event still tied to an in-flight live action will
       continue to be correctly skipped on the next board run.
     * MIP_AUDIT_LOG row is written for traceability.
     * Script is idempotent: re-running it is a no-op once all stale
       rows are already EXPIRED.

   This is NOT a fallback to the old selector. The retired rows
   remain in the table for full audit; only their active status is
   resolved.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE WAREHOUSE MIP_WH_XS;
USE SCHEMA APP;

-- 1) Pre-check snapshot. Lists exactly which rows match the retire
--    predicate before we mutate anything.
SELECT
    'PRE_CLEANUP_TARGETS' AS REPORT_SECTION,
    COUNT(*)              AS ROW_COUNT,
    ARRAY_AGG(PROPOSAL_ID) WITHIN GROUP (ORDER BY PROPOSAL_ID) AS PROPOSAL_IDS,
    ARRAY_AGG(SYMBOL)      WITHIN GROUP (ORDER BY PROPOSAL_ID) AS SYMBOLS
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE STATUS = 'PROPOSED'
  AND BOARD_RUN_ID IS NULL;

-- 2) Mutate + audit in a single Snowflake Scripting block so we can
--    bind one shared run_id / timestamp across the UPDATE and the
--    audit log insert. Wrapped in EXECUTE IMMEDIATE $$ ... $$ so the
--    multi-statement runner does not split on inner semicolons.
EXECUTE IMMEDIATE $$
DECLARE
    v_run_id     VARCHAR := UUID_STRING();
    v_event_ts   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_retired    NUMBER := 0;
BEGIN
    UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
       SET STATUS            = 'EXPIRED',
           BOARD_RATIONALE   = 'RETIRED_PRE_BOARD_CUTOVER_NO_BOARD_LINEAGE: '
                               || 'Created by pre-board deterministic selector on '
                               || TO_VARCHAR(CREATED_AT, 'YYYY-MM-DD HH24:MI:SS')
                               || '. Retired by '
                               || '600_retire_pre_board_cutover_proposals.sql '
                               || 'because the agentic proposal board is now the sole production selector.',
           COMMITTEE_PAYLOAD = OBJECT_INSERT(
               COALESCE(COMMITTEE_PAYLOAD, OBJECT_CONSTRUCT()),
               'cutover_retired',
               OBJECT_CONSTRUCT(
                   'reason',             'RETIRED_PRE_BOARD_CUTOVER_NO_BOARD_LINEAGE',
                   'retired_at',         :v_event_ts,
                   'retired_run_id',     :v_run_id,
                   'retired_by_script',  'MIP/SQL/scripts/600_retire_pre_board_cutover_proposals.sql',
                   'retired_from_status','PROPOSED',
                   'retired_to_status',  'EXPIRED',
                   'note',               'Identity, setup linkage, rationale, timestamps and original payload keys preserved.'
               ),
               TRUE
           )
     WHERE STATUS = 'PROPOSED'
       AND BOARD_RUN_ID IS NULL;

    v_retired := SQLROWCOUNT;

    INSERT INTO MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS, RUN_ID, PARENT_RUN_ID, EVENT_TYPE, EVENT_NAME, STATUS,
        ROWS_AFFECTED, DETAILS, INVOKED_BY_USER, INVOKED_BY_ROLE,
        INVOKED_WAREHOUSE, QUERY_ID, SESSION_ID
    )
    SELECT
        :v_event_ts,
        :v_run_id,
        NULL,
        'CUTOVER_CLEANUP',
        'RETIRE_PRE_BOARD_CUTOVER_PROPOSALS',
        'COMPLETE',
        :v_retired,
        OBJECT_CONSTRUCT(
            'reason_code',                'RETIRED_PRE_BOARD_CUTOVER_NO_BOARD_LINEAGE',
            'script',                     'MIP/SQL/scripts/600_retire_pre_board_cutover_proposals.sql',
            'predicate',                  'STATUS=PROPOSED AND BOARD_RUN_ID IS NULL',
            'from_status',                'PROPOSED',
            'to_status',                  'EXPIRED',
            'identity_preserved',         TRUE,
            'committee_payload_preserved',TRUE,
            'live_actions_touched',       FALSE
        ),
        CURRENT_USER(),
        CURRENT_ROLE(),
        CURRENT_WAREHOUSE(),
        LAST_QUERY_ID(),
        CURRENT_SESSION();

    RETURN OBJECT_CONSTRUCT(
        'reason_code',  'RETIRED_PRE_BOARD_CUTOVER_NO_BOARD_LINEAGE',
        'rows_retired', :v_retired,
        'run_id',       :v_run_id,
        'event_ts',     :v_event_ts
    );
END;
$$;

-- 3) Post-check: contract validation. MISSING_* must all be 0.
SELECT
    'POST_CLEANUP_LINEAGE' AS REPORT_SECTION,
    COUNT(*)                                    AS ACTIVE_PROPOSED,
    COUNT_IF(BOARD_RUN_ID IS NULL)              AS MISSING_RUN_ID,
    COUNT_IF(BOARD_CANDIDATE_ID IS NULL)        AS MISSING_CANDIDATE_ID,
    COUNT_IF(BOARD_FINAL_RANK IS NULL)          AS MISSING_RANK,
    COUNT_IF(BOARD_FINAL_VERDICT IS NULL)       AS MISSING_VERDICT,
    COUNT_IF(BOARD_PRIMARY_REASON_CODE IS NULL) AS MISSING_REASON
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE STATUS = 'PROPOSED';

-- 4) Post-check: list remaining active rows (should all be board-published).
SELECT
    'POST_CLEANUP_ACTIVE' AS REPORT_SECTION,
    PROPOSAL_ID,
    SYMBOL,
    STATUS,
    BOARD_RUN_ID,
    BOARD_FINAL_RANK,
    BOARD_FINAL_VERDICT,
    CREATED_AT
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE STATUS = 'PROPOSED'
ORDER BY BOARD_FINAL_RANK NULLS LAST, CREATED_AT;

-- 5) Confirm the retired rows now appear under STATUS='EXPIRED' with
--    intact identity, the new BOARD_RATIONALE, and an additive
--    'cutover_retired' marker inside COMMITTEE_PAYLOAD.
SELECT
    'POST_CLEANUP_RETIRED_DETAIL' AS REPORT_SECTION,
    PROPOSAL_ID,
    SYMBOL,
    STATUS,
    BOARD_RATIONALE,
    COMMITTEE_PAYLOAD:cutover_retired AS CUTOVER_RETIRED_BLOCK,
    COMMITTEE_PAYLOAD:setup_family    AS PRESERVED_SETUP_FAMILY,
    COMMITTEE_PAYLOAD:composite_score AS PRESERVED_COMPOSITE_SCORE_LEGACY,
    CREATED_AT
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE BOARD_RATIONALE LIKE 'RETIRED_PRE_BOARD_CUTOVER_NO_BOARD_LINEAGE%'
ORDER BY PROPOSAL_ID;

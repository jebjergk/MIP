/* ================================================================
   601_retire_duplicate_inflight_board_proposals.sql

   One-time follow-up cleanup for the Agentic Proposal Board.

   Retires any active board-published row in
   MIP.APP.STRUCTURAL_TRADE_PROPOSALS whose underlying SETUP_EVENT_ID
   already has an in-flight downstream MIP.LIVE.LIVE_ACTIONS row tied
   to a *different* (typically older / pre-cutover) PROPOSAL_ID.

   Background:
     The first production board run after the cutover cleanup
     (script 600) emitted duplicate proposals against five setups
     whose pre-cutover proposals had been retired to STATUS='EXPIRED'
     but whose live actions remained in flight (PORTFOLIO_ID = 1).
     The SP_RUN_PROPOSAL_BOARD live-actions dedup at that moment
     was using a COALESCE(la.PORTFOLIO_ID, -1) = COALESCE(:P_PORTFOLIO_ID, -1)
     pattern that failed open when the procedure was called with
     P_PORTFOLIO_ID = NULL but the live actions carried a concrete
     portfolio id. The procedure has been patched (561_sp_run_proposal_board.sql);
     this script clears the residual five duplicates so an operator
     cannot accidentally double-execute on the same setup event.

   Rules followed (mirror script 600):
     * Identity preserved: PROPOSAL_ID, SETUP_EVENT_ID, SYMBOL,
       DIRECTION, SETUP_FAMILY, ENTRY_*, INVALIDATION_*, RATIONALE_TEXT,
       CREATED_AT, BOARD_RUN_ID, BOARD_CANDIDATE_ID, BOARD_FINAL_RANK,
       BOARD_FINAL_VERDICT, BOARD_PRIMARY_REASON_CODE, BOARD_REASON_CODES,
       BOARD_RATIONALE, BOARD_PAYLOAD_JSON are NOT modified.
     * COMMITTEE_PAYLOAD is augmented additively via OBJECT_INSERT
       (existing keys preserved). The cutover marker lives under a
       new 'duplicate_inflight_retired' key.
     * STATUS moves from 'PROPOSED' to 'EXPIRED'.
     * Live actions remain untouched. The original (pre-cutover)
       proposal row that the live action references is unchanged.
     * MIP_AUDIT_LOG row is written.
     * Idempotent: re-running once duplicates are EXPIRED is a no-op.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE WAREHOUSE MIP_WH_XS;
USE SCHEMA APP;

-- 1) Pre-check: list the duplicate-inflight proposals we are about to retire.
WITH duplicates AS (
    SELECT
        p.PROPOSAL_ID,
        p.SYMBOL,
        p.SETUP_EVENT_ID,
        p.BOARD_RUN_ID,
        p.BOARD_FINAL_RANK,
        p.CREATED_AT,
        (SELECT MAX(la.STATUS)
         FROM MIP.LIVE.LIVE_ACTIONS la
         JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS op
           ON op.PROPOSAL_ID = la.PROPOSAL_ID
         WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
           AND op.SETUP_EVENT_ID = p.SETUP_EVENT_ID
           AND op.PROPOSAL_ID <> p.PROPOSAL_ID
           AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION',
                             'OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')) AS BLOCKING_LIVE_STATUS,
        (SELECT MAX(op.PROPOSAL_ID)
         FROM MIP.LIVE.LIVE_ACTIONS la
         JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS op
           ON op.PROPOSAL_ID = la.PROPOSAL_ID
         WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
           AND op.SETUP_EVENT_ID = p.SETUP_EVENT_ID
           AND op.PROPOSAL_ID <> p.PROPOSAL_ID
           AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION',
                             'OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')) AS BLOCKING_PROPOSAL_ID
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
    WHERE p.STATUS = 'PROPOSED'
      AND p.BOARD_RUN_ID IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM MIP.LIVE.LIVE_ACTIONS la
          JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS op
            ON op.PROPOSAL_ID = la.PROPOSAL_ID
          WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
            AND op.SETUP_EVENT_ID = p.SETUP_EVENT_ID
            AND op.PROPOSAL_ID <> p.PROPOSAL_ID
            AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION',
                              'OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')
      )
)
SELECT
    'PRE_CLEANUP_DUPLICATE_INFLIGHT' AS REPORT_SECTION,
    COUNT(*)                         AS ROW_COUNT,
    ARRAY_AGG(PROPOSAL_ID) WITHIN GROUP (ORDER BY PROPOSAL_ID) AS PROPOSAL_IDS,
    ARRAY_AGG(SYMBOL)      WITHIN GROUP (ORDER BY PROPOSAL_ID) AS SYMBOLS,
    ARRAY_AGG(BLOCKING_PROPOSAL_ID) WITHIN GROUP (ORDER BY PROPOSAL_ID) AS BLOCKING_OLDER_PROPOSAL_IDS,
    ARRAY_AGG(BLOCKING_LIVE_STATUS) WITHIN GROUP (ORDER BY PROPOSAL_ID) AS BLOCKING_LIVE_STATUSES
FROM duplicates;

-- 2) Mutate + audit in a single Snowflake Scripting block so we share
--    one run_id / timestamp across the UPDATE and the audit log insert.
EXECUTE IMMEDIATE $$
DECLARE
    v_run_id     VARCHAR := UUID_STRING();
    v_event_ts   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_retired    NUMBER := 0;
BEGIN
    UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
       SET STATUS            = 'EXPIRED',
           BOARD_RATIONALE   = COALESCE(BOARD_RATIONALE, '')
                               || CASE WHEN BOARD_RATIONALE IS NULL OR BOARD_RATIONALE = '' THEN '' ELSE ' | ' END
                               || 'RETIRED_DUPLICATE_OF_INFLIGHT_LIVE_ACTION: '
                               || 'Setup event '
                               || TO_VARCHAR(SETUP_EVENT_ID)
                               || ' already had an in-flight downstream live action under an older proposal at the time this board row was published. '
                               || 'Retired by 601_retire_duplicate_inflight_board_proposals.sql to prevent duplicate execution. '
                               || 'Original board lineage (BOARD_RUN_ID, BOARD_FINAL_RANK, BOARD_FINAL_VERDICT, BOARD_PRIMARY_REASON_CODE) preserved.',
           COMMITTEE_PAYLOAD = OBJECT_INSERT(
               COALESCE(COMMITTEE_PAYLOAD, OBJECT_CONSTRUCT()),
               'duplicate_inflight_retired',
               OBJECT_CONSTRUCT(
                   'reason',             'RETIRED_DUPLICATE_OF_INFLIGHT_LIVE_ACTION',
                   'retired_at',         :v_event_ts,
                   'retired_run_id',     :v_run_id,
                   'retired_by_script',  'MIP/SQL/scripts/601_retire_duplicate_inflight_board_proposals.sql',
                   'retired_from_status','PROPOSED',
                   'retired_to_status',  'EXPIRED',
                   'note',               'Identity, board lineage, setup linkage, rationale, timestamps and original payload keys preserved. Live actions and the older proposal that owns them are not touched.'
               ),
               TRUE
           )
     WHERE p.STATUS = 'PROPOSED'
       AND p.BOARD_RUN_ID IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM MIP.LIVE.LIVE_ACTIONS la
           JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS op
             ON op.PROPOSAL_ID = la.PROPOSAL_ID
           WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
             AND op.SETUP_EVENT_ID = p.SETUP_EVENT_ID
             AND op.PROPOSAL_ID <> p.PROPOSAL_ID
             AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION',
                               'OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')
       );

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
        'RETIRE_DUPLICATE_INFLIGHT_BOARD_PROPOSALS',
        'COMPLETE',
        :v_retired,
        OBJECT_CONSTRUCT(
            'reason_code',                'RETIRED_DUPLICATE_OF_INFLIGHT_LIVE_ACTION',
            'script',                     'MIP/SQL/scripts/601_retire_duplicate_inflight_board_proposals.sql',
            'predicate',                  'STATUS=PROPOSED AND BOARD_RUN_ID IS NOT NULL AND another older proposal for same SETUP_EVENT_ID has an in-flight LIVE_ACTION',
            'from_status',                'PROPOSED',
            'to_status',                  'EXPIRED',
            'identity_preserved',         TRUE,
            'board_lineage_preserved',    TRUE,
            'committee_payload_preserved',TRUE,
            'live_actions_touched',       FALSE,
            'sp_dedup_patch',             '561_sp_run_proposal_board.sql now uses NULL-aware portfolio match'
        ),
        CURRENT_USER(),
        CURRENT_ROLE(),
        CURRENT_WAREHOUSE(),
        LAST_QUERY_ID(),
        CURRENT_SESSION();

    RETURN OBJECT_CONSTRUCT(
        'reason_code',  'RETIRED_DUPLICATE_OF_INFLIGHT_LIVE_ACTION',
        'rows_retired', :v_retired,
        'run_id',       :v_run_id,
        'event_ts',     :v_event_ts
    );
END;
$$;

-- 3) Post-check: contract validation. MISSING_* must remain 0 and the
--    duplicate-vs-live-action invariant must be empty.
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

SELECT
    'POST_CLEANUP_DUPLICATE_INFLIGHT_RESIDUAL' AS REPORT_SECTION,
    COUNT(*) AS RESIDUAL_DUPLICATES
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
WHERE p.STATUS = 'PROPOSED'
  AND EXISTS (
      SELECT 1
      FROM MIP.LIVE.LIVE_ACTIONS la
      JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS op
        ON op.PROPOSAL_ID = la.PROPOSAL_ID
      WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
        AND op.SETUP_EVENT_ID = p.SETUP_EVENT_ID
        AND op.PROPOSAL_ID <> p.PROPOSAL_ID
        AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION',
                          'OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')
  );

-- 4) Post-check: list remaining active rows.
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

-- 5) Confirm the retired duplicate rows have intact board lineage and
--    the new 'duplicate_inflight_retired' marker.
SELECT
    'POST_CLEANUP_RETIRED_DETAIL' AS REPORT_SECTION,
    PROPOSAL_ID,
    SYMBOL,
    STATUS,
    BOARD_RUN_ID,
    BOARD_FINAL_RANK,
    BOARD_FINAL_VERDICT,
    BOARD_PRIMARY_REASON_CODE,
    BOARD_RATIONALE,
    COMMITTEE_PAYLOAD:duplicate_inflight_retired AS DUPLICATE_INFLIGHT_BLOCK,
    CREATED_AT
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE BOARD_RATIONALE LIKE '%RETIRED_DUPLICATE_OF_INFLIGHT_LIVE_ACTION%'
ORDER BY PROPOSAL_ID;

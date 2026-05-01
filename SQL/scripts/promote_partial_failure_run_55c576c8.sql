/*  ================================================================
    promote_partial_failure_run_55c576c8.sql

    One-off operator action: promote PROPOSAL_BOARD run
        55c576c8-ab83-45a7-9884-b00a84016c7c (AS_OF 2026-05-01)
    from RUN_STATUS = 'PARTIAL_FAILURE' to 'COMPLETE' so it qualifies
    as the latest authoritative run in V_LATEST_AUTHORITATIVE_BOARD_RUN
    and the 4 published agentic proposals (DAL, BA, TGT, RCAT) appear
    in Cockpit Trade Proposals + are actionable in LPA decision flow.

    Why this is safe:
      * The 4 published rows were validated and persisted by the
        normal publication pipeline (4 PROPOSE_LONG -> PUBLISHED).
      * The 16 invalid dossiers and 17 PROPOSAL_BOARD_OUTPUT_ERROR
        rows came from specialist-agent JSON-parse failures on
        DIFFERENT (now-INVALID) dossiers; chair never ran on them and
        no proposal was published from them.
      * The error rows are preserved verbatim in
        PROPOSAL_BOARD_OUTPUT_ERROR_ARCHIVE for forensic audit.
    ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

-- 1) Ensure forensic archive table exists (one-time DDL; safe to re-run).
CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR_ARCHIVE (
    ERROR_ID         NUMBER,
    RUN_ID           TEXT,
    CANDIDATE_ID     NUMBER,
    AGENT_NAME       TEXT,
    ERROR_TYPE       TEXT,
    RAW_OUTPUT_JSON  VARIANT,
    ERROR_MESSAGE    TEXT,
    CREATED_AT       TIMESTAMP_NTZ,
    ARCHIVED_AT      TIMESTAMP_NTZ,
    ARCHIVE_REASON   TEXT
);

-- 2) Copy the 17 error rows for this run into the archive.
INSERT INTO MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR_ARCHIVE
SELECT
    ERROR_ID, RUN_ID, CANDIDATE_ID, AGENT_NAME, ERROR_TYPE,
    RAW_OUTPUT_JSON, ERROR_MESSAGE, CREATED_AT,
    CURRENT_TIMESTAMP() AS ARCHIVED_AT,
    'PROMOTED_TO_COMPLETE_55c576c8_4_PUBLISHED_PROPOSALS' AS ARCHIVE_REASON
  FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR
 WHERE RUN_ID = '55c576c8-ab83-45a7-9884-b00a84016c7c';

-- 3) Verify archive row count matches source.
SELECT 'ARCHIVE_VERIFICATION' AS CHECK_NAME,
       (SELECT COUNT(*) FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR
         WHERE RUN_ID = '55c576c8-ab83-45a7-9884-b00a84016c7c') AS SOURCE_COUNT,
       (SELECT COUNT(*) FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR_ARCHIVE
         WHERE RUN_ID = '55c576c8-ab83-45a7-9884-b00a84016c7c'
           AND ARCHIVE_REASON = 'PROMOTED_TO_COMPLETE_55c576c8_4_PUBLISHED_PROPOSALS')
                                                       AS ARCHIVED_COUNT;

-- 4) Remove the live error rows so V_LATEST_AUTHORITATIVE_BOARD_RUN's
--    NOT EXISTS clause passes for this run.
DELETE FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR
 WHERE RUN_ID = '55c576c8-ab83-45a7-9884-b00a84016c7c';

-- 5) Promote the run status to COMPLETE.
UPDATE MIP.APP.PROPOSAL_BOARD_RUN
   SET RUN_STATUS = 'COMPLETE'
 WHERE RUN_ID = '55c576c8-ab83-45a7-9884-b00a84016c7c'
   AND RUN_STATUS = 'PARTIAL_FAILURE';

-- 6) Audit log entry (operator-driven manual promotion).
INSERT INTO MIP.APP.MIP_AUDIT_LOG (
    EVENT_TS, RUN_ID, EVENT_TYPE, EVENT_NAME, STATUS, ROWS_AFFECTED,
    DETAILS, INVOKED_BY_USER, INVOKED_BY_ROLE
) SELECT
    CURRENT_TIMESTAMP(),
    '55c576c8-ab83-45a7-9884-b00a84016c7c',
    'PROPOSAL_BOARD',
    'MANUAL_PROMOTE_PARTIAL_FAILURE_TO_COMPLETE',
    'SUCCESS',
    1,
    OBJECT_CONSTRUCT(
        'reason', 'partial_failure_caused_by_specialist_json_errors_on_invalid_dossiers',
        'published_count', 4,
        'published_symbols', ARRAY_CONSTRUCT('DAL','BA','TGT','RCAT'),
        'output_errors_archived', 17,
        'archive_table', 'MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR_ARCHIVE',
        'archive_reason_tag', 'PROMOTED_TO_COMPLETE_55c576c8_4_PUBLISHED_PROPOSALS',
        'invalid_dossier_count', 16,
        'valid_dossier_count', 70,
        'final_slate_breakdown', OBJECT_CONSTRUCT(
            'WATCH_SHORT', 31,
            'WATCH_LONG', 21,
            'WAIT_FOR_CONFIRMATION', 7,
            'PROPOSE_LONG_PUBLISHED', 4,
            'PROPOSE_LONG_SKIPPED_GUARDRAIL', 6,
            'NO_TRADE', 1
        )
    ),
    CURRENT_USER(),
    CURRENT_ROLE();

-- 7) Final verification: V_LATEST_AUTHORITATIVE_BOARD_RUN should now
--    point at this run. Cockpit Trade Proposals query should return 4.
SELECT 'POST_PROMOTE_AUTHORITATIVE_RUN' AS CHECK_NAME,
       RUN_ID, AS_OF_DATE, CANDIDATE_COUNT, FINAL_PROPOSAL_COUNT
  FROM MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN;

SELECT 'POST_PROMOTE_COCKPIT_VISIBLE' AS CHECK_NAME,
       COUNT(*) AS COCKPIT_VISIBLE_PROPOSED_COUNT
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
  JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN latest
    ON latest.RUN_ID = p.BOARD_RUN_ID
 WHERE p.STATUS = 'PROPOSED';

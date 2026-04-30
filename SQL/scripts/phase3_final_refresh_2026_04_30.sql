-- ============================================================
-- Phase 3 final controlled proposal refresh (2026-04-30)
--
-- Auditably retires every currently-active PROPOSED row (whether
-- it came from a pre-current-price transition run or from today's
-- post-fix smoke run) so the canonical Phase 3 run that follows
-- produces THE authoritative slate.
--
-- Audit reason code: RETIRED_PRE_CALIBRATION_PHASE3_REFRESH
-- (informational only; not in PROPOSAL_BOARD_REASON_CODE catalog,
--  it's a one-off audit-log marker for this controlled refresh).
--
-- This is a one-off maintenance script. Idempotent: re-running
-- after the canonical board has produced new PROPOSED rows would
-- expire those too, which is NOT the intent. Run once.
-- ============================================================

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

-- 1. Pre-snapshot of the rows we are about to retire.
INSERT INTO MIP.APP.MIP_AUDIT_LOG (
    EVENT_TYPE, EVENT_NAME, STATUS, ROWS_AFFECTED, DETAILS
)
SELECT
    'PROPOSAL_LIFECYCLE',
    'RETIRED_PRE_CALIBRATION_PHASE3_REFRESH',
    'INFO',
    COUNT(*),
    OBJECT_CONSTRUCT(
        'reason_code',         'RETIRED_PRE_CALIBRATION_PHASE3_REFRESH',
        'rationale',           'Phase 3 final controlled refresh: clearing transition / pre-current-price proposals so the canonical post-Step-5/Step-B run produces the authoritative slate.',
        'as_of_date',          CURRENT_DATE(),
        'proposal_ids',        ARRAY_AGG(PROPOSAL_ID),
        'distinct_board_runs', ARRAY_UNIQUE_AGG(BOARD_RUN_ID),
        'distinct_symbols',    ARRAY_UNIQUE_AGG(SYMBOL)
    )
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE STATUS = 'PROPOSED';

-- 2. Auditable expire: stamp the BOARD_RATIONALE so each row
-- carries the retirement reason code on its own.
UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
   SET STATUS          = 'EXPIRED',
       BOARD_RATIONALE = SUBSTR(
           '[' || TO_VARCHAR(CURRENT_TIMESTAMP()) || ' RETIRED_PRE_CALIBRATION_PHASE3_REFRESH] '
           || 'Retired by Phase 3 final controlled refresh (Step 5 hard validation + Step B current_price view fix). '
           || 'Original rationale: ' || COALESCE(BOARD_RATIONALE, '<none>'),
           1, 4000
       )
 WHERE STATUS = 'PROPOSED';

-- 3. Post-snapshot.
INSERT INTO MIP.APP.MIP_AUDIT_LOG (
    EVENT_TYPE, EVENT_NAME, STATUS, ROWS_AFFECTED, DETAILS
)
SELECT
    'PROPOSAL_LIFECYCLE',
    'RETIRED_PRE_CALIBRATION_PHASE3_REFRESH_DONE',
    'SUCCESS',
    (SELECT COUNT(*) FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
      WHERE BOARD_RATIONALE LIKE '[%RETIRED_PRE_CALIBRATION_PHASE3_REFRESH%'),
    OBJECT_CONSTRUCT(
        'reason_code',           'RETIRED_PRE_CALIBRATION_PHASE3_REFRESH',
        'remaining_active_rows', (SELECT COUNT(*) FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS WHERE STATUS = 'PROPOSED'),
        'as_of_date',            CURRENT_DATE()
    );

-- 4. Sanity checks: no rows left active.
SELECT 'remaining_active_proposed' AS CHECK_NAME, COUNT(*) AS N
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE STATUS = 'PROPOSED';

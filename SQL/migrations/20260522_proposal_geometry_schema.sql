/*  ================================================================
    20260522_proposal_geometry_schema.sql
    Direction / Geometry Fix — Schema Migration

    Changes:
      1. Drop NOT NULL constraint on SETUP_EVENT_ID so cross-direction
         evidence proposals can write NULL (direction mismatch stays in
         PRIMARY_EVIDENCE_SETUP_EVENT_ID only).
      2. Add EXECUTION_POLICY_STATUS — hard backend gate for LPA/API.
         Allowed values: EXECUTABLE | RESEARCH_ONLY | POLICY_BLOCKED |
                          BROKER_BLOCKED | RISK_BLOCKED | GEOMETRY_INVALID
      3. Add EXECUTION_POLICY_REASON — machine-readable block reason.
         Allowed values: SHORT_LIVE_DISABLED | FX_LIVE_DISABLED |
                          INVALID_LONG_GEOMETRY | INVALID_SHORT_GEOMETRY |
                          SETUP_EVENT_DIRECTION_MISMATCH | IBKR_NOT_PAPER
      4. Add IS_RESEARCH_ONLY BOOLEAN — human-readable flag driven by
         EXECUTION_POLICY_STATUS in {RESEARCH_ONLY, POLICY_BLOCKED,
         GEOMETRY_INVALID}.
      5. Explicit one-time backfill: existing rows with NULL status receive
         EXECUTABLE so no row is treated as blocked by default.
    ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;
USE WAREHOUSE MIP_WH_XS;

-- 1. Drop NOT NULL on SETUP_EVENT_ID
ALTER TABLE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ALTER COLUMN SETUP_EVENT_ID DROP NOT NULL;

-- 2. Add execution policy columns
ALTER TABLE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS EXECUTION_POLICY_STATUS  VARCHAR(30) DEFAULT 'EXECUTABLE';

ALTER TABLE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS EXECUTION_POLICY_REASON  VARCHAR(80);

ALTER TABLE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS IS_RESEARCH_ONLY         BOOLEAN     DEFAULT FALSE;

-- 3. Explicit backfill for existing rows where EXECUTION_POLICY_STATUS is NULL
--    (the column default only applies to new inserts)
UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
   SET EXECUTION_POLICY_STATUS = 'EXECUTABLE',
       IS_RESEARCH_ONLY        = FALSE
 WHERE EXECUTION_POLICY_STATUS IS NULL;

-- Verify: all rows should now have a non-null EXECUTION_POLICY_STATUS
SELECT
    EXECUTION_POLICY_STATUS,
    IS_RESEARCH_ONLY,
    COUNT(*) AS ROW_COUNT
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
GROUP BY EXECUTION_POLICY_STATUS, IS_RESEARCH_ONLY
ORDER BY ROW_COUNT DESC;

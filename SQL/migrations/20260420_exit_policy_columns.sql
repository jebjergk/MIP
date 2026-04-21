------------------------------------------------------------------------
-- 20260420 — Trailing Stop Phase 1 schema
--
-- Introduces EXIT_POLICY as a first-class execution contract on
-- LIVE_ACTIONS, EXIT_PROFILE bounded templates on STRUCTURAL_RISK_POLICY
-- (and per-proposal override on STRUCTURAL_TRADE_PROPOSALS), and two
-- APP_CONFIG safety gates: TRAIL_PHASE1_ENABLED (kill switch) and
-- TRAIL_REPLACEMENT_ENABLED (Phase 2 hard block).
--
-- LIVE_ACTIONS.TRAIL_PARAMS becomes the broker-executable shape for
-- TRAIL_BRACKET actions. STRUCTURAL_RISK_POLICY.TRAIL_PARAMS retains its
-- existing management-style semantics and is intentionally not changed.
------------------------------------------------------------------------

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE WAREHOUSE MIP_WH_XS;

------------------------------------------------------------------------
-- 1. LIVE_ACTIONS — execution contract columns
------------------------------------------------------------------------
ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ADD COLUMN IF NOT EXISTS EXIT_POLICY        VARCHAR(20);   -- FIXED_BRACKET | TRAIL_BRACKET

ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ADD COLUMN IF NOT EXISTS TRAIL_STATUS       VARCHAR(20);   -- NOT_REQUESTED | REQUESTED

ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ADD COLUMN IF NOT EXISTS EXIT_POLICY_REASON VARCHAR(255);  -- profile name or override note

------------------------------------------------------------------------
-- 2. STRUCTURAL_RISK_POLICY — bounded exit profile template
------------------------------------------------------------------------
ALTER TABLE MIP.APP.STRUCTURAL_RISK_POLICY
    ADD COLUMN IF NOT EXISTS EXIT_PROFILE       VARCHAR(30);   -- FIXED_STANDARD | TRAIL_TIGHT | TRAIL_STANDARD | TRAIL_WIDE

------------------------------------------------------------------------
-- 3. STRUCTURAL_TRADE_PROPOSALS — per-proposal override hook
------------------------------------------------------------------------
ALTER TABLE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS EXIT_PROFILE       VARCHAR(30);   -- per-proposal override; NULL means use policy

------------------------------------------------------------------------
-- 4. APP_CONFIG — Phase 1 kill switch + Phase 2 replacement gate
------------------------------------------------------------------------
MERGE INTO MIP.APP.APP_CONFIG t
USING (
    SELECT 'TRAIL_PHASE1_ENABLED'      AS CONFIG_KEY,
           'false'                     AS CONFIG_VALUE,
           'Global Phase 1 trailing stop rollout kill switch. If false, TRAIL_BRACKET actions are hard-blocked at execute_live_action with reason code TRAIL_PHASE1_DISABLED. Never silently downgrades to fixed STP.' AS DESCRIPTION
    UNION ALL
    SELECT 'TRAIL_REPLACEMENT_ENABLED',
           'false',
           'Phase 2 post-fill trail replacement (run_trail_activation). MUST remain false in Phase 1. The standalone-trail-after-cancel path has a known structural bug that creates a market exit alongside the trail. Do not enable until that bug is resolved.'
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
    VALUES (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, CURRENT_TIMESTAMP());

------------------------------------------------------------------------
-- 5. Backfill STRUCTURAL_RISK_POLICY → safe default profile
------------------------------------------------------------------------
UPDATE MIP.APP.STRUCTURAL_RISK_POLICY
   SET EXIT_PROFILE = 'FIXED_STANDARD'
 WHERE EXIT_PROFILE IS NULL
   AND IS_ACTIVE     = TRUE;

------------------------------------------------------------------------
-- 6. Verification
------------------------------------------------------------------------
SELECT 'LIVE_ACTIONS exit policy columns' AS CHECK_NAME,
       COUNT(*) AS COL_COUNT
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_SCHEMA = 'LIVE'
   AND TABLE_NAME   = 'LIVE_ACTIONS'
   AND COLUMN_NAME IN ('EXIT_POLICY', 'TRAIL_STATUS', 'EXIT_POLICY_REASON');

SELECT 'STRUCTURAL_RISK_POLICY exit profile column' AS CHECK_NAME,
       COUNT(*) AS COL_COUNT
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_SCHEMA = 'APP'
   AND TABLE_NAME   = 'STRUCTURAL_RISK_POLICY'
   AND COLUMN_NAME  = 'EXIT_PROFILE';

SELECT 'STRUCTURAL_TRADE_PROPOSALS exit profile column' AS CHECK_NAME,
       COUNT(*) AS COL_COUNT
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_SCHEMA = 'APP'
   AND TABLE_NAME   = 'STRUCTURAL_TRADE_PROPOSALS'
   AND COLUMN_NAME  = 'EXIT_PROFILE';

SELECT 'APP_CONFIG trailing safety flags' AS CHECK_NAME,
       COUNT(*) AS FLAG_COUNT
  FROM MIP.APP.APP_CONFIG
 WHERE CONFIG_KEY IN ('TRAIL_PHASE1_ENABLED', 'TRAIL_REPLACEMENT_ENABLED');

SELECT 'STRUCTURAL_RISK_POLICY EXIT_PROFILE backfill' AS CHECK_NAME,
       COUNT(*) AS ROWS_WITH_PROFILE
  FROM MIP.APP.STRUCTURAL_RISK_POLICY
 WHERE EXIT_PROFILE IS NOT NULL;

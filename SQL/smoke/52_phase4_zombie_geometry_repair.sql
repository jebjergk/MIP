-- =============================================================================
-- 52_phase4_zombie_geometry_repair.sql
-- One-time repair: reap zombie board runs + restore geometry-blocked proposals.
-- =============================================================================

USE ROLE MIP_ADMIN_ROLE;
USE WAREHOUSE MIP_WH_XS;
USE DATABASE MIP;
USE SCHEMA APP;

-- T52-A: Reap board runs stuck RUNNING > 90 minutes
UPDATE MIP.APP.PROPOSAL_BOARD_RUN
   SET RUN_STATUS = 'FAILED',
       FINISHED_AT = CURRENT_TIMESTAMP(),
       ERROR_JSON = OBJECT_CONSTRUCT(
           'reason_code', 'ZOMBIE_RUN_REAPED',
           'message', 'Run exceeded max age while RUNNING — marked failed by smoke repair.',
           'max_age_minutes', 90
       )
 WHERE RUN_STATUS = 'RUNNING'
   AND STARTED_AT < DATEADD('minute', -90, CURRENT_TIMESTAMP());

-- T52-B: Normalize structural invalidation to broker stop outside entry zone
UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
   SET PRICE_INVALIDATION_LEVEL = CASE
           WHEN p.DIRECTION = 'LONG'
                AND p.ENTRY_ZONE_LOW IS NOT NULL
                AND (
                    p.PRICE_INVALIDATION_LEVEL IS NULL
                    OR p.PRICE_INVALIDATION_LEVEL >= p.ENTRY_ZONE_LOW
                )
               THEN p.ENTRY_ZONE_LOW * 0.985
           WHEN p.DIRECTION = 'SHORT'
                AND p.ENTRY_ZONE_HIGH IS NOT NULL
                AND (
                    p.PRICE_INVALIDATION_LEVEL IS NULL
                    OR p.PRICE_INVALIDATION_LEVEL <= p.ENTRY_ZONE_HIGH
                )
               THEN p.ENTRY_ZONE_HIGH * 1.015
           ELSE p.PRICE_INVALIDATION_LEVEL
       END,
       INVALIDATION_RULE = CASE
           WHEN p.DIRECTION = 'LONG'
                AND p.ENTRY_ZONE_LOW IS NOT NULL
                AND p.PRICE_INVALIDATION_LEVEL IS NOT NULL
                AND p.PRICE_INVALIDATION_LEVEL >= p.ENTRY_ZONE_LOW
               THEN 'BROKER_STOP_BELOW_ZONE'
           WHEN p.DIRECTION = 'SHORT'
                AND p.ENTRY_ZONE_HIGH IS NOT NULL
                AND p.PRICE_INVALIDATION_LEVEL IS NOT NULL
                AND p.PRICE_INVALIDATION_LEVEL <= p.ENTRY_ZONE_HIGH
               THEN 'BROKER_STOP_ABOVE_ZONE'
           ELSE COALESCE(p.INVALIDATION_RULE, 'AGENTIC_INVALIDATION')
       END,
       EXECUTION_POLICY_STATUS = 'EXECUTABLE',
       EXECUTION_POLICY_REASON = NULL,
       IS_RESEARCH_ONLY = FALSE
 WHERE p.BOARD_RUN_ID = '2840aa14-38e5-43ff-b5f8-1d7ee5ae02b5'
   AND p.EXECUTION_POLICY_STATUS = 'GEOMETRY_INVALID';

-- T52-C: No zombie RUNNING rows older than 90 minutes
SELECT 'T52C_NO_ZOMBIE_RUNNING' AS CHECK_NAME,
       COUNT(*) AS zombie_count
  FROM MIP.APP.PROPOSAL_BOARD_RUN
 WHERE RUN_STATUS = 'RUNNING'
   AND STARTED_AT < DATEADD('minute', -90, CURRENT_TIMESTAMP());
-- Expect 0

-- T52-D: Run 2840aa14 executable proposals pass geometry gate
SELECT 'T52D_RUN2840_EXECUTABLE_GEOMETRY' AS CHECK_NAME,
       COUNT(*) AS bad_rows
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
 WHERE p.BOARD_RUN_ID = '2840aa14-38e5-43ff-b5f8-1d7ee5ae02b5'
   AND p.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
   AND NOT COALESCE(p.IS_RESEARCH_ONLY, FALSE)
   AND (
       (p.DIRECTION = 'LONG'
        AND p.PRICE_INVALIDATION_LEVEL IS NOT NULL
        AND p.ENTRY_ZONE_LOW IS NOT NULL
        AND p.PRICE_INVALIDATION_LEVEL >= p.ENTRY_ZONE_LOW)
       OR
       (p.DIRECTION = 'SHORT'
        AND p.PRICE_INVALIDATION_LEVEL IS NOT NULL
        AND p.ENTRY_ZONE_HIGH IS NOT NULL
        AND p.PRICE_INVALIDATION_LEVEL <= p.ENTRY_ZONE_HIGH)
       OR
       (p.ENTRY_ZONE_LOW IS NOT NULL AND p.ENTRY_ZONE_HIGH IS NOT NULL
        AND p.ENTRY_ZONE_LOW > p.ENTRY_ZONE_HIGH)
   );
-- Expect 0

-- T52-E: Run 2840aa14 executable count
SELECT 'T52E_RUN2840_EXECUTABLE_COUNT' AS CHECK_NAME,
       COUNT(*) AS executable_count
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
 WHERE p.BOARD_RUN_ID = '2840aa14-38e5-43ff-b5f8-1d7ee5ae02b5'
   AND p.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
   AND NOT COALESCE(p.IS_RESEARCH_ONLY, FALSE);
-- Expect 8

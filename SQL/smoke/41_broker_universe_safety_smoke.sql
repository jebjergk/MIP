-- 41_broker_universe_safety_smoke.sql
--
-- Smoke tests for Phase 1: Broker-Universe Safety Foundation
--
-- Verifies:
--   1. New schema columns exist on LIVE_PORTFOLIO_CONFIG
--   2. New schema columns exist on LIVE_ACTIONS
--   3. No executable LIVE_ACTIONS row has null/empty BROKER_NAME
--   4. No executable LIVE_ACTIONS row has null/empty IBKR_ACCOUNT_ID
--   5. No executable LIVE_ACTIONS row has null/empty BROKER_UNIVERSE_TYPE
--   6. Frozen context on LIVE_ACTIONS matches LIVE_PORTFOLIO_CONFIG (IBKR_ACCOUNT_ID + universe)
--   7. IS_EXECUTION_ENABLED = TRUE for the paper portfolio
--   8. No active portfolio config has IBKR_ACCOUNT_MODE = 'UNKNOWN'
--   9. IBKR_ACCOUNT_ID uniqueness expectation (advisory: Snowflake UNIQUEs are not enforced)
--  10. LIVE_PORTFOLIO_CONFIG.BROKER_NAME is non-null for all rows
--
-- "Executable" actions: those with STATUS not in terminal states.
-- Terminal states: EXECUTED, REJECTED, EXPIRED, TERMINATED, CANCELLED.
-- Non-terminal: all other statuses (e.g. PENDING_OPEN_VALIDATION, READY_FOR_APPROVAL_FLOW, SUBMITTED, etc.)

-- ── 1. Columns present on LIVE_PORTFOLIO_CONFIG ───────────────────────────────
-- Expected: BROKER_NAME, IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED all > 0 rows defined
SELECT
    SUM(CASE WHEN BROKER_NAME          IS NOT NULL THEN 1 ELSE 0 END)    AS LPC_BROKER_NAME_POPULATED,
    SUM(CASE WHEN IS_EXECUTION_ENABLED IS NOT NULL THEN 1 ELSE 0 END)    AS LPC_IS_EXEC_ENABLED_POPULATED,
    SUM(CASE WHEN REAL_MONEY_ENABLED   IS NOT NULL THEN 1 ELSE 0 END)    AS LPC_REAL_MONEY_ENABLED_POPULATED,
    COUNT(*) AS TOTAL_CONFIGS
  FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG;

-- ── 2. Columns present on LIVE_ACTIONS ───────────────────────────────────────
-- Expected: all three new columns populated on backfilled rows; zero nulls
SELECT
    COUNT(*)                                                              AS TOTAL_ACTIONS,
    SUM(CASE WHEN BROKER_NAME          IS NULL OR BROKER_NAME = ''          THEN 1 ELSE 0 END) AS NULL_BROKER_NAME,
    SUM(CASE WHEN IBKR_ACCOUNT_ID      IS NULL OR IBKR_ACCOUNT_ID = ''      THEN 1 ELSE 0 END) AS NULL_IBKR_ACCOUNT_ID,
    SUM(CASE WHEN BROKER_UNIVERSE_TYPE IS NULL OR BROKER_UNIVERSE_TYPE = '' THEN 1 ELSE 0 END) AS NULL_BROKER_UNIVERSE_TYPE
  FROM MIP.LIVE.LIVE_ACTIONS;

-- ── 3-5. No executable action missing any frozen context field ────────────────
-- Expected: 0 rows
SELECT
    ACTION_ID, PORTFOLIO_ID, STATUS,
    BROKER_NAME, IBKR_ACCOUNT_ID, BROKER_UNIVERSE_TYPE
  FROM MIP.LIVE.LIVE_ACTIONS
 WHERE STATUS NOT IN ('EXECUTED', 'REJECTED', 'EXPIRED', 'TERMINATED', 'CANCELLED')
   AND (
         BROKER_NAME          IS NULL OR BROKER_NAME = ''
      OR IBKR_ACCOUNT_ID      IS NULL OR IBKR_ACCOUNT_ID = ''
      OR BROKER_UNIVERSE_TYPE IS NULL OR BROKER_UNIVERSE_TYPE = ''
   )
 ORDER BY STATUS, PORTFOLIO_ID;

-- ── 6. Frozen context on executable actions must match portfolio config ────────
-- Expected: 0 mismatched rows
SELECT
    la.ACTION_ID,
    la.PORTFOLIO_ID,
    la.STATUS,
    la.IBKR_ACCOUNT_ID                    AS ACTION_IBKR_ACCOUNT_ID,
    lpc.IBKR_ACCOUNT_ID                   AS CONFIG_IBKR_ACCOUNT_ID,
    la.BROKER_UNIVERSE_TYPE               AS ACTION_UNIVERSE_TYPE,
    lpc.IBKR_ACCOUNT_MODE                 AS CONFIG_IBKR_ACCOUNT_MODE,
    la.BROKER_NAME                        AS ACTION_BROKER_NAME,
    lpc.BROKER_NAME                       AS CONFIG_BROKER_NAME
  FROM MIP.LIVE.LIVE_ACTIONS la
  JOIN MIP.LIVE.LIVE_PORTFOLIO_CONFIG lpc
    ON la.PORTFOLIO_ID = lpc.PORTFOLIO_ID
 WHERE la.STATUS NOT IN ('EXECUTED', 'REJECTED', 'EXPIRED', 'TERMINATED', 'CANCELLED')
   AND (
         la.IBKR_ACCOUNT_ID      != lpc.IBKR_ACCOUNT_ID
      OR la.BROKER_UNIVERSE_TYPE != COALESCE(lpc.IBKR_ACCOUNT_MODE, 'PAPER')
      OR la.BROKER_NAME          != COALESCE(lpc.BROKER_NAME, 'IBKR')
   )
 ORDER BY la.PORTFOLIO_ID;

-- ── 7. IS_EXECUTION_ENABLED = TRUE for PAPER portfolio(s) ────────────────────
-- Expected: all paper portfolios have IS_EXECUTION_ENABLED = TRUE
SELECT
    PORTFOLIO_ID,
    IBKR_ACCOUNT_ID,
    IBKR_ACCOUNT_MODE,
    IS_EXECUTION_ENABLED,
    REAL_MONEY_ENABLED,
    BROKER_NAME
  FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
 WHERE COALESCE(IBKR_ACCOUNT_MODE, 'UNKNOWN') = 'PAPER'
   AND COALESCE(IS_EXECUTION_ENABLED, FALSE) != TRUE;
-- Zero rows expected

-- ── 8. No active config with IBKR_ACCOUNT_MODE = 'UNKNOWN' ───────────────────
-- Expected: 0 rows
SELECT PORTFOLIO_ID, IBKR_ACCOUNT_ID, IBKR_ACCOUNT_MODE
  FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
 WHERE COALESCE(IS_ACTIVE, TRUE) = TRUE
   AND COALESCE(IBKR_ACCOUNT_MODE, 'UNKNOWN') = 'UNKNOWN';

-- ── 9. IBKR_ACCOUNT_ID uniqueness check (advisory) ───────────────────────────
-- Expected: every row has count=1 (no duplicate live accounts)
SELECT IBKR_ACCOUNT_ID, COUNT(*) AS DUPLICATE_COUNT
  FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
 WHERE COALESCE(IS_ACTIVE, TRUE) = TRUE
 GROUP BY IBKR_ACCOUNT_ID
HAVING COUNT(*) > 1;

-- ── 10. LIVE_PORTFOLIO_CONFIG.BROKER_NAME fully populated ────────────────────
-- Expected: 0 rows (every config has a broker name)
SELECT PORTFOLIO_ID, IBKR_ACCOUNT_ID
  FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
 WHERE BROKER_NAME IS NULL OR BROKER_NAME = '';

-- ── Summary ───────────────────────────────────────────────────────────────────
SELECT
    'PHASE1_SAFETY' AS TEST_SUITE,
    (SELECT COUNT(*)
       FROM MIP.LIVE.LIVE_ACTIONS
      WHERE STATUS NOT IN ('EXECUTED','REJECTED','EXPIRED','TERMINATED','CANCELLED')
        AND (BROKER_NAME IS NULL OR BROKER_NAME = ''
          OR IBKR_ACCOUNT_ID IS NULL OR IBKR_ACCOUNT_ID = ''
          OR BROKER_UNIVERSE_TYPE IS NULL OR BROKER_UNIVERSE_TYPE = ''))  AS INCOMPLETE_FROZEN_CTX,
    (SELECT COUNT(*)
       FROM MIP.LIVE.LIVE_ACTIONS la
       JOIN MIP.LIVE.LIVE_PORTFOLIO_CONFIG lpc ON la.PORTFOLIO_ID = lpc.PORTFOLIO_ID
      WHERE la.STATUS NOT IN ('EXECUTED','REJECTED','EXPIRED','TERMINATED','CANCELLED')
        AND (la.IBKR_ACCOUNT_ID != lpc.IBKR_ACCOUNT_ID
          OR la.BROKER_UNIVERSE_TYPE != COALESCE(lpc.IBKR_ACCOUNT_MODE,'PAPER')
          OR la.BROKER_NAME != COALESCE(lpc.BROKER_NAME,'IBKR')))          AS CONTEXT_MISMATCHES,
    (SELECT COUNT(*)
       FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
      WHERE COALESCE(IS_ACTIVE,TRUE)=TRUE
        AND COALESCE(IBKR_ACCOUNT_MODE,'UNKNOWN')='UNKNOWN')               AS UNKNOWN_ACCOUNT_MODE_CONFIGS,
    (SELECT COUNT(*)
       FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
      WHERE BROKER_NAME IS NULL OR BROKER_NAME = '')                        AS NULL_BROKER_NAME_CONFIGS;
-- All values should be 0 for a passing suite.

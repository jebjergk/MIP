-- ============================================================
-- Script: Add real IBKR account (U24621464) in read-only mode
-- Phase 2B — Real Account Read-Only Linkage
--
-- Safety guarantees (do NOT change these):
--   IS_EXECUTION_ENABLED = FALSE  → Phase 1 execution gate blocks all order submit
--   REAL_MONEY_ENABLED   = FALSE  → DB kill switch; requires explicit TRUE + env flag
--
-- ADAPTER_MODE = 'LIVE'  → Connects to the live (real money) IB Gateway.
--   This is semantically correct: ADAPTER_MODE describes the gateway mode,
--   not the execution permission. IS_EXECUTION_ENABLED=false is the hard gate.
--   Side-effect: _compute_live_activation_guard counts ADAPTER_MODE='LIVE' rows.
--   This is acceptable since this is a live gateway connection, even read-only.
--
-- IB_GATEWAY_PORT = 7496 → TWS real-money default port (vs 7497 for paper).
-- IB_CLIENT_ID = 9403    → Must not collide with paper client ID (9402 default).
--
-- This script is idempotent: the WHERE NOT EXISTS guard prevents duplicate inserts.
-- ============================================================

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

-- Step 1: Safety pre-check — confirm no existing row for U24621464
SELECT PORTFOLIO_ID, IBKR_ACCOUNT_ID, IBKR_ACCOUNT_MODE, IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
WHERE IBKR_ACCOUNT_ID = 'U24621464';
-- Expected: 0 rows. If a row already exists, do NOT re-insert.

-- Step 2: Confirm uniqueness invariant holds before insert
SELECT IBKR_ACCOUNT_ID, COUNT(*) AS ACTIVE_CONFIGS
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
WHERE IS_ACTIVE = TRUE
GROUP BY IBKR_ACCOUNT_ID
HAVING COUNT(*) > 1;
-- Expected: 0 rows.

-- Step 3: Insert the real account read-only config row
INSERT INTO MIP.LIVE.LIVE_PORTFOLIO_CONFIG (
    PORTFOLIO_ID,
    IBKR_ACCOUNT_ID,
    BROKER_NAME,
    ADAPTER_MODE,
    IBKR_ACCOUNT_MODE,
    BASE_CURRENCY,
    IS_ACTIVE,
    IS_EXECUTION_ENABLED,
    REAL_MONEY_ENABLED,
    IB_GATEWAY_HOST,
    IB_GATEWAY_PORT,
    IB_CLIENT_ID,
    -- Risk params: inherit paper portfolio defaults as safe starting point.
    -- These only matter if execution is ever enabled (which it is not for Phase 2B).
    MAX_POSITIONS,
    MAX_POSITION_PCT,
    CASH_BUFFER_PCT,
    MAX_SLIPPAGE_PCT,
    VALIDITY_WINDOW_SEC,
    QUOTE_FRESHNESS_THRESHOLD_SEC,
    SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
    DRAWDOWN_STOP_PCT,
    BUST_PCT,
    COOLDOWN_BARS,
    CONFIG_VERSION,
    CREATED_AT,
    UPDATED_AT
)
SELECT
    (SELECT COALESCE(MAX(PORTFOLIO_ID), 0) + 1 FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG),
    'U24621464',
    'IBKR',
    'LIVE',                -- live gateway (real account); distinct from IBKR_ACCOUNT_MODE
    'REAL',                -- real-money IBKR account
    'EUR',
    TRUE,                  -- IS_ACTIVE: visible and selectable in LPA
    FALSE,                 -- IS_EXECUTION_ENABLED: execution permanently disabled
    FALSE,                 -- REAL_MONEY_ENABLED: DB kill switch; do NOT change
    '127.0.0.1',           -- IB_GATEWAY_HOST: TWS on same machine
    7496,                  -- IB_GATEWAY_PORT: TWS real default
    9403,                  -- IB_CLIENT_ID: distinct from paper (9402)
    -- Risk params copied from paper portfolio (ID=1)
    MAX_POSITIONS,
    MAX_POSITION_PCT,
    CASH_BUFFER_PCT,
    MAX_SLIPPAGE_PCT,
    VALIDITY_WINDOW_SEC,
    QUOTE_FRESHNESS_THRESHOLD_SEC,
    SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
    DRAWDOWN_STOP_PCT,
    BUST_PCT,
    COOLDOWN_BARS,
    1,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
WHERE PORTFOLIO_ID = 1   -- paper portfolio as template for risk params
  AND NOT EXISTS (
    SELECT 1 FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG WHERE IBKR_ACCOUNT_ID = 'U24621464'
  );

-- Step 4: Verify the inserted row
SELECT PORTFOLIO_ID, IBKR_ACCOUNT_ID, BROKER_NAME, ADAPTER_MODE, IBKR_ACCOUNT_MODE,
       IS_ACTIVE, IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED,
       IB_GATEWAY_HOST, IB_GATEWAY_PORT, IB_CLIENT_ID
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
ORDER BY PORTFOLIO_ID;

-- Step 5: Re-check uniqueness invariant after insert
SELECT IBKR_ACCOUNT_ID, COUNT(*) AS ACTIVE_CONFIGS
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
WHERE IS_ACTIVE = TRUE
GROUP BY IBKR_ACCOUNT_ID
HAVING COUNT(*) > 1;
-- Expected: 0 rows.

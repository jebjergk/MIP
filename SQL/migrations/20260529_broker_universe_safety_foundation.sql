-- 20260529_broker_universe_safety_foundation.sql
--
-- Phase 1: Broker-universe safety foundation.
--
-- Goal:
--   Freeze the full broker-universe execution context onto every LIVE_ACTIONS
--   row at action-creation time. This makes it impossible for a future account
--   switch or config change to silently re-target an already-created action at
--   a different broker, account, or universe.
--
--   The four frozen fields form the canonical execution universe for an action:
--     PORTFOLIO_ID         (already present on LIVE_ACTIONS)
--     BROKER_NAME          (new — e.g. 'IBKR')
--     IBKR_ACCOUNT_ID      (new — e.g. 'DUQ101771')
--     BROKER_UNIVERSE_TYPE (new — 'PAPER' | 'REAL', from IBKR_ACCOUNT_MODE)
--
--   Execution must fail closed if any frozen field is missing or disagrees
--   with the live portfolio config at execute time.
--
-- LIVE_PORTFOLIO_CONFIG additions:
--   BROKER_NAME          STRING DEFAULT 'IBKR'   -- which broker this portfolio targets
--   IS_EXECUTION_ENABLED BOOLEAN DEFAULT FALSE    -- separates visibility from execution
--   REAL_MONEY_ENABLED   BOOLEAN DEFAULT FALSE    -- explicit real-money DB kill switch
--
-- LIVE_ACTIONS additions:
--   BROKER_NAME          STRING    -- frozen at action creation; must match config
--   IBKR_ACCOUNT_ID      STRING    -- frozen at action creation; must match config
--   BROKER_UNIVERSE_TYPE STRING    -- frozen at action creation; must match config IBKR_ACCOUNT_MODE
--
-- BROKER_UNIVERSE_TYPE DEFAULT 'PAPER' is for backward-compat backfill only.
-- Every new INSERT must supply an explicit value from the portfolio config.
--
-- Relationship to existing columns:
--   ADAPTER_MODE='LIVE'        means "use the IBKR broker submit path"
--                              (not "real money" — name is overloaded, not changed here)
--   IBKR_ACCOUNT_MODE='PAPER'  means "IBKR paper account" (added by 20260501 migration)
--   BROKER_UNIVERSE_TYPE       mirrors IBKR_ACCOUNT_MODE at action creation time,
--                              frozen and validated at execution time
--   IS_EXECUTION_ENABLED       a separate, explicit gate; must be TRUE for any order
--                              to reach the broker path regardless of ADAPTER_MODE

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

-- ===========================================================================
-- 1. LIVE_PORTFOLIO_CONFIG — add execution-context columns
-- ===========================================================================

ALTER TABLE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    ADD COLUMN IF NOT EXISTS BROKER_NAME STRING DEFAULT 'IBKR';

COMMENT ON COLUMN MIP.LIVE.LIVE_PORTFOLIO_CONFIG.BROKER_NAME IS
    'The broker system this portfolio is connected to. Currently always IBKR. Stored so execution context can be frozen onto action rows.';

ALTER TABLE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    ADD COLUMN IF NOT EXISTS IS_EXECUTION_ENABLED BOOLEAN DEFAULT FALSE;

COMMENT ON COLUMN MIP.LIVE.LIVE_PORTFOLIO_CONFIG.IS_EXECUTION_ENABLED IS
    'Explicit execution gate. Must be TRUE for any broker order to be submitted. Separates broker visibility (read-only link) from live execution. Default FALSE — new portfolios are read-only until explicitly enabled.';

ALTER TABLE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    ADD COLUMN IF NOT EXISTS REAL_MONEY_ENABLED BOOLEAN DEFAULT FALSE;

COMMENT ON COLUMN MIP.LIVE.LIVE_PORTFOLIO_CONFIG.REAL_MONEY_ENABLED IS
    'DB-level real-money kill switch. When IBKR_ACCOUNT_MODE=REAL, execution requires both REAL_MONEY_ENABLED=true here AND env ENABLE_REAL_MONEY_TRADING=true. Default FALSE — remains disabled until explicitly approved.';

-- Backfill BROKER_NAME for all existing rows.
UPDATE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
   SET BROKER_NAME = 'IBKR',
       UPDATED_AT  = CURRENT_TIMESTAMP()
 WHERE COALESCE(BROKER_NAME, '') = '';

-- Backfill IS_EXECUTION_ENABLED: existing PAPER portfolio retains its ability
-- to execute. Any row that currently runs as IBKR_ACCOUNT_MODE='PAPER' and
-- ADAPTER_MODE='LIVE' should stay executable.
UPDATE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
   SET IS_EXECUTION_ENABLED = TRUE,
       UPDATED_AT            = CURRENT_TIMESTAMP()
 WHERE COALESCE(IBKR_ACCOUNT_MODE, 'UNKNOWN') = 'PAPER'
   AND COALESCE(IS_EXECUTION_ENABLED, FALSE) = FALSE;

-- REAL_MONEY_ENABLED: intentionally left FALSE for all rows. No real-money
-- account exists; this column must be set manually when adding one.

-- ===========================================================================
-- 2. LIVE_ACTIONS — add frozen execution-context columns
-- ===========================================================================

ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ADD COLUMN IF NOT EXISTS BROKER_NAME STRING;

COMMENT ON COLUMN MIP.LIVE.LIVE_ACTIONS.BROKER_NAME IS
    'Frozen broker name at action creation time. Must match LIVE_PORTFOLIO_CONFIG.BROKER_NAME for the action PORTFOLIO_ID at execute time. Immutable after creation.';

ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ADD COLUMN IF NOT EXISTS IBKR_ACCOUNT_ID STRING;

COMMENT ON COLUMN MIP.LIVE.LIVE_ACTIONS.IBKR_ACCOUNT_ID IS
    'Frozen IBKR account ID at action creation time. Must match LIVE_PORTFOLIO_CONFIG.IBKR_ACCOUNT_ID for the action PORTFOLIO_ID at execute time. Immutable after creation.';

ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ADD COLUMN IF NOT EXISTS BROKER_UNIVERSE_TYPE STRING DEFAULT 'PAPER';

COMMENT ON COLUMN MIP.LIVE.LIVE_ACTIONS.BROKER_UNIVERSE_TYPE IS
    'Frozen broker universe type at action creation time (PAPER or REAL), sourced from LIVE_PORTFOLIO_CONFIG.IBKR_ACCOUNT_MODE. DEFAULT PAPER is for backward-compat backfill only — every new INSERT must supply an explicit value. Must match config IBKR_ACCOUNT_MODE at execute time.';

-- Backfill existing rows from LIVE_PORTFOLIO_CONFIG.
-- All 151 existing rows belong to PORTFOLIO_ID=1 / DUQ101771 / PAPER.
UPDATE MIP.LIVE.LIVE_ACTIONS la
   SET BROKER_NAME          = lpc.BROKER_NAME,
       IBKR_ACCOUNT_ID      = lpc.IBKR_ACCOUNT_ID,
       BROKER_UNIVERSE_TYPE = COALESCE(lpc.IBKR_ACCOUNT_MODE, 'PAPER')
  FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG lpc
 WHERE la.PORTFOLIO_ID = lpc.PORTFOLIO_ID
   AND la.BROKER_NAME IS NULL;

-- Defensive fallback: any row that still has no BROKER_NAME after the join
-- (e.g. orphaned actions with no matching config) gets 'IBKR' / 'PAPER'.
UPDATE MIP.LIVE.LIVE_ACTIONS
   SET BROKER_NAME          = COALESCE(BROKER_NAME, 'IBKR'),
       BROKER_UNIVERSE_TYPE = COALESCE(BROKER_UNIVERSE_TYPE, 'PAPER')
 WHERE BROKER_NAME IS NULL OR BROKER_UNIVERSE_TYPE IS NULL;

-- ===========================================================================
-- 3. Integrity verification queries (manual inspection after run)
-- ===========================================================================

-- Verify LIVE_PORTFOLIO_CONFIG backfill
SELECT PORTFOLIO_ID, IBKR_ACCOUNT_ID, IBKR_ACCOUNT_MODE,
       BROKER_NAME, IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED
  FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
 ORDER BY PORTFOLIO_ID;

-- Verify LIVE_ACTIONS backfill — should show 0 nulls
SELECT
    COUNT(*) AS TOTAL_ACTIONS,
    SUM(CASE WHEN BROKER_NAME IS NULL OR BROKER_NAME = '' THEN 1 ELSE 0 END) AS NULL_BROKER_NAME,
    SUM(CASE WHEN IBKR_ACCOUNT_ID IS NULL OR IBKR_ACCOUNT_ID = '' THEN 1 ELSE 0 END) AS NULL_IBKR_ACCOUNT_ID,
    SUM(CASE WHEN BROKER_UNIVERSE_TYPE IS NULL OR BROKER_UNIVERSE_TYPE = '' THEN 1 ELSE 0 END) AS NULL_BROKER_UNIVERSE_TYPE
  FROM MIP.LIVE.LIVE_ACTIONS;

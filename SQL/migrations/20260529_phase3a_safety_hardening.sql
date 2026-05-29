-- Phase 3A Safety Hardening — schema preparation
-- All ADD COLUMN IF NOT EXISTS (idempotent, safe to re-run).
-- No execution flags are changed; no IS_EXECUTION_ENABLED/REAL_MONEY_ENABLED altered.
-- Note: Snowflake requires separate ALTER TABLE per column for ADD COLUMN IF NOT EXISTS.

-- ─────────────────────────────────────────────────────────────
-- LIVE_PORTFOLIO_CONFIG: per-portfolio explicit safety flags
-- ─────────────────────────────────────────────────────────────
ALTER TABLE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    ADD COLUMN IF NOT EXISTS ALLOW_SHORT_SELLING BOOLEAN DEFAULT FALSE;

-- TRAIL_ENABLED is a real-money CERTIFICATION GATE, not a long-term prohibition.
-- Trailing stops are a required risk-management / profit-locking mechanism for this
-- operator workflow. Defaults to FALSE so real-money trailing cannot be placed
-- accidentally; flips to TRUE per portfolio once trailing placement, broker
-- persistence, reconciliation, cancel handling, and UI visibility are verified.
ALTER TABLE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    ADD COLUMN IF NOT EXISTS TRAIL_ENABLED BOOLEAN DEFAULT FALSE;

-- ─────────────────────────────────────────────────────────────
-- LIVE_ORDERS: frozen broker universe type for audit trail
-- ─────────────────────────────────────────────────────────────
ALTER TABLE MIP.LIVE.LIVE_ORDERS
    ADD COLUMN IF NOT EXISTS BROKER_UNIVERSE_TYPE TEXT DEFAULT NULL;

-- ─────────────────────────────────────────────────────────────
-- LIVE_ACTIONS: Phase 1 frozen context columns (add if missing)
-- Phase 1 migration already added these; IF NOT EXISTS is safe.
-- ─────────────────────────────────────────────────────────────
ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ADD COLUMN IF NOT EXISTS BROKER_NAME TEXT DEFAULT NULL;

ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ADD COLUMN IF NOT EXISTS IBKR_ACCOUNT_ID TEXT DEFAULT NULL;

ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ADD COLUMN IF NOT EXISTS BROKER_UNIVERSE_TYPE TEXT DEFAULT NULL;

-- Backfill from current config for any existing rows where
-- these are still null (pre-Phase-1 rows or migration gap).
-- Idempotent: WHERE clause only touches rows that still need it.
UPDATE MIP.LIVE.LIVE_ACTIONS a
SET    BROKER_NAME          = c.BROKER_NAME,
       IBKR_ACCOUNT_ID      = c.IBKR_ACCOUNT_ID,
       BROKER_UNIVERSE_TYPE = c.IBKR_ACCOUNT_MODE
FROM   MIP.LIVE.LIVE_PORTFOLIO_CONFIG c
WHERE  a.PORTFOLIO_ID = c.PORTFOLIO_ID
  AND  (
         a.BROKER_NAME          IS NULL
      OR a.IBKR_ACCOUNT_ID      IS NULL
      OR a.BROKER_UNIVERSE_TYPE IS NULL
  );

-- ─────────────────────────────────────────────────────────────
-- Verification
-- ─────────────────────────────────────────────────────────────
SELECT
    c.PORTFOLIO_ID,
    c.IBKR_ACCOUNT_ID,
    c.IBKR_ACCOUNT_MODE,
    c.IS_EXECUTION_ENABLED,
    c.REAL_MONEY_ENABLED,
    c.ALLOW_SHORT_SELLING,
    c.TRAIL_ENABLED
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG c
ORDER BY c.PORTFOLIO_ID;

SELECT COUNT(*) AS live_actions_missing_broker_ctx
FROM   MIP.LIVE.LIVE_ACTIONS
WHERE  BROKER_NAME IS NULL
    OR IBKR_ACCOUNT_ID IS NULL
    OR BROKER_UNIVERSE_TYPE IS NULL;

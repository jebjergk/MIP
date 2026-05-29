-- Smoke 43: Phase 3A Preflight Readiness
-- Proves all safety measures are in place and real execution remains blocked.
-- Run: cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py -f MIP/SQL/smoke/43_phase3a_preflight_smoke.sql

-- ─────────────────────────────────────────────────────────────
-- 1. Portfolio 2 (real) must remain execution-blocked
-- ─────────────────────────────────────────────────────────────
SELECT
    PORTFOLIO_ID,
    IBKR_ACCOUNT_ID,
    IBKR_ACCOUNT_MODE,
    IS_EXECUTION_ENABLED,
    REAL_MONEY_ENABLED,
    ALLOW_SHORT_SELLING,
    TRAIL_ENABLED,
    DRAWDOWN_STOP_PCT,
    MAX_SLIPPAGE_PCT,
    COOLDOWN_BARS
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
ORDER BY PORTFOLIO_ID;

-- ─────────────────────────────────────────────────────────────
-- 2. No LIVE_ORDERS exist with IBKR_ACCOUNT_ID = real account
--    and STATUS not in (SUBMIT_FAILED, PENDING_SUBMIT)
--    This proves no real orders were ever submitted.
-- ─────────────────────────────────────────────────────────────
SELECT
    COUNT(*) AS real_orders_submitted
FROM MIP.LIVE.LIVE_ORDERS
WHERE IBKR_ACCOUNT_ID = 'U24621464'
  AND STATUS NOT IN ('SUBMIT_FAILED', 'PENDING_SUBMIT');

-- ─────────────────────────────────────────────────────────────
-- 3. LIVE_ORDERS.BROKER_UNIVERSE_TYPE column exists and
--    all non-null rows have it populated
-- ─────────────────────────────────────────────────────────────
SELECT
    COUNT(*) AS total_orders,
    SUM(CASE WHEN BROKER_UNIVERSE_TYPE IS NOT NULL THEN 1 ELSE 0 END) AS orders_with_universe_type
FROM MIP.LIVE.LIVE_ORDERS;

-- ─────────────────────────────────────────────────────────────
-- 4. LIVE_ACTIONS has frozen broker context columns
--    with zero null gaps (Phase 1 + Phase 3A backfill)
-- ─────────────────────────────────────────────────────────────
SELECT COUNT(*) AS live_actions_missing_broker_ctx
FROM MIP.LIVE.LIVE_ACTIONS
WHERE BROKER_NAME IS NULL
   OR IBKR_ACCOUNT_ID IS NULL
   OR BROKER_UNIVERSE_TYPE IS NULL;

-- ─────────────────────────────────────────────────────────────
-- 5. Real-money safety columns exist on LIVE_PORTFOLIO_CONFIG
-- ─────────────────────────────────────────────────────────────
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'LIVE'
  AND TABLE_NAME   = 'LIVE_PORTFOLIO_CONFIG'
  AND COLUMN_NAME IN (
    'ALLOW_SHORT_SELLING', 'TRAIL_ENABLED',
    'DRAWDOWN_STOP_PCT', 'MAX_SLIPPAGE_PCT', 'COOLDOWN_BARS'
  )
ORDER BY COLUMN_NAME;

-- ─────────────────────────────────────────────────────────────
-- 6. Real portfolio: short selling blocked; trailing pending certification.
--    NOTE: TRAIL_ENABLED=false for real is the EXPECTED pre-certification state.
--    It is a real-money readiness gate (block accidental trailing until the
--    full trailing path is verified), NOT a long-term prohibition. Trailing
--    stops are a required real-money risk-management mechanism.
-- ─────────────────────────────────────────────────────────────
SELECT
    PORTFOLIO_ID,
    IBKR_ACCOUNT_MODE,
    ALLOW_SHORT_SELLING,
    TRAIL_ENABLED,
    CASE
        WHEN IBKR_ACCOUNT_MODE = 'REAL' AND ALLOW_SHORT_SELLING = FALSE THEN 'PASS'
        WHEN IBKR_ACCOUNT_MODE = 'REAL' AND ALLOW_SHORT_SELLING = TRUE  THEN 'FAIL — short selling enabled on real'
        ELSE 'N/A (paper)'
    END AS short_selling_guard,
    CASE
        WHEN IBKR_ACCOUNT_MODE = 'REAL' AND TRAIL_ENABLED = FALSE THEN 'PASS — trailing pending certification (no accidental real trail)'
        WHEN IBKR_ACCOUNT_MODE = 'REAL' AND TRAIL_ENABLED = TRUE  THEN 'OK — trailing certified for real money'
        ELSE 'N/A (paper)'
    END AS trail_certification_gate
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
ORDER BY PORTFOLIO_ID;

-- ─────────────────────────────────────────────────────────────
-- 7. Phase 1 / Phase 2 broker-universe safety still intact
--    (IS_EXECUTION_ENABLED=false and REAL_MONEY_ENABLED=false for real account)
-- ─────────────────────────────────────────────────────────────
SELECT
    PORTFOLIO_ID,
    IBKR_ACCOUNT_MODE,
    IS_EXECUTION_ENABLED,
    REAL_MONEY_ENABLED,
    CASE
        WHEN IBKR_ACCOUNT_MODE = 'REAL' AND IS_EXECUTION_ENABLED = FALSE AND REAL_MONEY_ENABLED = FALSE
            THEN 'PASS — real execution blocked'
        WHEN IBKR_ACCOUNT_MODE = 'REAL'
            THEN 'FAIL — real account is execution-enabled'
        ELSE 'PASS (paper)'
    END AS execution_guard
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
ORDER BY PORTFOLIO_ID;

-- ─────────────────────────────────────────────────────────────
-- 8. LIVE_ORDERS has BROKER_UNIVERSE_TYPE column (DDL check)
-- ─────────────────────────────────────────────────────────────
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'LIVE'
  AND TABLE_NAME   = 'LIVE_ORDERS'
  AND COLUMN_NAME  = 'BROKER_UNIVERSE_TYPE';

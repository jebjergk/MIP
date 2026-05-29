-- ============================================================
-- Migration: 20260529_live_config_gateway_params
-- Purpose: Add per-portfolio IB Gateway connection params to
--          LIVE_PORTFOLIO_CONFIG so each portfolio can connect
--          to a different IB Gateway / TWS port.
--
-- Motivation: Paper (port 7497) and real (port 7496) accounts
--   run on separate TWS instances. The snapshot sync script
--   (sync_ibkr_paper_snapshot.py) needs account-specific
--   host/port/client_id to route correctly.
--
-- Design:
--   NULL in all three columns → inherit env-var defaults (no
--     change for the existing paper portfolio, portfolio 1).
--   Non-null → used by _snapshot_sync_params_for_portfolio()
--     in live.py when resolving connection for a given portfolio.
--
-- Phase safety:
--   These columns do NOT affect execution gating.
--   IS_EXECUTION_ENABLED and REAL_MONEY_ENABLED remain the
--   authoritative execution gates.
-- ============================================================

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

ALTER TABLE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    ADD COLUMN IF NOT EXISTS IB_GATEWAY_HOST   STRING   DEFAULT NULL;

COMMENT ON COLUMN MIP.LIVE.LIVE_PORTFOLIO_CONFIG.IB_GATEWAY_HOST IS
    'IB Gateway / TWS hostname for this portfolio. NULL = use env-var default (IB_API_HOST / IBKR_SNAPSHOT_HOST / 127.0.0.1).';

ALTER TABLE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    ADD COLUMN IF NOT EXISTS IB_GATEWAY_PORT   INTEGER  DEFAULT NULL;

COMMENT ON COLUMN MIP.LIVE.LIVE_PORTFOLIO_CONFIG.IB_GATEWAY_PORT IS
    'IB Gateway / TWS port for this portfolio. NULL = use env-var default (IB_API_PORT / IBKR_SNAPSHOT_PORT). Paper TWS default: 7497. Real TWS default: 7496.';

ALTER TABLE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    ADD COLUMN IF NOT EXISTS IB_CLIENT_ID      INTEGER  DEFAULT NULL;

COMMENT ON COLUMN MIP.LIVE.LIVE_PORTFOLIO_CONFIG.IB_CLIENT_ID IS
    'IB client ID for snapshot reads on this portfolio. NULL = use env-var default (IB_CLIENT_ID_SNAPSHOT / IBKR_SNAPSHOT_CLIENT_ID / 9402). Must not collide with the paper portfolio client ID.';

-- Paper portfolio (ID=1) explicitly left with all three NULL so it
-- continues to inherit current env-var defaults without any change.

-- Verify
SELECT
    PORTFOLIO_ID,
    IBKR_ACCOUNT_ID,
    IB_GATEWAY_HOST,
    IB_GATEWAY_PORT,
    IB_CLIENT_ID
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
ORDER BY PORTFOLIO_ID;

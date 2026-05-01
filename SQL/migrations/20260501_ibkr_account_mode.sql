-- 20260501_ibkr_account_mode.sql
--
-- Add an explicit IBKR account-mode guard on MIP.LIVE.LIVE_PORTFOLIO_CONFIG.
--
-- Why:
--   In current MIP, ADAPTER_MODE='LIVE' means "use the IBKR broker submit path",
--   not "real money". ADAPTER_MODE='PAPER' means "internal MIP placeholder legs",
--   not "IBKR paper account". These names are overloaded and unsafe to use as
--   the short-publication safety gate.
--
--   The Phase 4 proposal board needs an explicit, persisted statement of
--   "which side of IBKR are we connected to" so that short publication can
--   fail closed unless we are demonstrably on an IBKR PAPER account.
--
-- Allowed values:
--   PAPER : IBKR paper-account-backed broker submit.
--   REAL  : real-money IBKR account.
--   UNKNOWN : explicit "we do not know" — must fail short publication closed.
--
-- This column is independent of ADAPTER_MODE and LIVE_EXECUTION_MODE.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

ALTER TABLE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    ADD COLUMN IF NOT EXISTS IBKR_ACCOUNT_MODE STRING DEFAULT 'UNKNOWN';

COMMENT ON COLUMN MIP.LIVE.LIVE_PORTFOLIO_CONFIG.IBKR_ACCOUNT_MODE IS 'Explicit IBKR account-mode guard. Allowed: PAPER, REAL, UNKNOWN. Independent of ADAPTER_MODE (which only encodes broker-submit path). Phase 4 short publication requires PAPER for the configured test portfolio.';

-- Backfill the configured test portfolio (1 / DUQ101771) as PAPER. Any other
-- portfolio without an explicit setting stays at UNKNOWN and will fail closed
-- on short publication.
UPDATE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
   SET IBKR_ACCOUNT_MODE = 'PAPER',
       UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE PORTFOLIO_ID = 1
   AND UPPER(COALESCE(IBKR_ACCOUNT_ID, '')) = 'DUQ101771'
   AND COALESCE(IBKR_ACCOUNT_MODE, 'UNKNOWN') <> 'PAPER';

-- Defensive integrity check that downstream code can rely on.
-- Rows where IBKR_ACCOUNT_MODE is not in the allowlist are treated as UNKNOWN.

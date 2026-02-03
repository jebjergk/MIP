-- reset_portfolio_to_initial.sql
-- =============================================================================
-- HARD RESET – Operational wipe for a single portfolio (quick & dirty).
-- =============================================================================
--
-- WHAT THIS WIPES (for the chosen portfolio_id only):
--   - MIP.APP.PORTFOLIO_POSITIONS   (open positions)
--   - MIP.APP.PORTFOLIO_TRADES      (trade history)
--   - MIP.APP.PORTFOLIO_DAILY       (daily equity/drawdown; feeds MART KPIs and risk views)
--
-- WHY: Reset-by-profile is not a real reset; it leaves portfolio_id history so the
-- UI still shows old drawdown/win/loss. This script wipes operational state so
-- after the next pipeline run the UI shows: equity = starting cash, drawdown = 0,
-- win/loss days = 0, and risk gate from current state (e.g. SAFE).
--
-- WHAT WE DO NOT CHANGE: PROFILE_ID, NAME, BASE_CURRENCY, STARTING_CASH (strategy
-- selection and starting capital are unchanged; this is operational reset only).
--
-- *** GUARD: Only run when the portfolio is blocked or you explicitly intend to
--     wipe all operational state. Run Phase 0 (dry run) first, then Phase 2.
-- =============================================================================
--
-- Run as MIP_ADMIN_ROLE. Set :portfolio_id below (one place); replace 1 with your
-- PORTFOLIO_ID if not using a variable in your client.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ========== CONFIG: portfolio to hard reset ==========
-- Replace 1 with your PORTFOLIO_ID in all places below, or set a variable in your client.
set reset_portfolio_id = 1;

-- =============================================================================
-- PHASE 0 – DRY RUN / PREVIEW (run this first; no changes)
-- =============================================================================
-- Row counts that will be deleted for this portfolio:

select 'PORTFOLIO_POSITIONS' as tbl, count(*) as cnt
  from MIP.APP.PORTFOLIO_POSITIONS
 where PORTFOLIO_ID = $reset_portfolio_id
union all
select 'PORTFOLIO_TRADES', count(*)
  from MIP.APP.PORTFOLIO_TRADES
 where PORTFOLIO_ID = $reset_portfolio_id
union all
select 'PORTFOLIO_DAILY', count(*)
  from MIP.APP.PORTFOLIO_DAILY
 where PORTFOLIO_ID = $reset_portfolio_id
union all
select 'PORTFOLIO_RISK_OVERRIDE', count(*)  -- comment out if table does not exist
  from MIP.APP.PORTFOLIO_RISK_OVERRIDE
 where PORTFOLIO_ID = $reset_portfolio_id;

-- Current portfolio header (before reset):
select PORTFOLIO_ID, PROFILE_ID, NAME, STATUS, STARTING_CASH, FINAL_EQUITY,
       MAX_DRAWDOWN, WIN_DAYS, LOSS_DAYS, BUST_AT
  from MIP.APP.PORTFOLIO
 where PORTFOLIO_ID = $reset_portfolio_id;

-- =============================================================================
-- PHASE 2 – DESTRUCTIVE RESET (run only after confirming Phase 0)
-- Order: POSITIONS -> TRADES -> DAILY -> risk override -> PORTFOLIO update
-- =============================================================================

begin;

delete from MIP.APP.PORTFOLIO_POSITIONS
 where PORTFOLIO_ID = $reset_portfolio_id;

delete from MIP.APP.PORTFOLIO_TRADES
 where PORTFOLIO_ID = $reset_portfolio_id;

delete from MIP.APP.PORTFOLIO_DAILY
 where PORTFOLIO_ID = $reset_portfolio_id;

delete from MIP.APP.PORTFOLIO_RISK_OVERRIDE  -- comment out if table does not exist
 where PORTFOLIO_ID = $reset_portfolio_id;

update MIP.APP.PORTFOLIO
   set LAST_SIMULATION_RUN_ID = null,
       LAST_SIMULATED_AT      = null,
       FINAL_EQUITY           = null,
       TOTAL_RETURN           = null,
       MAX_DRAWDOWN           = null,
       WIN_DAYS               = null,
       LOSS_DAYS              = null,
       STATUS                 = 'ACTIVE',
       BUST_AT                = null,
       UPDATED_AT             = CURRENT_TIMESTAMP()
 where PORTFOLIO_ID = $reset_portfolio_id;

commit;

-- =============================================================================
-- PHASE 3 – POST-RESET SMOKE CHECKS (expect 0 rows in child tables, clean header)
-- =============================================================================

select 'PORTFOLIO_POSITIONS (expect 0)' as check_name, count(*) as cnt
  from MIP.APP.PORTFOLIO_POSITIONS
 where PORTFOLIO_ID = $reset_portfolio_id
union all
select 'PORTFOLIO_TRADES (expect 0)', count(*)
  from MIP.APP.PORTFOLIO_TRADES
 where PORTFOLIO_ID = $reset_portfolio_id
union all
select 'PORTFOLIO_DAILY (expect 0)', count(*)
  from MIP.APP.PORTFOLIO_DAILY
 where PORTFOLIO_ID = $reset_portfolio_id;

select PORTFOLIO_ID, STATUS, STARTING_CASH, FINAL_EQUITY,
       MAX_DRAWDOWN, WIN_DAYS, LOSS_DAYS, BUST_AT
  from MIP.APP.PORTFOLIO
 where PORTFOLIO_ID = $reset_portfolio_id;
-- Expect: STATUS='ACTIVE', FINAL_EQUITY/MAX_DRAWDOWN/WIN_DAYS/LOSS_DAYS/BUST_AT all null.

-- Done. Next pipeline run will show equity = starting cash, drawdown = 0, win/loss = 0,
-- and risk gate from current constraints (e.g. SAFE) until new suggestions/trades exist.

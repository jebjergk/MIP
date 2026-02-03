-- reset_portfolio_to_initial.sql
-- Purpose: Reset a portfolio completely back to initial state and start over.
-- Deletes all trades, positions, and daily data for the portfolio, then resets
-- the PORTFOLIO row so the next simulation run starts fresh (same PORTFOLIO_ID,
-- same profile and starting cash).
--
-- Run as MIP_ADMIN_ROLE. Replace 1 with your PORTFOLIO_ID in all four places below.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Delete all child data for this portfolio
delete from MIP.APP.PORTFOLIO_TRADES    where PORTFOLIO_ID = 1;
delete from MIP.APP.PORTFOLIO_POSITIONS where PORTFOLIO_ID = 1;
delete from MIP.APP.PORTFOLIO_DAILY     where PORTFOLIO_ID = 1;

-- 2) Remove risk override if present (comment out if table PORTFOLIO_RISK_OVERRIDE does not exist)
delete from MIP.APP.PORTFOLIO_RISK_OVERRIDE where PORTFOLIO_ID = 1;

-- 3) Reset portfolio row to initial state (keep PROFILE_ID, NAME, STARTING_CASH, etc.)
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
 where PORTFOLIO_ID = 1;

-- Done. The portfolio is now in initial state; next pipeline/simulation run will start from scratch.

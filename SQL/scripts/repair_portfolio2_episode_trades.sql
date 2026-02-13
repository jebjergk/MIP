-- repair_portfolio2_episode_trades.sql
-- Purpose: Fix Portfolio 2's trades that were mis-assigned to the wrong episode
-- by the old backfill script (Step 3b).
--
-- The old Step 3b blindly assigned all orphan trades to the earliest episode,
-- causing trades from prior episodes to land in the current one. This poisons
-- cash recovery (stale CASH_AFTER), equity, return, and drawdown calculations.
--
-- This script:
--   1. Previews mis-assigned trades (TRADE_TS outside their episode window)
--   2. NULLs out mis-assigned EPISODE_IDs
--   3. Re-runs the correct episode assignment (timestamp-window based)
--   4. Resets simulation state so the next run starts fresh
--
-- SAFE TO RUN MULTIPLE TIMES (idempotent).
-- Run AFTER deploying the updated stored procedures.

use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- STEP 1: Preview mis-assigned trades
-- A trade is mis-assigned if its TRADE_TS is OUTSIDE the episode window
-- [START_TS, END_TS] for the EPISODE_ID it currently has.
-- =============================================================================
select
    t.TRADE_ID,
    t.PORTFOLIO_ID,
    t.EPISODE_ID,
    t.SYMBOL,
    t.SIDE,
    t.TRADE_TS,
    t.CASH_AFTER,
    e.START_TS as EPISODE_START,
    e.END_TS   as EPISODE_END,
    e.STATUS   as EPISODE_STATUS,
    case
        when t.TRADE_TS < e.START_TS then 'BEFORE_EPISODE'
        when e.END_TS is not null and t.TRADE_TS > e.END_TS then 'AFTER_EPISODE'
        else 'OK'
    end as ASSIGNMENT_STATUS
from MIP.APP.PORTFOLIO_TRADES t
join MIP.APP.PORTFOLIO_EPISODE e
  on e.PORTFOLIO_ID = t.PORTFOLIO_ID
 and e.EPISODE_ID = t.EPISODE_ID
where (
    t.TRADE_TS < e.START_TS
    or (e.END_TS is not null and t.TRADE_TS > e.END_TS)
)
order by t.PORTFOLIO_ID, t.TRADE_TS;

-- =============================================================================
-- STEP 2: NULL out EPISODE_ID on mis-assigned trades
-- This puts them back into "orphan" state so they can be re-assigned correctly.
-- =============================================================================
update MIP.APP.PORTFOLIO_TRADES t
   set EPISODE_ID = null
  from (
      select t2.TRADE_ID
      from MIP.APP.PORTFOLIO_TRADES t2
      join MIP.APP.PORTFOLIO_EPISODE e
        on e.PORTFOLIO_ID = t2.PORTFOLIO_ID
       and e.EPISODE_ID = t2.EPISODE_ID
      where t2.TRADE_TS < e.START_TS
         or (e.END_TS is not null and t2.TRADE_TS > e.END_TS)
  ) bad
where t.TRADE_ID = bad.TRADE_ID;

select 'Mis-assigned trades un-assigned' as action, $1 as rows_affected;

-- =============================================================================
-- STEP 3: Re-assign orphan trades to the correct episode (timestamp-window match)
-- Same logic as cleanup script Step 3a: match TRADE_TS within [START_TS, END_TS].
-- =============================================================================
update MIP.APP.PORTFOLIO_TRADES t
   set EPISODE_ID = e.EPISODE_ID
  from (
      select
          t2.TRADE_ID,
          ep.EPISODE_ID
      from MIP.APP.PORTFOLIO_TRADES t2
      join MIP.APP.PORTFOLIO_EPISODE ep
        on ep.PORTFOLIO_ID = t2.PORTFOLIO_ID
       and t2.TRADE_TS >= ep.START_TS
       and (ep.END_TS is null or t2.TRADE_TS <= ep.END_TS)
      where t2.EPISODE_ID is null
  ) e
where t.TRADE_ID = e.TRADE_ID
  and t.EPISODE_ID is null;

select 'Orphan trades re-assigned to correct episode' as action, $1 as rows_affected;

-- =============================================================================
-- STEP 4: Verify — show remaining orphans (should be 0 or only very old trades
-- that predate all episodes and are harmless).
-- =============================================================================
select
    PORTFOLIO_ID,
    count(*) as orphan_count,
    min(TRADE_TS) as earliest_trade,
    max(TRADE_TS) as latest_trade
from MIP.APP.PORTFOLIO_TRADES
where EPISODE_ID is null
group by PORTFOLIO_ID;

-- =============================================================================
-- STEP 5: Verify — show trades per episode after repair (sanity check).
-- Each episode should only have trades within its time window.
-- =============================================================================
select
    t.PORTFOLIO_ID,
    t.EPISODE_ID,
    e.START_TS as EP_START,
    e.END_TS as EP_END,
    e.STATUS,
    count(*) as trade_count,
    min(t.TRADE_TS) as first_trade,
    max(t.TRADE_TS) as last_trade,
    min(t.CASH_AFTER) as min_cash_after,
    max(t.CASH_AFTER) as max_cash_after
from MIP.APP.PORTFOLIO_TRADES t
join MIP.APP.PORTFOLIO_EPISODE e
  on e.PORTFOLIO_ID = t.PORTFOLIO_ID
 and e.EPISODE_ID = t.EPISODE_ID
group by t.PORTFOLIO_ID, t.EPISODE_ID, e.START_TS, e.END_TS, e.STATUS
order by t.PORTFOLIO_ID, e.START_TS;

-- =============================================================================
-- STEP 6: Reset LAST_SIMULATION_RUN_ID on affected portfolios so the next
-- simulation run starts fresh with correct cash from the repaired trades.
--
-- We identify affected portfolios as those that have trades whose TRADE_TS
-- falls outside their assigned episode window — i.e. the same predicate used
-- in Step 2 above. On a second run (after repair) this returns 0 rows, making
-- the UPDATE a no-op (idempotent).
-- =============================================================================
update MIP.APP.PORTFOLIO
   set LAST_SIMULATION_RUN_ID = null,
       UPDATED_AT = current_timestamp()
 where PORTFOLIO_ID in (
     select distinct t.PORTFOLIO_ID
     from MIP.APP.PORTFOLIO_TRADES t
     join MIP.APP.PORTFOLIO_EPISODE e
       on e.PORTFOLIO_ID = t.PORTFOLIO_ID
      and e.EPISODE_ID = t.EPISODE_ID
     where t.TRADE_TS < e.START_TS
        or (e.END_TS is not null and t.TRADE_TS > e.END_TS)
 );

select 'LAST_SIMULATION_RUN_ID reset on repaired portfolios' as action, $1 as rows_affected;

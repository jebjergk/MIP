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
-- STEP 6: Portfolio 1 — assign same-day orphans to episode 1
-- The simulation stamps TRADE_TS at midnight (bar date) but the episode was
-- created later that same day (08:52). Match on DATE instead of exact TS.
-- =============================================================================
-- Preview:
select 'Portfolio 1 same-day orphans to assign' as action,
       count(*) as trade_count
from MIP.APP.PORTFOLIO_TRADES
where PORTFOLIO_ID = 1
  and EPISODE_ID is null
  and date_trunc('day', TRADE_TS) = (
      select date_trunc('day', START_TS)
      from MIP.APP.PORTFOLIO_EPISODE
      where PORTFOLIO_ID = 1 and STATUS = 'ACTIVE'
  );

update MIP.APP.PORTFOLIO_TRADES
   set EPISODE_ID = (
       select EPISODE_ID
       from MIP.APP.PORTFOLIO_EPISODE
       where PORTFOLIO_ID = 1 and STATUS = 'ACTIVE'
   )
 where PORTFOLIO_ID = 1
   and EPISODE_ID is null
   and date_trunc('day', TRADE_TS) = (
       select date_trunc('day', START_TS)
       from MIP.APP.PORTFOLIO_EPISODE
       where PORTFOLIO_ID = 1 and STATUS = 'ACTIVE'
   );

select 'Portfolio 1 same-day orphans assigned to episode 1' as action, $1 as rows_affected;

-- =============================================================================
-- STEP 7: Portfolio 2 — delete old orphan trades (Nov-Dec 2025)
-- These belonged to phantom episodes that were already deleted. Their episodes
-- are gone, so these trades have no valid home and will never be needed.
-- Only deletes trades BEFORE the earliest surviving episode for portfolio 2.
-- =============================================================================
-- Preview:
select 'Portfolio 2 old orphan trades to delete' as action,
       count(*) as trade_count,
       min(TRADE_TS) as earliest,
       max(TRADE_TS) as latest
from MIP.APP.PORTFOLIO_TRADES
where PORTFOLIO_ID = 2
  and EPISODE_ID is null
  and TRADE_TS < (
      select min(START_TS)
      from MIP.APP.PORTFOLIO_EPISODE
      where PORTFOLIO_ID = 2
  );

delete from MIP.APP.PORTFOLIO_TRADES
where PORTFOLIO_ID = 2
  and EPISODE_ID is null
  and TRADE_TS < (
      select min(START_TS)
      from MIP.APP.PORTFOLIO_EPISODE
      where PORTFOLIO_ID = 2
  );

select 'Portfolio 2 old orphan trades deleted' as action, $1 as rows_affected;

-- =============================================================================
-- STEP 8: Portfolio 2 — assign agent trades (Feb 10) to episode 402
-- These were placed during the cooldown gap (before cooldown enforcement was
-- deployed). Episode 402 starts Feb 11 but these Feb 10 trades are the real
-- opening trades for this period. Nudge episode 402's START_TS back to cover
-- them, then assign.
-- =============================================================================
-- Preview:
select 'Portfolio 2 gap orphan trades to assign' as action,
       count(*) as trade_count,
       min(TRADE_TS) as earliest,
       max(TRADE_TS) as latest
from MIP.APP.PORTFOLIO_TRADES
where PORTFOLIO_ID = 2
  and EPISODE_ID is null
  and TRADE_TS >= '2026-02-01';  -- recent orphans only

-- 8a: Move episode 402 START_TS back to the earliest orphan trade TS
-- so the episode window covers these trades.
update MIP.APP.PORTFOLIO_EPISODE
   set START_TS = (
       select min(TRADE_TS)
       from MIP.APP.PORTFOLIO_TRADES
       where PORTFOLIO_ID = 2
         and EPISODE_ID is null
         and TRADE_TS >= '2026-02-01'
   )
 where PORTFOLIO_ID = 2
   and EPISODE_ID = 402
   and START_TS > (
       select min(TRADE_TS)
       from MIP.APP.PORTFOLIO_TRADES
       where PORTFOLIO_ID = 2
         and EPISODE_ID is null
         and TRADE_TS >= '2026-02-01'
   );

select 'Episode 402 START_TS moved back' as action, $1 as rows_affected;

-- 8b: Assign the orphan trades to episode 402
update MIP.APP.PORTFOLIO_TRADES
   set EPISODE_ID = 402
 where PORTFOLIO_ID = 2
   and EPISODE_ID is null
   and TRADE_TS >= (
       select START_TS
       from MIP.APP.PORTFOLIO_EPISODE
       where PORTFOLIO_ID = 2 and EPISODE_ID = 402
   );

select 'Portfolio 2 gap orphans assigned to episode 402' as action, $1 as rows_affected;

-- =============================================================================
-- STEP 9: Also delete orphan PORTFOLIO_POSITIONS for portfolio 2 that reference
-- trades from deleted episodes (positions without matching trades are stale).
-- Only deletes positions whose ENTRY_TS is before all surviving episodes.
-- =============================================================================
delete from MIP.APP.PORTFOLIO_POSITIONS
where PORTFOLIO_ID = 2
  and ENTRY_TS < (
      select min(START_TS)
      from MIP.APP.PORTFOLIO_EPISODE
      where PORTFOLIO_ID = 2
  )
  and not exists (
      select 1 from MIP.APP.PORTFOLIO_TRADES t
      where t.PORTFOLIO_ID = 2
        and t.SYMBOL = PORTFOLIO_POSITIONS.SYMBOL
        and t.SIDE = 'BUY'
        and t.TRADE_TS = PORTFOLIO_POSITIONS.ENTRY_TS
  );

select 'Portfolio 2 stale positions deleted' as action, $1 as rows_affected;

-- =============================================================================
-- STEP 10: Final verification — no more orphans
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
-- STEP 11: Reset LAST_SIMULATION_RUN_ID on both portfolios so the next
-- simulation starts fresh with correct cash.
-- =============================================================================
update MIP.APP.PORTFOLIO
   set LAST_SIMULATION_RUN_ID = null,
       UPDATED_AT = current_timestamp()
 where PORTFOLIO_ID in (1, 2);

select 'LAST_SIMULATION_RUN_ID reset on portfolios 1 and 2' as action, $1 as rows_affected;

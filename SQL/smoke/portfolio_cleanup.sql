-- portfolio_cleanup.sql
-- Purpose: Clean up corrupted portfolio data before re-deploying fixes.
-- Run this AFTER deploying the fixed SPs/views but BEFORE re-running the pipeline.
--
-- All statements use Snowflake-compatible SQL (no correlated subqueries in UPDATE SET).

use role MIP_ADMIN_ROLE;
use database MIP;

-- ============================================================
-- STEP 1: Remove duplicate positions (keep only one per position key)
-- Query 8 showed 9x duplicates for Portfolio 2.
-- ============================================================
-- Preview what duplicates exist:
select 'DUPLICATE POSITIONS' as check_name,
       PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_INDEX, count(*) as dup_count
from MIP.APP.PORTFOLIO_POSITIONS
group by PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_INDEX
having count(*) > 1
order by PORTFOLIO_ID, SYMBOL;

-- Delete duplicates: keep only the row with the latest RUN_ID per position.
-- Uses a CTE to identify rows to keep, then deletes the rest.
create or replace temporary table TEMP_POSITIONS_TO_KEEP as
select PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_INDEX, RUN_ID
from MIP.APP.PORTFOLIO_POSITIONS
qualify row_number() over (
    partition by PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_INDEX
    order by CREATED_AT desc nulls last
) = 1;

delete from MIP.APP.PORTFOLIO_POSITIONS p
using TEMP_POSITIONS_TO_KEEP k
where p.PORTFOLIO_ID = k.PORTFOLIO_ID
  and p.SYMBOL = k.SYMBOL
  and p.MARKET_TYPE = k.MARKET_TYPE
  and p.ENTRY_TS = k.ENTRY_TS
  and p.ENTRY_INDEX = k.ENTRY_INDEX
  and p.RUN_ID != k.RUN_ID;

-- Verify: should now have 0 duplicates
select 'REMAINING DUPLICATES' as check_name, count(*)
from (
    select PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_INDEX, count(*) as cnt
    from MIP.APP.PORTFOLIO_POSITIONS
    group by PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_INDEX
    having count(*) > 1
);

-- ============================================================
-- STEP 2: Remove orphaned PORTFOLIO_DAILY with null EPISODE_ID
-- These are legacy rows from before the episode system was introduced.
-- They contaminate stats for Portfolio 2 (64 days of old data).
-- ============================================================
-- Preview:
select 'DAILY ROWS TO DELETE (null episode)' as action,
       PORTFOLIO_ID, count(*) as cnt
from MIP.APP.PORTFOLIO_DAILY
where EPISODE_ID is null
group by PORTFOLIO_ID;

-- Execute:
delete from MIP.APP.PORTFOLIO_DAILY
where EPISODE_ID is null;

-- ============================================================
-- STEP 3: Create missing ACTIVE episode for Portfolio 2
-- Episode 102 was ended by crystallize but no new episode was created.
-- ============================================================
-- Check current state:
select 'ACTIVE EPISODES BEFORE' as check_name, PORTFOLIO_ID, EPISODE_ID, STATUS, START_TS
from MIP.APP.PORTFOLIO_EPISODE
where STATUS = 'ACTIVE';

-- Only create if Portfolio 2 has no active episode:
insert into MIP.APP.PORTFOLIO_EPISODE (
    PORTFOLIO_ID, PROFILE_ID, START_TS, END_TS, STATUS, END_REASON, START_EQUITY
)
select
    2 as PORTFOLIO_ID,
    p.PROFILE_ID,
    '2026-02-06 00:00:00.000'::timestamp_ntz as START_TS,
    null as END_TS,
    'ACTIVE' as STATUS,
    null as END_REASON,
    p.STARTING_CASH as START_EQUITY
from MIP.APP.PORTFOLIO p
where p.PORTFOLIO_ID = 2
  and not exists (
    select 1 from MIP.APP.PORTFOLIO_EPISODE e
    where e.PORTFOLIO_ID = 2 and e.STATUS = 'ACTIVE'
  );

-- Verify:
select 'ACTIVE EPISODES AFTER' as check_name, PORTFOLIO_ID, EPISODE_ID, STATUS, START_TS, START_EQUITY
from MIP.APP.PORTFOLIO_EPISODE
where STATUS = 'ACTIVE';

-- ============================================================
-- STEP 4: Backfill START_EQUITY on existing episodes where NULL
-- Episode 1 (Portfolio 1) has START_EQUITY = null.
-- ============================================================
merge into MIP.APP.PORTFOLIO_EPISODE e
using MIP.APP.PORTFOLIO p
  on p.PORTFOLIO_ID = e.PORTFOLIO_ID
 and e.START_EQUITY is null
when matched then
  update set START_EQUITY = p.STARTING_CASH;

-- Verify:
select 'EPISODE START_EQUITY' as check_name, PORTFOLIO_ID, EPISODE_ID, STATUS, START_EQUITY
from MIP.APP.PORTFOLIO_EPISODE
order by PORTFOLIO_ID, EPISODE_ID;

-- ============================================================
-- STEP 5: Backfill EPISODE_ID on trades where NULL
-- Map trades to episodes by matching PORTFOLIO_ID and timestamp range.
-- Uses MERGE (Snowflake-compatible) instead of correlated subqueries.
-- ============================================================
-- 5a. Assign trades to ACTIVE episodes (TRADE_TS >= episode START_TS)
merge into MIP.APP.PORTFOLIO_TRADES t
using (
    select PORTFOLIO_ID, EPISODE_ID, START_TS
    from MIP.APP.PORTFOLIO_EPISODE
    where STATUS = 'ACTIVE'
) e
on e.PORTFOLIO_ID = t.PORTFOLIO_ID
   and t.TRADE_TS >= e.START_TS
   and t.EPISODE_ID is null
when matched then
  update set EPISODE_ID = e.EPISODE_ID;

-- 5b. Assign remaining null trades to ENDED episodes (TRADE_TS between START_TS and END_TS)
merge into MIP.APP.PORTFOLIO_TRADES t
using (
    select PORTFOLIO_ID, EPISODE_ID, START_TS, END_TS
    from MIP.APP.PORTFOLIO_EPISODE
    where STATUS = 'ENDED'
) e
on e.PORTFOLIO_ID = t.PORTFOLIO_ID
   and t.TRADE_TS >= e.START_TS
   and t.TRADE_TS <= e.END_TS
   and t.EPISODE_ID is null
when matched then
  update set EPISODE_ID = e.EPISODE_ID;

-- Verify:
select 'TRADES BY EPISODE' as check_name,
       PORTFOLIO_ID, EPISODE_ID, count(*) as cnt
from MIP.APP.PORTFOLIO_TRADES
group by PORTFOLIO_ID, EPISODE_ID
order by PORTFOLIO_ID, EPISODE_ID;

-- ============================================================
-- STEP 6: Reset PORTFOLIO table stats (will be recomputed by pipeline)
-- ============================================================
update MIP.APP.PORTFOLIO
   set FINAL_EQUITY = STARTING_CASH,
       TOTAL_RETURN = 0,
       MAX_DRAWDOWN = 0,
       WIN_DAYS = 0,
       LOSS_DAYS = 0,
       BUST_AT = null,
       LAST_SIMULATION_RUN_ID = null,
       UPDATED_AT = current_timestamp();

-- Verify:
select 'PORTFOLIO STATS RESET' as check_name,
       PORTFOLIO_ID, NAME, STARTING_CASH, FINAL_EQUITY, TOTAL_RETURN, WIN_DAYS, LOSS_DAYS
from MIP.APP.PORTFOLIO;

-- ============================================================
-- STEP 7: Delete ALL existing PORTFOLIO_DAILY so pipeline starts fresh
-- This ensures no stale equity calculations (with EQUITY_VALUE=0 bug) remain.
-- The pipeline will recreate all daily rows with correct equity values.
-- ============================================================
-- Preview:
select 'PORTFOLIO_DAILY TO DELETE' as action,
       PORTFOLIO_ID, count(*) as cnt
from MIP.APP.PORTFOLIO_DAILY
group by PORTFOLIO_ID;

-- Execute:
delete from MIP.APP.PORTFOLIO_DAILY;

-- ============================================================
-- FINAL VERIFICATION
-- ============================================================
select 'EPISODES' as table_name, count(*) as cnt from MIP.APP.PORTFOLIO_EPISODE;
select 'ACTIVE EPISODES' as table_name, count(*) as cnt from MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE;
select 'POSITIONS' as table_name, count(*) as cnt from MIP.APP.PORTFOLIO_POSITIONS;
select 'TRADES' as table_name, count(*) as cnt from MIP.APP.PORTFOLIO_TRADES;
select 'DAILY ROWS' as table_name, count(*) as cnt from MIP.APP.PORTFOLIO_DAILY;
select 'EPISODE RESULTS' as table_name, count(*) as cnt from MIP.APP.PORTFOLIO_EPISODE_RESULTS;

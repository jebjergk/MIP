-- portfolio_diagnostic.sql
-- Purpose: Diagnose portfolio data integrity issues.
-- Run this BEFORE re-deploying the fixes to understand current state.
-- Run AFTER re-deploying + re-running pipeline to verify the fixes.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ============================================================
-- 1. PORTFOLIO table current stats (what the UI header shows)
-- ============================================================
select
    PORTFOLIO_ID,
    NAME,
    STARTING_CASH,
    FINAL_EQUITY,
    TOTAL_RETURN,
    MAX_DRAWDOWN,
    WIN_DAYS,
    LOSS_DAYS,
    STATUS,
    BUST_AT,
    LAST_SIMULATION_RUN_ID,
    LAST_SIMULATED_AT
from MIP.APP.PORTFOLIO
order by PORTFOLIO_ID;

-- ============================================================
-- 2. Episodes per portfolio — check for multiple/orphaned episodes
-- ============================================================
select
    e.PORTFOLIO_ID,
    e.EPISODE_ID,
    e.PROFILE_ID,
    e.STATUS,
    e.START_TS,
    e.END_TS,
    e.END_REASON,
    e.START_EQUITY,
    r.END_EQUITY,
    r.REALIZED_PNL,
    r.RETURN_PCT,
    r.DISTRIBUTION_AMOUNT,
    r.DISTRIBUTION_MODE
from MIP.APP.PORTFOLIO_EPISODE e
left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
  on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
order by e.PORTFOLIO_ID, e.START_TS;

-- ============================================================
-- 3. PORTFOLIO_DAILY row counts by portfolio, episode, run
--    Detects orphaned data or cross-episode contamination
-- ============================================================
select
    PORTFOLIO_ID,
    EPISODE_ID,
    RUN_ID,
    count(*) as day_count,
    min(TS) as first_ts,
    max(TS) as last_ts,
    min(TOTAL_EQUITY) as min_equity,
    max(TOTAL_EQUITY) as max_equity,
    sum(case when DAILY_PNL > 0 then 1 else 0 end) as win_days,
    sum(case when DAILY_PNL < 0 then 1 else 0 end) as loss_days,
    max(DRAWDOWN) as max_drawdown
from MIP.APP.PORTFOLIO_DAILY
group by PORTFOLIO_ID, EPISODE_ID, RUN_ID
order by PORTFOLIO_ID, first_ts desc;

-- ============================================================
-- 4. BAR_INDEX verification: confirm per-symbol BAR_INDEX is different
--    This was the root cause of the equity calculation bug
-- ============================================================
select
    SYMBOL,
    MARKET_TYPE,
    count(*) as bar_count,
    min(BAR_INDEX) as min_bar_index,
    max(BAR_INDEX) as max_bar_index,
    min(TS) as first_ts,
    max(TS) as last_ts
from MIP.MART.V_BAR_INDEX
where INTERVAL_MINUTES = 1440
group by SYMBOL, MARKET_TYPE
order by SYMBOL;

-- ============================================================
-- 5. PORTFOLIO_DAILY equity breakdown for latest day per portfolio
--    Check: is EQUITY_VALUE = 0 when there are open positions?
-- ============================================================
select
    d.PORTFOLIO_ID,
    d.TS,
    d.CASH,
    d.EQUITY_VALUE,
    d.TOTAL_EQUITY,
    d.OPEN_POSITIONS,
    d.DAILY_PNL,
    d.DRAWDOWN,
    d.EPISODE_ID,
    d.RUN_ID
from MIP.APP.PORTFOLIO_DAILY d
qualify row_number() over (partition by d.PORTFOLIO_ID order by d.TS desc, d.RUN_ID desc) = 1
order by d.PORTFOLIO_ID;

-- ============================================================
-- 6. Open positions with their BAR_INDEX values
--    Compare ENTRY_INDEX with TEMP_DAY_SPINE's BAR_INDEX
-- ============================================================
select
    p.PORTFOLIO_ID,
    p.SYMBOL,
    p.MARKET_TYPE,
    p.ENTRY_TS,
    p.ENTRY_INDEX,
    p.HOLD_UNTIL_INDEX,
    p.QUANTITY,
    p.COST_BASIS,
    p.EPISODE_ID,
    p.RUN_ID,
    vb_entry.BAR_INDEX as vb_entry_bar_index,
    vb_entry.CLOSE as entry_close,
    -- Current market value
    (select vb2.CLOSE
     from MIP.MART.V_BAR_INDEX vb2
     where vb2.SYMBOL = p.SYMBOL
       and vb2.MARKET_TYPE = p.MARKET_TYPE
       and vb2.INTERVAL_MINUTES = 1440
     order by vb2.TS desc
     limit 1) as latest_close,
    p.QUANTITY * (select vb2.CLOSE
     from MIP.MART.V_BAR_INDEX vb2
     where vb2.SYMBOL = p.SYMBOL
       and vb2.MARKET_TYPE = p.MARKET_TYPE
       and vb2.INTERVAL_MINUTES = 1440
     order by vb2.TS desc
     limit 1) as expected_equity_value
from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL p
left join MIP.MART.V_BAR_INDEX vb_entry
  on vb_entry.SYMBOL = p.SYMBOL
 and vb_entry.MARKET_TYPE = p.MARKET_TYPE
 and vb_entry.INTERVAL_MINUTES = 1440
 and vb_entry.BAR_INDEX = p.ENTRY_INDEX
order by p.PORTFOLIO_ID, p.SYMBOL;

-- ============================================================
-- 7. Trades per portfolio/episode
-- ============================================================
select
    PORTFOLIO_ID,
    EPISODE_ID,
    count(*) as trade_count,
    sum(case when SIDE = 'BUY' then 1 else 0 end) as buys,
    sum(case when SIDE = 'SELL' then 1 else 0 end) as sells,
    min(TRADE_TS) as first_trade,
    max(TRADE_TS) as last_trade
from MIP.APP.PORTFOLIO_TRADES
group by PORTFOLIO_ID, EPISODE_ID
order by PORTFOLIO_ID, EPISODE_ID;

-- ============================================================
-- 8. Duplicate positions check (same symbol, same entry, multiple runs)
-- ============================================================
select
    PORTFOLIO_ID,
    SYMBOL,
    MARKET_TYPE,
    ENTRY_TS,
    ENTRY_INDEX,
    count(*) as dup_count,
    count(distinct RUN_ID) as run_count,
    listagg(RUN_ID, ', ') within group (order by RUN_ID) as run_ids
from MIP.APP.PORTFOLIO_POSITIONS
group by PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_INDEX
having count(*) > 1
order by PORTFOLIO_ID, SYMBOL;

-- ============================================================
-- OPTIONAL CLEANUP (run only if diagnostics show problems)
-- ============================================================

-- 9. Remove duplicate positions (keep the latest RUN_ID only)
-- UNCOMMENT to run:
/*
delete from MIP.APP.PORTFOLIO_POSITIONS
where (PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_INDEX, RUN_ID)
  not in (
    select PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_INDEX,
           max(RUN_ID) as keep_run
    from MIP.APP.PORTFOLIO_POSITIONS
    group by PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_INDEX
  );
*/

-- 10. Remove orphaned PORTFOLIO_DAILY from non-active episodes
--     (only runs this for portfolios that have active episodes)
-- UNCOMMENT to run:
/*
delete from MIP.APP.PORTFOLIO_DAILY d
where exists (
    select 1 from MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e
    where e.PORTFOLIO_ID = d.PORTFOLIO_ID
)
and d.EPISODE_ID is not null
and d.EPISODE_ID not in (
    select EPISODE_ID from MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE
    where PORTFOLIO_ID = d.PORTFOLIO_ID
);
*/

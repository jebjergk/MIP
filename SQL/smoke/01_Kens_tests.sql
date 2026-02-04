use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================

-- Q0a: Check portfolio header - what does it say?
select PORTFOLIO_ID, NAME, STARTING_CASH, FINAL_EQUITY, 
       STARTING_CASH - coalesce(FINAL_EQUITY, STARTING_CASH) as CASH_LOST,
       LAST_SIMULATION_RUN_ID, LAST_SIMULATED_AT
from MIP.APP.PORTFOLIO
where PORTFOLIO_ID = 1;

-- Q0b: Check episode - did it start with full $100k?
select e.EPISODE_ID, e.PORTFOLIO_ID, e.START_TS, e.STATUS, e.START_EQUITY,
       p.STARTING_CASH,
       p.STARTING_CASH - coalesce(e.START_EQUITY, p.STARTING_CASH) as DISCREPANCY
from MIP.APP.PORTFOLIO_EPISODE e
join MIP.APP.PORTFOLIO p on p.PORTFOLIO_ID = e.PORTFOLIO_ID
where e.PORTFOLIO_ID = 1 and e.STATUS = 'ACTIVE';

-- Q0c: Check PORTFOLIO_DAILY for this episode - any cash changes?
select d.TS, d.RUN_ID, d.CASH, d.EQUITY_VALUE, d.TOTAL_EQUITY, d.OPEN_POSITIONS,
       lag(d.CASH) over (partition by d.PORTFOLIO_ID order by d.TS) as PREV_CASH,
       d.CASH - lag(d.CASH) over (partition by d.PORTFOLIO_ID order by d.TS) as CASH_CHANGE
from MIP.APP.PORTFOLIO_DAILY d
join MIP.APP.PORTFOLIO_EPISODE e on e.PORTFOLIO_ID = d.PORTFOLIO_ID
where d.PORTFOLIO_ID = 1
  and e.STATUS = 'ACTIVE'
  and d.TS >= e.START_TS
order by d.TS desc
limit 20;

-- Q0d: Check PORTFOLIO_TRADES for this episode - any trades at all?
select t.TRADE_ID, t.TRADE_TS, t.SYMBOL, t.SIDE, t.QUANTITY, t.PRICE, 
       t.COST_BASIS, t.TOTAL_COST, t.FEES, t.RUN_ID
from MIP.APP.PORTFOLIO_TRADES t
join MIP.APP.PORTFOLIO_EPISODE e on e.PORTFOLIO_ID = t.PORTFOLIO_ID
where t.PORTFOLIO_ID = 1
  and e.STATUS = 'ACTIVE'
  and t.TRADE_TS >= e.START_TS
order by t.TRADE_TS desc;

-- Q0e: Check if there are orphan positions from BEFORE the episode
select p.POSITION_ID, p.SYMBOL, p.ENTRY_TS, p.QUANTITY, p.COST_BASIS,
       e.START_TS as EPISODE_START,
       case when p.ENTRY_TS < e.START_TS then 'ORPHAN_PRE_EPISODE' else 'IN_EPISODE' end as STATUS
from MIP.APP.PORTFOLIO_POSITIONS p
join MIP.APP.PORTFOLIO_EPISODE e on e.PORTFOLIO_ID = p.PORTFOLIO_ID and e.STATUS = 'ACTIVE'
where p.PORTFOLIO_ID = 1
order by p.ENTRY_TS desc
limit 20;

-- Q0f: What was the FIRST PORTFOLIO_DAILY row after episode start?
-- This shows what cash value the simulation started with
select d.*
from MIP.APP.PORTFOLIO_DAILY d
join MIP.APP.PORTFOLIO_EPISODE e on e.PORTFOLIO_ID = d.PORTFOLIO_ID and e.STATUS = 'ACTIVE'
where d.PORTFOLIO_ID = 1
  and d.TS >= e.START_TS
order by d.TS asc
limit 5;

-- Q0g: ALL PORTFOLIO_DAILY rows for portfolio 1 (including before episode)
-- Check if old data is still there affecting calculations
select 
    d.RUN_ID, 
    d.TS, 
    d.CASH, 
    d.TOTAL_EQUITY, 
    d.PEAK_EQUITY, 
    d.DRAWDOWN,
    e.START_TS as EPISODE_START,
    case when d.TS < e.START_TS then 'BEFORE_EPISODE' else 'IN_EPISODE' end as STATUS
from MIP.APP.PORTFOLIO_DAILY d
cross join (select START_TS from MIP.APP.PORTFOLIO_EPISODE where PORTFOLIO_ID = 1 and STATUS = 'ACTIVE') e
where d.PORTFOLIO_ID = 1
order by d.TS desc
limit 30;

-- Q0h: How many PORTFOLIO_DAILY rows exist for portfolio 1?
select 
    count(*) as TOTAL_ROWS,
    count_if(d.TS >= e.START_TS) as IN_EPISODE,
    count_if(d.TS < e.START_TS) as BEFORE_EPISODE,
    max(d.PEAK_EQUITY) as MAX_PEAK_EQUITY_ALL_TIME
from MIP.APP.PORTFOLIO_DAILY d
cross join (select START_TS from MIP.APP.PORTFOLIO_EPISODE where PORTFOLIO_ID = 1 and STATUS = 'ACTIVE') e
where d.PORTFOLIO_ID = 1;

-- =============================================================================
-- CLEANUP: Delete pre-episode PORTFOLIO_DAILY rows (run this to fix the data)
-- =============================================================================

-- Step 1: Preview what will be deleted
select 
    d.PORTFOLIO_ID,
    count(*) as ROWS_TO_DELETE,
    min(d.TS) as MIN_TS,
    max(d.TS) as MAX_TS
from MIP.APP.PORTFOLIO_DAILY d
join MIP.APP.PORTFOLIO_EPISODE e 
  on e.PORTFOLIO_ID = d.PORTFOLIO_ID and e.STATUS = 'ACTIVE'
where d.TS < e.START_TS
group by d.PORTFOLIO_ID;

-- Step 2: Delete rows from BEFORE the active episode for ALL portfolios
-- UNCOMMENT TO RUN:
/*
delete from MIP.APP.PORTFOLIO_DAILY d
using MIP.APP.PORTFOLIO_EPISODE e
where d.PORTFOLIO_ID = e.PORTFOLIO_ID
  and e.STATUS = 'ACTIVE'
  and d.TS < e.START_TS;
*/

-- Step 3: Also delete PORTFOLIO_TRADES and PORTFOLIO_POSITIONS from before episode
-- Preview:
select 
    'TRADES' as TABLE_NAME,
    t.PORTFOLIO_ID,
    count(*) as ROWS_TO_DELETE
from MIP.APP.PORTFOLIO_TRADES t
join MIP.APP.PORTFOLIO_EPISODE e 
  on e.PORTFOLIO_ID = t.PORTFOLIO_ID and e.STATUS = 'ACTIVE'
where t.TRADE_TS < e.START_TS
group by t.PORTFOLIO_ID
union all
select 
    'POSITIONS' as TABLE_NAME,
    p.PORTFOLIO_ID,
    count(*) as ROWS_TO_DELETE
from MIP.APP.PORTFOLIO_POSITIONS p
join MIP.APP.PORTFOLIO_EPISODE e 
  on e.PORTFOLIO_ID = p.PORTFOLIO_ID and e.STATUS = 'ACTIVE'
where p.ENTRY_TS < e.START_TS
group by p.PORTFOLIO_ID;

-- Step 4: Delete old trades and positions (UNCOMMENT TO RUN):
/*
delete from MIP.APP.PORTFOLIO_TRADES t
using MIP.APP.PORTFOLIO_EPISODE e
where t.PORTFOLIO_ID = e.PORTFOLIO_ID
  and e.STATUS = 'ACTIVE'
  and t.TRADE_TS < e.START_TS;

delete from MIP.APP.PORTFOLIO_POSITIONS p
using MIP.APP.PORTFOLIO_EPISODE e
where p.PORTFOLIO_ID = e.PORTFOLIO_ID
  and e.STATUS = 'ACTIVE'
  and p.ENTRY_TS < e.START_TS;
*/

-- Step 5: After cleanup, re-run the pipeline to get fresh data with correct metrics
-- call MIP.APP.SP_RUN_DAILY_PIPELINE();

-- =============================================================================

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


select *
from MIP.MART.V_PORTFOLIO_RISK_STATE
where PORTFOLIO_ID in (1,2)
order by AS_OF_TS desc
limit 20;

select PORTFOLIO_ID, RUN_ID_VARCHAR, count(*) as PROPOSALS,
       min(proposed_AT) as MIN_CREATED_AT, max(proposed_AT) as MAX_CREATED_AT
from MIP.AGENT_OUT.ORDER_PROPOSALS
where PORTFOLIO_ID in (1,2)
group by 1,2
order by MAX_CREATED_AT desc
limit 50;

select PORTFOLIO_ID, RUN_ID_VARCHAR, STATUS, count(*) as CNT
from MIP.AGENT_OUT.ORDER_PROPOSALS
where PORTFOLIO_ID in (1,2)
group by 1,2,3
order by 2 desc, 1, 3;

select PORTFOLIO_ID, RUN_ID, count(*) as TRADES,
       min(CREATED_AT) as MIN_CREATED_AT, max(CREATED_AT) as MAX_CREATED_AT
from MIP.APP.PORTFOLIO_TRADES
where PORTFOLIO_ID in (1,2)
group by 1,2
order by MAX_CREATED_AT desc
limit 50;

select PORTFOLIO_ID, RUN_ID, count(*) as OPEN_POSITIONS
from MIP.APP.PORTFOLIO_POSITIONS
where PORTFOLIO_ID in (1,2)
group by 1,2
order by 2 desc
limit 50;

-- =============================================================================
-- DIAGNOSTIC: Why no trades showing?
-- =============================================================================

-- 1. Compare PORTFOLIO.LAST_SIMULATION_RUN_ID vs latest audit log run
select 
    p.PORTFOLIO_ID,
    p.NAME,
    p.LAST_SIMULATION_RUN_ID as portfolio_run_id,
    p.LAST_SIMULATED_AT as portfolio_run_ts,
    audit.RUN_ID as audit_latest_run_id,
    audit.EVENT_TS as audit_latest_ts,
    case when p.LAST_SIMULATION_RUN_ID = audit.RUN_ID then 'MATCH' else 'MISMATCH' end as run_id_match
from MIP.APP.PORTFOLIO p
cross join (
    select RUN_ID, EVENT_TS
    from MIP.APP.MIP_AUDIT_LOG
    where EVENT_TYPE = 'PIPELINE' and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
      and STATUS in ('SUCCESS', 'SUCCESS_WITH_SKIPS')
    order by EVENT_TS desc
    limit 1
) audit
where p.STATUS = 'ACTIVE';

-- 2. Check PORTFOLIO_DAILY for recent data
select 
    PORTFOLIO_ID, 
    RUN_ID, 
    TS,
    TOTAL_EQUITY,
    CASH,
    EQUITY_VALUE,
    OPEN_POSITIONS
from MIP.APP.PORTFOLIO_DAILY
where PORTFOLIO_ID in (1,2)
order by TS desc
limit 20;

-- 3. Equity changes explained: Mark-to-market on existing positions
-- If positions exist but no new trades, equity changes from price movements
select 
    pos.PORTFOLIO_ID,
    pos.RUN_ID,
    count(*) as position_count,
    sum(pos.COST_BASIS) as total_cost_basis,
    sum(pos.QUANTITY * pos.ENTRY_PRICE) as entry_value
from MIP.APP.PORTFOLIO_POSITIONS pos
where pos.PORTFOLIO_ID in (1,2)
group by 1,2
order by 2 desc
limit 10;

-- 4. Check canonical open positions (what the UI should show)
select *
from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
where PORTFOLIO_ID in (1,2)
order by PORTFOLIO_ID, ENTRY_TS desc;

-- 5. DIAGNOSTIC: Why are positions not showing as "open"?
-- Check HOLD_UNTIL_INDEX vs CURRENT_BAR_INDEX
with latest_bar as (
    select
        TS as AS_OF_TS,
        BAR_INDEX as CURRENT_BAR_INDEX
    from MIP.MART.V_BAR_INDEX
    where INTERVAL_MINUTES = 1440
    qualify row_number() over (order by TS desc, BAR_INDEX desc) = 1
)
select 
    p.PORTFOLIO_ID,
    p.SYMBOL,
    p.ENTRY_TS,
    p.ENTRY_INDEX,
    p.HOLD_UNTIL_INDEX,
    b.CURRENT_BAR_INDEX,
    b.AS_OF_TS as MARKET_AS_OF,
    p.HOLD_UNTIL_INDEX >= b.CURRENT_BAR_INDEX as IS_OPEN_BY_BAR,
    e.START_TS as EPISODE_START_TS,
    p.ENTRY_TS >= e.START_TS as IS_IN_EPISODE_SCOPE
from MIP.APP.PORTFOLIO_POSITIONS p
cross join latest_bar b
left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e on e.PORTFOLIO_ID = p.PORTFOLIO_ID
where p.PORTFOLIO_ID in (1,2)
order by p.PORTFOLIO_ID, p.ENTRY_TS desc
limit 30;

-- 6. Check active episodes and their start dates
select 
    PORTFOLIO_ID,
    EPISODE_ID,
    START_TS,
    STATUS
from MIP.APP.PORTFOLIO_EPISODE
where PORTFOLIO_ID in (1,2)
  and STATUS = 'ACTIVE';

-- 7. Check the latest bar index and date
select * from (
    select TS, BAR_INDEX, INTERVAL_MINUTES
    from MIP.MART.V_BAR_INDEX
    where INTERVAL_MINUTES = 1440
    order by TS desc
    limit 5
);


-- Why are positions not showing as "open"?
with latest_bar as (
    select
        TS as AS_OF_TS,
        BAR_INDEX as CURRENT_BAR_INDEX
    from MIP.MART.V_BAR_INDEX
    where INTERVAL_MINUTES = 1440
    qualify row_number() over (order by TS desc, BAR_INDEX desc) = 1
)
select 
    p.PORTFOLIO_ID,
    p.SYMBOL,
    p.ENTRY_TS,
    p.ENTRY_INDEX,
    p.HOLD_UNTIL_INDEX,
    b.CURRENT_BAR_INDEX,
    b.AS_OF_TS as MARKET_AS_OF,
    p.HOLD_UNTIL_INDEX >= b.CURRENT_BAR_INDEX as IS_OPEN_BY_BAR,
    e.START_TS as EPISODE_START_TS,
    p.ENTRY_TS >= coalesce(e.START_TS, '1900-01-01') as IS_IN_EPISODE_SCOPE
from MIP.APP.PORTFOLIO_POSITIONS p
cross join latest_bar b
left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e on e.PORTFOLIO_ID = p.PORTFOLIO_ID
where p.PORTFOLIO_ID in (1,2)
order by p.PORTFOLIO_ID, p.ENTRY_TS desc
limit 20;

select PORTFOLIO_ID, EPISODE_ID, START_TS, STATUS
from MIP.APP.PORTFOLIO_EPISODE
where PORTFOLIO_ID in (1,2) and STATUS = 'ACTIVE';

select PORTFOLIO_ID, STATUS, count(*) 
from MIP.AGENT_OUT.ORDER_PROPOSALS 
where PORTFOLIO_ID in (1,2)
  and PROPOSED_AT >= dateadd(day, -7, current_timestamp())
group by 1,2;

-- When was the last market bar ingested?
select 
    MARKET_TYPE,
    max(TS) as LATEST_BAR_TS,
    count(*) as BAR_COUNT
from MIP.MART.MARKET_BARS
where TS >= dateadd(day, -7, current_timestamp())
group by 1
order by 2 desc;

-- What's the effective_to_ts the pipeline is using?
select 
    RUN_ID,
    EVENT_TS,
    DETAILS:effective_to_ts::timestamp_ntz as EFFECTIVE_TO_TS,
    DETAILS:latest_market_bars_ts::timestamp_ntz as LATEST_MARKET_BARS_TS,
    DETAILS:has_new_bars::boolean as HAS_NEW_BARS,
    STATUS
from MIP.APP.MIP_AUDIT_LOG
where EVENT_TYPE = 'PIPELINE' and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
order by EVENT_TS desc
limit 5;
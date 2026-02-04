use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Find procedures with :P_PORTFOLIO_ID in exception blocks
-- Run this FIRST to find the culprit procedure
-- =============================================================================

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
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
from MIP.MART.V_PORTFOLIO_RISK_GATE
where PORTFOLIO_ID in (1,2)
order by AS_OF_TS desc
limit 20;

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

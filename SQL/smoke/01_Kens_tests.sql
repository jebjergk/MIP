use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


select * from mip.app.pattern_definition;

DROP TABLE MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE;
DROP TABLE MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT;

-- Count ETF candidates at each filter stage for the latest bar
with latest as (
    select max(TS) as ts from MIP.MART.MARKET_RETURNS 
    where MARKET_TYPE = 'ETF' and INTERVAL_MINUTES = 1440
)
select
    count(*) as total_etf_rows_at_latest,
    count_if(RETURN_SIMPLE >= 0) as positive_return,
    count_if(RETURN_SIMPLE >= 0 and VOLUME >= 1000) as positive_with_volume
from MIP.MART.MARKET_RETURNS r
join latest l on r.TS = l.ts
where r.MARKET_TYPE = 'ETF' and r.INTERVAL_MINUTES = 1440;

-- Show the last 5 daily returns per ETF symbol to see the momentum picture
select SYMBOL, TS, RETURN_SIMPLE, VOLUME,
       case when RETURN_SIMPLE > 0 then 'GREEN' else 'RED' end as DAY_COLOR
from MIP.MART.MARKET_RETURNS
where MARKET_TYPE = 'ETF' and INTERVAL_MINUTES = 1440
qualify row_number() over (partition by SYMBOL order by TS desc) <= 5
order by SYMBOL, TS desc;
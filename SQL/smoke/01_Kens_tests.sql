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
-- 1. Check: did SP actually get redeployed? (Look for position_days_expanded in latest sim log)
select EVENT_TS, EVENT_TYPE, DETAILS:position_days_expanded::number as pos_days_expanded,
       DETAILS:final_equity::number as final_equity,
       DETAILS:daily_rows::number as daily_rows,
       DETAILS:effective_from_ts::string as eff_from,
       DETAILS:portfolio_id::number as pid
from MIP.APP.mip_audit_LOG
where EVENT_TYPE = 'PORTFOLIO_SIM'
order by EVENT_TS desc
limit 4;

-- 2. Check: do existing positions have INTERVAL_MINUTES set?
select PORTFOLIO_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, ENTRY_INDEX, HOLD_UNTIL_INDEX
from MIP.APP.PORTFOLIO_POSITIONS
order by PORTFOLIO_ID, SYMBOL;

-- 3. Simulate the TEMP_POSITION_DAYS join manually
select
    vb.TS, vb.BAR_INDEX, p.SYMBOL, p.MARKET_TYPE, p.QUANTITY,
    vb.CLOSE as CLOSE_PRICE, p.INTERVAL_MINUTES as pos_interval, p.ENTRY_INDEX, p.HOLD_UNTIL_INDEX
from MIP.APP.PORTFOLIO_POSITIONS p
join MIP.MART.V_BAR_INDEX vb
  on vb.SYMBOL = p.SYMBOL
 and vb.MARKET_TYPE = p.MARKET_TYPE
 and vb.INTERVAL_MINUTES = 1440
 and vb.BAR_INDEX between p.ENTRY_INDEX and p.HOLD_UNTIL_INDEX
where p.PORTFOLIO_ID = 1
  and p.INTERVAL_MINUTES = 1440
  and vb.TS >= '2026-02-06'
order by p.SYMBOL, vb.TS
limit 20;

-- What BAR_INDEX does V_BAR_INDEX currently assign to bars near Feb 6?
select SYMBOL, MARKET_TYPE, TS, BAR_INDEX, CLOSE
from MIP.MART.V_BAR_INDEX
where SYMBOL in ('AUD/USD', 'JNJ', 'KO', 'PG')
  and INTERVAL_MINUTES = 1440
  and TS >= '2026-02-04'
order by SYMBOL, TS;

-- What's the MAX BAR_INDEX per symbol?
select SYMBOL, MARKET_TYPE, max(BAR_INDEX) as max_bar_index, max(TS) as max_ts, count(*) as total_bars
from MIP.MART.V_BAR_INDEX
where SYMBOL in ('AUD/USD', 'JNJ', 'KO', 'PG')
  and INTERVAL_MINUTES = 1440
group by SYMBOL, MARKET_TYPE;

delete from MIP.APP.PORTFOLIO_DAILY;
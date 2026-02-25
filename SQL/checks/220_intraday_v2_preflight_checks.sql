-- 220_intraday_v2_preflight_checks.sql
-- Purpose: Phase 0 pre-flight checks for intraday v2 build.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) 15m bar stability by day (recent sample)
select
    MARKET_TYPE,
    TS::date as BAR_DATE,
    count(*) as BAR_COUNT,
    count(distinct SYMBOL) as SYMBOL_COUNT
from MIP.MART.MARKET_BARS
where INTERVAL_MINUTES = 15
  and TS >= dateadd(day, -30, current_timestamp())
group by 1, 2
order by 2, 1;

-- 2) Bar key uniqueness contract check: (MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS)
select
    MARKET_TYPE,
    SYMBOL,
    INTERVAL_MINUTES,
    TS,
    count(*) as DUP_COUNT
from MIP.MART.MARKET_BARS
where INTERVAL_MINUTES = 15
group by 1, 2, 3, 4
having count(*) > 1
order by DUP_COUNT desc, TS desc;

-- 3) Active 15m universe by market_type
select
    MARKET_TYPE,
    count(distinct SYMBOL) as ENABLED_SYMBOLS_15M
from MIP.APP.INGEST_UNIVERSE
where INTERVAL_MINUTES = 15
  and IS_ENABLED = true
group by 1
order by 1;

-- 4) Existing intraday signal natural key uniqueness check
-- Current contract used by detector dedupe:
-- (PATTERN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, TS)
select
    PATTERN_ID,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    TS,
    count(*) as DUP_COUNT
from MIP.APP.RECOMMENDATION_LOG
where INTERVAL_MINUTES = 15
group by 1, 2, 3, 4, 5
having count(*) > 1
order by DUP_COUNT desc, TS desc;

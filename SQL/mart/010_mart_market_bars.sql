-- 010_mart_market_bars.sql
-- Purpose:
--   Analytic views on top of MIP.RAW_EXT.MARKET_BARS_RAW
--   - MARKET_BARS: cleaned base view
--   - MARKET_LATEST_PER_SYMBOL: latest bar per symbol/interval
--   - MARKET_RETURNS: simple & log returns per bar

use role MIP_ADMIN_ROLE;
use database MIP;

-------------------------------
-- 1. Base view: MARKET_BARS
-------------------------------
create or replace view MIP.MART.MARKET_BARS as
select
    TS,
    SYMBOL,
    SOURCE,
    MARKET_TYPE,       -- 'STOCK' or 'FX'
    INTERVAL_MINUTES,  -- e.g. 5 for intraday stocks, 1440 for FX daily
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME,
    INGESTED_AT
from MIP.RAW_EXT.MARKET_BARS_RAW;

-- Notes:
-- - RAW JSON is intentionally not exposed here; use RAW_EXT if you need it.
-- - This view is the main "fact table" for time-series analytics.


----------------------------------------------
-- 2. Latest bar per symbol/interval: MARKET_LATEST_PER_SYMBOL
----------------------------------------------
create or replace view MIP.MART.MARKET_LATEST_PER_SYMBOL as
select
    TS,
    SYMBOL,
    SOURCE,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME,
    INGESTED_AT
from (
    select
        TS,
        SYMBOL,
        SOURCE,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        INGESTED_AT,
        row_number() over (
            partition by SYMBOL, MARKET_TYPE, INTERVAL_MINUTES
            order by TS desc, INGESTED_AT desc
        ) as RN
    from MIP.MART.MARKET_BARS
)
where RN = 1;

-- Notes:
-- - For each (SYMBOL, MARKET_TYPE, INTERVAL_MINUTES) we keep only the most recent bar.
-- - Ideal for a "Market Overview" page in Streamlit.


----------------------------------------------
-- 3. Returns: MARKET_RETURNS
----------------------------------------------
create or replace view MIP.MART.MARKET_RETURNS as
with ordered as (
    select
        TS,
        SYMBOL,
        SOURCE,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        INGESTED_AT,
        lag(CLOSE) over (
            partition by SYMBOL, MARKET_TYPE, INTERVAL_MINUTES
            order by TS
        ) as PREV_CLOSE
    from MIP.MART.MARKET_BARS
)
select
    TS,
    SYMBOL,
    SOURCE,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME,
    INGESTED_AT,
    PREV_CLOSE,
    case 
        when PREV_CLOSE is not null and PREV_CLOSE <> 0 
        then (CLOSE - PREV_CLOSE) / PREV_CLOSE
        else null
    end as RETURN_SIMPLE,
    case 
        when PREV_CLOSE is not null and PREV_CLOSE > 0 and CLOSE > 0
        then ln(CLOSE / PREV_CLOSE)
        else null
    end as RETURN_LOG
from ordered;

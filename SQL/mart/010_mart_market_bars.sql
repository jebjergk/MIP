-- 010_mart_market_bars.sql
-- Purpose:
--   Analytic views on top of MIP.MART.MARKET_BARS
--   - MARKET_BARS: cleaned base table
--   - MARKET_LATEST_PER_SYMBOL: latest bar per symbol/interval
--   - MARKET_RETURNS: simple & log returns per bar

use role MIP_ADMIN_ROLE;
use database MIP;

-------------------------------
-- 1. Base table: MARKET_BARS
-------------------------------
create or replace table MIP.MART.MARKET_BARS (
    TS               TIMESTAMP_NTZ,
    SYMBOL           STRING,
    SOURCE           STRING,
    MARKET_TYPE      STRING,         -- 'STOCK' or 'FX'
    INTERVAL_MINUTES NUMBER,         -- e.g. 5 for intraday stocks, 1440 for FX daily
    OPEN             NUMBER,
    HIGH             NUMBER,
    LOW              NUMBER,
    CLOSE            NUMBER,
    VOLUME           NUMBER,
    INGESTED_AT      TIMESTAMP_NTZ
);

-- Notes:
-- - This table is the main "fact table" for time-series analytics.


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

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
create table if not exists MIP.MART.MARKET_BARS (
    TS               TIMESTAMP_NTZ,
    SYMBOL           STRING,
    SOURCE           STRING,
    MARKET_TYPE      STRING,         -- 'STOCK' or 'FX'
    INTERVAL_MINUTES NUMBER,         -- e.g. 5 for intraday stocks, 1440 for FX daily
    OPEN             NUMBER(18,8),
    HIGH             NUMBER(18,8),
    LOW              NUMBER(18,8),
    CLOSE            NUMBER(18,8),
    VOLUME           NUMBER,
    INGESTED_AT      TIMESTAMP_NTZ
);

alter table MIP.MART.MARKET_BARS
    add column if not exists TS TIMESTAMP_NTZ;
alter table MIP.MART.MARKET_BARS
    add column if not exists SYMBOL STRING;
alter table MIP.MART.MARKET_BARS
    add column if not exists SOURCE STRING;
alter table MIP.MART.MARKET_BARS
    add column if not exists MARKET_TYPE STRING;
alter table MIP.MART.MARKET_BARS
    add column if not exists INTERVAL_MINUTES NUMBER;
alter table MIP.MART.MARKET_BARS
    add column if not exists OPEN NUMBER(18,8);
alter table MIP.MART.MARKET_BARS
    add column if not exists HIGH NUMBER(18,8);
alter table MIP.MART.MARKET_BARS
    add column if not exists LOW NUMBER(18,8);
alter table MIP.MART.MARKET_BARS
    add column if not exists CLOSE NUMBER(18,8);
alter table MIP.MART.MARKET_BARS
    add column if not exists VOLUME NUMBER;
alter table MIP.MART.MARKET_BARS
    add column if not exists INGESTED_AT TIMESTAMP_NTZ;

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
with deduped as (
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
                partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS
                order by INGESTED_AT desc, SOURCE desc
            ) as RN
        from MIP.MART.MARKET_BARS
    )
    where RN = 1
),
ordered as (
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
    from deduped
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

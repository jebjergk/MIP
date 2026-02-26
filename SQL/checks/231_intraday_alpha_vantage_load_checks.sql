-- 231_intraday_alpha_vantage_load_checks.sql
-- Purpose: Validate Alpha Vantage month-by-month 15m backfill quality.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Latest load-run status overview.
select
    RUN_ID,
    STATUS,
    count(*) as WORK_ITEMS,
    sum(coalesce(ROWS_PARSED, 0)) as ROWS_PARSED,
    sum(coalesce(ROWS_INSERTED, 0)) as ROWS_INSERTED,
    sum(coalesce(ROWS_UPDATED, 0)) as ROWS_UPDATED,
    min(STARTED_AT) as STARTED_AT,
    max(COMPLETED_AT) as COMPLETED_AT
from MIP.APP.INTRADAY_ALPHA_VANTAGE_LOAD_LOG
where CREATED_AT >= dateadd(day, -2, current_timestamp())
group by 1,2
order by max(UPDATED_AT) desc, RUN_ID, STATUS;

-- 2) DONE/FAILED split by market type and month for latest 6 months.
select
    MARKET_TYPE,
    MONTH_YYYY_MM,
    STATUS,
    count(*) as WORK_ITEMS
from MIP.APP.INTRADAY_ALPHA_VANTAGE_LOAD_LOG
where INTERVAL_MINUTES = 15
  and MONTH_YYYY_MM >= to_char(dateadd(month, -5, date_trunc(month, current_date())), 'YYYY-MM')
group by 1,2,3
order by 2,1,3;

-- 3) Coverage: expected symbol-month worklist vs done in latest 6 months.
with months as (
    select to_char(dateadd(month, -seq4(), date_trunc(month, current_date())), 'YYYY-MM') as MONTH_YYYY_MM
    from table(generator(rowcount => 6))
),
universe as (
    select upper(SYMBOL) as SYMBOL, upper(MARKET_TYPE) as MARKET_TYPE, INTERVAL_MINUTES
    from MIP.APP.INGEST_UNIVERSE
    where coalesce(IS_ENABLED, true)
      and INTERVAL_MINUTES = 15
),
expected as (
    select u.SYMBOL, u.MARKET_TYPE, u.INTERVAL_MINUTES, m.MONTH_YYYY_MM
    from universe u
    cross join months m
),
done_keys as (
    select distinct SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, MONTH_YYYY_MM
    from MIP.APP.INTRADAY_ALPHA_VANTAGE_LOAD_LOG
    where STATUS = 'DONE'
      and INTERVAL_MINUTES = 15
      and MONTH_YYYY_MM in (select MONTH_YYYY_MM from months)
)
select
    count(*) as EXPECTED_WORK_ITEMS,
    sum(iff(d.SYMBOL is not null, 1, 0)) as DONE_WORK_ITEMS,
    sum(iff(d.SYMBOL is null, 1, 0)) as MISSING_WORK_ITEMS
from expected e
left join done_keys d
  on d.SYMBOL = e.SYMBOL
 and d.MARKET_TYPE = e.MARKET_TYPE
 and d.INTERVAL_MINUTES = e.INTERVAL_MINUTES
 and d.MONTH_YYYY_MM = e.MONTH_YYYY_MM;

-- 4) Duplicate guard on MARKET_BARS natural key.
select
    count(*) as DUP_KEY_ROWS
from (
    select MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS, count(*) as C
    from MIP.MART.MARKET_BARS
    where INTERVAL_MINUTES = 15
      and TS >= dateadd(month, -6, current_timestamp())
    group by 1,2,3,4
    having count(*) > 1
);

-- 5) Symbol-month bar counts for loaded range (sanity coverage).
select
    MARKET_TYPE,
    SYMBOL,
    to_char(date_trunc(month, TS), 'YYYY-MM') as MONTH_YYYY_MM,
    count(*) as BAR_ROWS,
    min(TS) as MIN_TS,
    max(TS) as MAX_TS
from MIP.MART.MARKET_BARS
where INTERVAL_MINUTES = 15
  and TS >= dateadd(month, -6, current_timestamp())
group by 1,2,3
order by 3,1,2;

-- 6) Gap diagnostics (>90 minutes between consecutive bars) per symbol-month.
with gaps as (
    select
        MARKET_TYPE,
        SYMBOL,
        to_char(date_trunc(month, TS), 'YYYY-MM') as MONTH_YYYY_MM,
        datediff(minute,
            lag(TS) over (partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES order by TS),
            TS
        ) as GAP_MINUTES
    from MIP.MART.MARKET_BARS
    where INTERVAL_MINUTES = 15
      and TS >= dateadd(month, -6, current_timestamp())
)
select
    MARKET_TYPE,
    SYMBOL,
    MONTH_YYYY_MM,
    count_if(GAP_MINUTES > 90) as LARGE_GAP_COUNT,
    max(GAP_MINUTES) as MAX_GAP_MINUTES
from gaps
group by 1,2,3
order by MONTH_YYYY_MM, MARKET_TYPE, SYMBOL;


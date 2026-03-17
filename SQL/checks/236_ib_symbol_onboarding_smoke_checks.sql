-- 236_ib_symbol_onboarding_smoke_checks.sql
-- Purpose: Smoke checks for latest IB symbol onboarding run.

use role MIP_ADMIN_ROLE;
use database MIP;

with latest_run as (
    select RUN_ID, SYMBOL_COHORT, MARKET_TYPE, START_DATE, END_DATE, STATUS, STARTED_AT, FINISHED_AT
    from MIP.APP.IB_SYMBOL_ONBOARDING_RUN_LOG
    qualify row_number() over (order by STARTED_AT desc) = 1
)
select * from latest_run;

-- 1) Latest run symbols exist and have per-symbol status.
with latest_run as (
    select RUN_ID
    from MIP.APP.IB_SYMBOL_ONBOARDING_RUN_LOG
    qualify row_number() over (order by STARTED_AT desc) = 1
)
select
    l.SYMBOL,
    l.MARKET_TYPE,
    l.STATUS,
    l.READY_FLAG,
    l.ACTIVATED_FLAG,
    l.REASON,
    l.FIRST_BAR_DATE,
    l.LAST_BAR_DATE,
    l.BAR_COUNT
from MIP.APP.IB_SYMBOL_ONBOARDING_SYMBOL_LOG l
join latest_run r
  on r.RUN_ID = l.RUN_ID
order by l.SYMBOL;

-- 2) Universe rows are present for the run cohort.
with latest_run as (
    select RUN_ID, SYMBOL_COHORT, MARKET_TYPE
    from MIP.APP.IB_SYMBOL_ONBOARDING_RUN_LOG
    qualify row_number() over (order by STARTED_AT desc) = 1
)
select
    u.SYMBOL,
    u.MARKET_TYPE,
    u.INTERVAL_MINUTES,
    u.IS_ENABLED,
    u.SYMBOL_COHORT
from MIP.APP.INGEST_UNIVERSE u
join latest_run r
  on upper(r.SYMBOL_COHORT) = upper(coalesce(u.SYMBOL_COHORT, ''))
 and upper(r.MARKET_TYPE) = upper(u.MARKET_TYPE)
where u.INTERVAL_MINUTES = 1440
order by u.SYMBOL;

-- 3) Trade activation state is persisted for symbols in the latest run.
with latest_run as (
    select RUN_ID
    from MIP.APP.IB_SYMBOL_ONBOARDING_RUN_LOG
    qualify row_number() over (order by STARTED_AT desc) = 1
),
latest_symbols as (
    select SYMBOL, MARKET_TYPE
    from MIP.APP.IB_SYMBOL_ONBOARDING_SYMBOL_LOG s
    join latest_run r on r.RUN_ID = s.RUN_ID
)
select
    a.SYMBOL,
    a.MARKET_TYPE,
    a.IS_ACTIVE_FOR_TRADE,
    a.ACTIVATED_AT,
    a.LAST_RUN_ID,
    a.REASON
from MIP.APP.IB_SYMBOL_TRADE_ACTIVATION a
join latest_symbols s
  on upper(s.SYMBOL) = upper(a.SYMBOL)
 and upper(s.MARKET_TYPE) = upper(a.MARKET_TYPE)
order by a.SYMBOL;

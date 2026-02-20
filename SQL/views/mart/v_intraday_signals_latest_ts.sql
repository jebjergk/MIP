-- v_intraday_signals_latest_ts.sql
-- Purpose: Intraday equivalent of V_SIGNALS_LATEST_TS.
-- All intraday signals at the latest intraday bar TS.
-- Interval is driven by APP_CONFIG('INTRADAY_INTERVAL_MINUTES').

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_INTRADAY_SIGNALS_LATEST_TS as
with cfg as (
    select coalesce(
        try_to_number((select CONFIG_VALUE from MIP.APP.APP_CONFIG
                       where CONFIG_KEY = 'INTRADAY_INTERVAL_MINUTES')),
        60
    ) as INTERVAL_MINUTES
),
latest_ts as (
    select max(b.TS) as TS
    from MIP.MART.MARKET_BARS b
    cross join cfg c
    where b.INTERVAL_MINUTES = c.INTERVAL_MINUTES
)
select
    r.RECOMMENDATION_ID,
    r.PATTERN_ID,
    r.SYMBOL,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    r.TS as SIGNAL_TS,
    r.GENERATED_AT,
    r.SCORE,
    r.DETAILS,
    coalesce(r.DETAILS:run_id::string,
             to_varchar(r.GENERATED_AT, 'YYYYMMDD"T"HH24MISS')) as RUN_ID
from MIP.APP.RECOMMENDATION_LOG r
cross join latest_ts lt
cross join cfg c
where r.INTERVAL_MINUTES = c.INTERVAL_MINUTES
  and r.TS = lt.TS;

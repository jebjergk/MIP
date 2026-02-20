-- v_intraday_trusted_signals.sql
-- Purpose: Intraday equivalent of V_TRUSTED_SIGNALS_LATEST_TS.
-- Intraday signals at the latest intraday bar TS that have passed trust gate.
-- Reuses V_TRUSTED_PATTERN_HORIZONS (already interval-aware).

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_INTRADAY_TRUSTED_SIGNALS as
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
    t.HORIZON_BARS,
    r.TS as SIGNAL_TS,
    r.SCORE,
    r.DETAILS,
    r.GENERATED_AT,
    coalesce(r.DETAILS:run_id::string,
             to_varchar(r.GENERATED_AT, 'YYYYMMDD"T"HH24MISS')) as RUN_ID,
    lt.TS as LAST_SIGNAL_TS,
    t.N_SIGNALS,
    t.HIT_RATE_SUCCESS,
    t.AVG_RETURN_SUCCESS,
    t.SHARPE_LIKE_SUCCESS,
    t.CONFIDENCE,
    'GATE_PASS' as TRUST_REASON
from MIP.APP.RECOMMENDATION_LOG r
cross join latest_ts lt
cross join cfg c
join MIP.MART.V_TRUSTED_PATTERN_HORIZONS t
  on t.PATTERN_ID = r.PATTERN_ID
 and t.MARKET_TYPE = r.MARKET_TYPE
 and t.INTERVAL_MINUTES = r.INTERVAL_MINUTES
where r.INTERVAL_MINUTES = c.INTERVAL_MINUTES
  and r.TS = lt.TS;

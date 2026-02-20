-- v_intraday_pattern_stability.sql
-- Purpose: Rolling-window hit rate and avg net return per intraday pattern.
-- Detects performance decay by comparing recent vs full-history metrics.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_INTRADAY_PATTERN_STABILITY as
with full_history as (
    select
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS,
        count_if(EVAL_STATUS = 'SUCCESS') as N_FULL,
        count_if(NET_HIT_FLAG and EVAL_STATUS = 'SUCCESS') as HITS_FULL,
        avg(case when EVAL_STATUS = 'SUCCESS' then NET_RETURN end) as AVG_NET_RETURN_FULL
    from MIP.MART.V_INTRADAY_OUTCOMES_FEE_ADJUSTED
    group by PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS
),
recent_window as (
    select
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS,
        count_if(EVAL_STATUS = 'SUCCESS') as N_RECENT,
        count_if(NET_HIT_FLAG and EVAL_STATUS = 'SUCCESS') as HITS_RECENT,
        avg(case when EVAL_STATUS = 'SUCCESS' then NET_RETURN end) as AVG_NET_RETURN_RECENT
    from MIP.MART.V_INTRADAY_OUTCOMES_FEE_ADJUSTED
    where SIGNAL_TS >= dateadd(day, -30, current_timestamp())
    group by PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS
),
patterns as (
    select PATTERN_ID, NAME, PATTERN_TYPE
    from MIP.APP.PATTERN_DEFINITION
)
select
    f.PATTERN_ID,
    p.NAME as PATTERN_NAME,
    p.PATTERN_TYPE,
    f.MARKET_TYPE,
    f.INTERVAL_MINUTES,
    f.HORIZON_BARS,
    f.N_FULL,
    f.HITS_FULL,
    case when f.N_FULL > 0 then f.HITS_FULL::float / f.N_FULL else null end as HIT_RATE_FULL,
    f.AVG_NET_RETURN_FULL,
    coalesce(r.N_RECENT, 0) as N_RECENT,
    coalesce(r.HITS_RECENT, 0) as HITS_RECENT,
    case when coalesce(r.N_RECENT, 0) > 0
         then r.HITS_RECENT::float / r.N_RECENT
         else null end as HIT_RATE_RECENT,
    r.AVG_NET_RETURN_RECENT,
    case when f.N_FULL > 0 and coalesce(r.N_RECENT, 0) >= 5
         then (r.HITS_RECENT::float / r.N_RECENT) - (f.HITS_FULL::float / f.N_FULL)
         else null end as HIT_RATE_DRIFT,
    case when coalesce(r.N_RECENT, 0) >= 5
         then coalesce(r.AVG_NET_RETURN_RECENT, 0) - coalesce(f.AVG_NET_RETURN_FULL, 0)
         else null end as RETURN_DRIFT,
    case
        when coalesce(r.N_RECENT, 0) < 5 then 'INSUFFICIENT_RECENT_DATA'
        when (r.HITS_RECENT::float / r.N_RECENT) - (f.HITS_FULL::float / f.N_FULL) < -0.10
            then 'DEGRADING'
        when (r.HITS_RECENT::float / r.N_RECENT) - (f.HITS_FULL::float / f.N_FULL) > 0.10
            then 'IMPROVING'
        else 'STABLE'
    end as STABILITY_STATUS
from full_history f
left join recent_window r
  on r.PATTERN_ID = f.PATTERN_ID
 and r.MARKET_TYPE = f.MARKET_TYPE
 and r.INTERVAL_MINUTES = f.INTERVAL_MINUTES
 and r.HORIZON_BARS = f.HORIZON_BARS
left join patterns p on p.PATTERN_ID = f.PATTERN_ID;

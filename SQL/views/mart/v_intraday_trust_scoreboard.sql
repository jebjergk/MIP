-- v_intraday_trust_scoreboard.sql
-- Purpose: Pattern-level dashboard for intraday learning loop.
-- Fee-adjusted hit rate, avg net return, signal maturity, confidence tier.
-- This is the primary view for assessing whether intraday patterns have alpha.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_INTRADAY_TRUST_SCOREBOARD as
with outcomes as (
    select
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS,
        count(*) as N_OUTCOMES,
        count_if(EVAL_STATUS = 'SUCCESS') as N_EVALUATED,
        count_if(NET_HIT_FLAG and EVAL_STATUS = 'SUCCESS') as N_NET_HITS,
        avg(case when EVAL_STATUS = 'SUCCESS' then GROSS_RETURN end) as AVG_GROSS_RETURN,
        avg(case when EVAL_STATUS = 'SUCCESS' then NET_RETURN end) as AVG_NET_RETURN,
        median(case when EVAL_STATUS = 'SUCCESS' then NET_RETURN end) as MEDIAN_NET_RETURN,
        stddev(case when EVAL_STATUS = 'SUCCESS' then NET_RETURN end) as STDDEV_NET_RETURN,
        avg(case when EVAL_STATUS = 'SUCCESS' then ROUND_TRIP_COST end) as AVG_ROUND_TRIP_COST,
        avg(case when EVAL_STATUS = 'SUCCESS' then MAX_FAVORABLE_EXCURSION end) as AVG_MAX_FAVORABLE,
        avg(case when EVAL_STATUS = 'SUCCESS' then MAX_ADVERSE_EXCURSION end) as AVG_MAX_ADVERSE,
        min(SIGNAL_TS) as FIRST_SIGNAL_TS,
        max(SIGNAL_TS) as LAST_SIGNAL_TS
    from MIP.MART.V_INTRADAY_OUTCOMES_FEE_ADJUSTED
    group by PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS
),
gate as (
    select
        MIN_SIGNALS,
        coalesce(MIN_SIGNALS_BOOTSTRAP, 5) as MIN_SIGNALS_BOOTSTRAP,
        MIN_HIT_RATE,
        MIN_AVG_RETURN
    from MIP.APP.TRAINING_GATE_PARAMS
    where IS_ACTIVE
    qualify row_number() over (order by PARAM_SET) = 1
),
patterns as (
    select PATTERN_ID, NAME, PATTERN_TYPE, DESCRIPTION
    from MIP.APP.PATTERN_DEFINITION
)
select
    o.PATTERN_ID,
    p.NAME as PATTERN_NAME,
    p.PATTERN_TYPE,
    o.MARKET_TYPE,
    o.INTERVAL_MINUTES,
    o.HORIZON_BARS,
    o.N_OUTCOMES,
    o.N_EVALUATED,
    o.N_NET_HITS,
    case when o.N_EVALUATED > 0
         then o.N_NET_HITS::float / o.N_EVALUATED
         else null end as NET_HIT_RATE,
    o.AVG_GROSS_RETURN,
    o.AVG_NET_RETURN,
    o.MEDIAN_NET_RETURN,
    o.STDDEV_NET_RETURN,
    case when o.STDDEV_NET_RETURN is not null and o.STDDEV_NET_RETURN > 0
         then o.AVG_NET_RETURN / o.STDDEV_NET_RETURN
         else null end as NET_SHARPE_LIKE,
    o.AVG_ROUND_TRIP_COST,
    o.AVG_MAX_FAVORABLE,
    o.AVG_MAX_ADVERSE,
    o.FIRST_SIGNAL_TS,
    o.LAST_SIGNAL_TS,
    case
        when o.N_EVALUATED >= g.MIN_SIGNALS
             and coalesce(o.N_NET_HITS::float / nullif(o.N_EVALUATED, 0), 0) >= g.MIN_HIT_RATE
             and coalesce(o.AVG_NET_RETURN, -999) >= g.MIN_AVG_RETURN
            then 'TRUSTED'
        when o.N_EVALUATED >= g.MIN_SIGNALS_BOOTSTRAP
             and coalesce(o.N_NET_HITS::float / nullif(o.N_EVALUATED, 0), 0) >= g.MIN_HIT_RATE
             and coalesce(o.AVG_NET_RETURN, -999) >= g.MIN_AVG_RETURN
            then 'WATCH'
        when o.N_EVALUATED < g.MIN_SIGNALS_BOOTSTRAP
            then 'IMMATURE'
        else 'UNTRUSTED'
    end as TRUST_STATUS,
    case
        when o.N_EVALUATED >= g.MIN_SIGNALS then 'HIGH'
        when o.N_EVALUATED >= g.MIN_SIGNALS_BOOTSTRAP then 'LOW'
        else 'NONE'
    end as CONFIDENCE_LEVEL
from outcomes o
cross join gate g
left join patterns p on p.PATTERN_ID = o.PATTERN_ID;

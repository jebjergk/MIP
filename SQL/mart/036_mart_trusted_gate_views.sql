-- 036_mart_trusted_gate_views.sql
-- Purpose: Trusted signal gate — allow-list from V_TRAINING_LEADERBOARD + TRAINING_GATE_PARAMS; trusted-signals-at-latest-TS; top-N.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ------------------------------------------------------------------------------
-- D1: V_TRUSTED_PATTERN_HORIZONS
-- One row per (pattern_id, market_type, interval, horizon) that passes training thresholds.
-- Full gate: N_SIGNALS >= MIN_SIGNALS -> confidence HIGH. Bootstrap: N_SIGNALS >= MIN_SIGNALS_BOOTSTRAP -> confidence LOW.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRUSTED_PATTERN_HORIZONS as
with p as (
    select
        MIN_SIGNALS,
        coalesce(MIN_SIGNALS_BOOTSTRAP, 5) as MIN_SIGNALS_BOOTSTRAP,
        MIN_HIT_RATE,
        MIN_AVG_RETURN
    from MIP.APP.TRAINING_GATE_PARAMS
    where IS_ACTIVE
    qualify row_number() over (order by PARAM_SET) = 1
)
select
    l.PATTERN_ID,
    l.MARKET_TYPE,
    l.INTERVAL_MINUTES,
    l.HORIZON_BARS,
    l.N_SIGNALS,
    l.N_SUCCESS,
    l.HIT_RATE_SUCCESS,
    l.AVG_RETURN_SUCCESS,
    l.SHARPE_LIKE_SUCCESS,
    case
        when l.N_SIGNALS >= p.MIN_SIGNALS then 'HIGH'
        when l.N_SIGNALS >= p.MIN_SIGNALS_BOOTSTRAP then 'LOW'
        else 'LOW'
    end as CONFIDENCE
from MIP.MART.V_TRAINING_LEADERBOARD l
cross join p
where (l.N_SIGNALS >= p.MIN_SIGNALS or l.N_SIGNALS >= p.MIN_SIGNALS_BOOTSTRAP)
  and coalesce(l.HIT_RATE_SUCCESS, 0) >= p.MIN_HIT_RATE
  and coalesce(l.AVG_RETURN_SUCCESS, -999) >= p.MIN_AVG_RETURN;

-- ------------------------------------------------------------------------------
-- V_SIGNALS_LATEST_TS (helper for audit)
-- All signals at latest bar TS, no trust filter. Used for candidate_count_raw.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_SIGNALS_LATEST_TS as
with latest_ts as (
    select max(TS) as TS
    from MIP.MART.MARKET_BARS
    where INTERVAL_MINUTES = 1440
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
    coalesce(r.DETAILS:run_id::string, to_varchar(r.GENERATED_AT, 'YYYYMMDD"T"HH24MISS')) as RUN_ID
from MIP.APP.RECOMMENDATION_LOG r
cross join latest_ts lt
where r.INTERVAL_MINUTES = 1440
  and r.TS = lt.TS;

-- ------------------------------------------------------------------------------
-- D2: V_TRUSTED_SIGNALS_LATEST_TS
-- Today's trusted candidates: V_SIGNAL_OUTCOMES_BASE at latest signal_ts, restricted to
-- (pattern_id, market_type, interval_minutes, horizon_bars) in V_TRUSTED_PATTERN_HORIZONS.
-- One row per (recommendation_id, horizon_bars); explainability fields from trusted horizon.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS as
with latest as (
    select max(SIGNAL_TS) as latest_ts
    from MIP.MART.V_SIGNAL_OUTCOMES_BASE
),
trusted_ph as (
    select
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS,
        N_SIGNALS,
        HIT_RATE_SUCCESS,
        AVG_RETURN_SUCCESS,
        SHARPE_LIKE_SUCCESS,
        CONFIDENCE
    from MIP.MART.V_TRUSTED_PATTERN_HORIZONS
),
candidates as (
    select
        o.RECOMMENDATION_ID,
        o.PATTERN_ID,
        o.SYMBOL,
        o.MARKET_TYPE,
        o.INTERVAL_MINUTES,
        o.HORIZON_BARS,
        o.SIGNAL_TS,
        o.SCORE,
        o.DETAILS,
        coalesce(o.DETAILS:run_id::string, to_varchar(o.GENERATED_AT, 'YYYYMMDD"T"HH24MISS')) as RUN_ID,
        l.latest_ts as LAST_SIGNAL_TS,
        t.N_SIGNALS,
        t.HIT_RATE_SUCCESS,
        t.AVG_RETURN_SUCCESS,
        t.SHARPE_LIKE_SUCCESS,
        t.CONFIDENCE,
        'GATE_PASS' as TRUST_REASON
    from MIP.MART.V_SIGNAL_OUTCOMES_BASE o
    join trusted_ph t
      on t.PATTERN_ID = o.PATTERN_ID
     and t.MARKET_TYPE = o.MARKET_TYPE
     and t.INTERVAL_MINUTES = o.INTERVAL_MINUTES
     and t.HORIZON_BARS = o.HORIZON_BARS
    join latest l
      on o.SIGNAL_TS = l.latest_ts
)
select
    RECOMMENDATION_ID,
    PATTERN_ID,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    SIGNAL_TS,
    SCORE,
    DETAILS,
    RUN_ID,
    LAST_SIGNAL_TS,
    N_SIGNALS,
    HIT_RATE_SUCCESS,
    AVG_RETURN_SUCCESS,
    SHARPE_LIKE_SUCCESS,
    CONFIDENCE,
    TRUST_REASON
from candidates;

-- ------------------------------------------------------------------------------
-- Bonus: V_TRUSTED_TOP10
-- Top 10 trusted (pattern, horizon) combos by SHARPE_LIKE_SUCCESS for brief anchor.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRUSTED_TOP10 as
select *
from MIP.MART.V_TRUSTED_PATTERN_HORIZONS
qualify row_number() over (order by SHARPE_LIKE_SUCCESS desc nulls last) <= 10;

-- ------------------------------------------------------------------------------
-- Smoke / acceptance queries (run after deploy)
-- ------------------------------------------------------------------------------
-- select * from MIP.MART.V_TRAINING_LEADERBOARD limit 20;
-- select * from MIP.MART.V_TRUSTED_PATTERN_HORIZONS order by SHARPE_LIKE_SUCCESS desc nulls last limit 50;
-- select count(*) as trusted_count from MIP.MART.V_TRUSTED_PATTERN_HORIZONS;
-- select * from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS limit 50;

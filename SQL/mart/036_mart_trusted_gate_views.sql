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
with active_version as (
    select TRAINING_VERSION
    from MIP.APP.V_TRAINING_VERSION_CURRENT
    where POLICY_NAME = 'DAILY_POLICY'
),
p as (
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
    av.TRAINING_VERSION,
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
cross join active_version av
cross join p
where (l.N_SIGNALS >= p.MIN_SIGNALS or l.N_SIGNALS >= p.MIN_SIGNALS_BOOTSTRAP)
  and coalesce(l.HIT_RATE_SUCCESS, 0) >= p.MIN_HIT_RATE
  and coalesce(l.AVG_RETURN_SUCCESS, -999) >= p.MIN_AVG_RETURN;

-- ------------------------------------------------------------------------------
-- V_SIGNALS_LATEST_TS (helper for audit)
-- All signals at latest recommendation TS, no trust filter. Used for candidate_count_raw.
-- NOTE: Anchor on RECOMMENDATION_LOG (not MARKET_BARS) to avoid mixed-market
-- day-boundary drift (e.g., FX rolling into next date before STOCK/ETF).
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_SIGNALS_LATEST_TS as
with latest_ts as (
    select max(TS) as TS
    from MIP.APP.RECOMMENDATION_LOG
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
-- Today's trusted candidates: RECOMMENDATION_LOG at latest recommendation TS, restricted to
-- (pattern_id, market_type, interval_minutes, horizon_bars) in V_TRUSTED_PATTERN_HORIZONS.
-- One row per (recommendation_id, horizon_bars); explainability fields from trusted horizon.
--
-- IMPORTANT: Uses RECOMMENDATION_LOG directly, NOT V_SIGNAL_OUTCOMES_BASE.
-- New signals should be eligible even before outcomes are evaluated.
-- Trust is based on the PATTERN's historical performance, not the individual signal's outcome.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS as
with latest_ts as (
    -- Latest recommendation TS from RECOMMENDATION_LOG (same as V_SIGNALS_LATEST_TS)
    select max(TS) as TS
    from MIP.APP.RECOMMENDATION_LOG
    where INTERVAL_MINUTES = 1440
),
trusted_ph as (
    select
        TRAINING_VERSION,
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
policy_active as (
    select
        TRAINING_VERSION,
        SYMBOL,
        MARKET_TYPE,
        PATTERN_ID,
        HORIZON_BARS,
        PATTERN_TARGET,
        SYMBOL_MULTIPLIER,
        EFFECTIVE_TARGET,
        TARGET_SOURCE,
        EFFECTIVE_HORIZON_BARS,
        HORIZON_SOURCE,
        N_OUTCOMES as POLICY_N_OUTCOMES,
        CI_WIDTH as POLICY_CI_WIDTH,
        ELIGIBLE_FLAG as POLICY_ELIGIBLE_FLAG,
        FALLBACK_REASON as POLICY_FALLBACK_REASON
    from MIP.MART.V_DAILY_POLICY_EFFECTIVE_ACTIVE
),
candidates as (
    -- Join today's signals to trusted pattern/horizon combos
    -- A signal is trusted if its pattern is trusted for at least one horizon
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
        coalesce(r.DETAILS:run_id::string, to_varchar(r.GENERATED_AT, 'YYYYMMDD"T"HH24MISS')) as RUN_ID,
        t.TRAINING_VERSION,
        lt.TS as LAST_SIGNAL_TS,
        t.N_SIGNALS,
        t.HIT_RATE_SUCCESS,
        t.AVG_RETURN_SUCCESS,
        t.SHARPE_LIKE_SUCCESS,
        t.CONFIDENCE,
        pa.PATTERN_TARGET,
        pa.SYMBOL_MULTIPLIER,
        coalesce(pa.EFFECTIVE_TARGET, t.AVG_RETURN_SUCCESS) as EFFECTIVE_TARGET,
        coalesce(pa.TARGET_SOURCE, 'PATTERN_ONLY') as TARGET_SOURCE,
        pa.EFFECTIVE_HORIZON_BARS,
        pa.HORIZON_SOURCE,
        pa.POLICY_N_OUTCOMES,
        pa.POLICY_CI_WIDTH,
        pa.POLICY_ELIGIBLE_FLAG,
        pa.POLICY_FALLBACK_REASON,
        'GATE_PASS' as TRUST_REASON
    from MIP.APP.RECOMMENDATION_LOG r
    cross join latest_ts lt
    join trusted_ph t
      on t.PATTERN_ID = r.PATTERN_ID
     and t.MARKET_TYPE = r.MARKET_TYPE
     and t.INTERVAL_MINUTES = r.INTERVAL_MINUTES
    left join policy_active pa
      on pa.TRAINING_VERSION = t.TRAINING_VERSION
     and pa.SYMBOL = r.SYMBOL
     and pa.MARKET_TYPE = r.MARKET_TYPE
     and pa.PATTERN_ID = r.PATTERN_ID
     and pa.HORIZON_BARS = t.HORIZON_BARS
    where r.INTERVAL_MINUTES = 1440
      and r.TS = lt.TS
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
    TRAINING_VERSION,
    LAST_SIGNAL_TS,
    N_SIGNALS,
    HIT_RATE_SUCCESS,
    AVG_RETURN_SUCCESS,
    PATTERN_TARGET,
    SYMBOL_MULTIPLIER,
    EFFECTIVE_TARGET,
    TARGET_SOURCE,
    EFFECTIVE_HORIZON_BARS,
    HORIZON_SOURCE,
    POLICY_N_OUTCOMES,
    POLICY_CI_WIDTH,
    POLICY_ELIGIBLE_FLAG,
    POLICY_FALLBACK_REASON,
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

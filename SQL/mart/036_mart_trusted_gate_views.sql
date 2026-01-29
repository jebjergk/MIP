-- 036_mart_trusted_gate_views.sql
-- Purpose: Trusted signal gate v1 — allow-list from training KPIs, trusted-signals-at-latest-TS, top-N.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ------------------------------------------------------------------------------
-- D1: V_TRUSTED_PATTERN_HORIZONS
-- Allow-list for what we are willing to trade. Built from V_TRAINING_KPIS.
-- v1 thresholds: N_SIGNALS >= 30, HIT_RATE >= 0.52, SHARPE_LIKE > 0, AVG_RETURN > 0 (optional).
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRUSTED_PATTERN_HORIZONS as
select
    k.PATTERN_ID,
    k.MARKET_TYPE,
    k.INTERVAL_MINUTES,
    k.HORIZON_BARS,
    k.N_SIGNALS,
    k.HIT_RATE_SUCCESS as HIT_RATE,
    k.AVG_RETURN_SUCCESS as AVG_RETURN,
    k.SHARPE_LIKE_SUCCESS as SHARPE_LIKE,
    (
        coalesce(k.N_SIGNALS, 0) >= 30
        and coalesce(k.HIT_RATE_SUCCESS, 0) >= 0.52
        and coalesce(k.SHARPE_LIKE_SUCCESS, 0) > 0
        and coalesce(k.AVG_RETURN_SUCCESS, 0) > 0
    ) as IS_TRUSTED,
    case
        when coalesce(k.N_SIGNALS, 0) < 30
            then 'N_SIGNALS<' || 30
        when coalesce(k.HIT_RATE_SUCCESS, 0) < 0.52
            then 'HIT_RATE<0.52'
        when coalesce(k.SHARPE_LIKE_SUCCESS, 0) <= 0
            then 'SHARPE_LIKE<=0'
        when coalesce(k.AVG_RETURN_SUCCESS, 0) <= 0
            then 'AVG_RETURN<=0'
        else 'OK'
    end as TRUST_REASON
from MIP.MART.V_TRAINING_KPIS k;

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
-- Signals at latest bar TS (INTERVAL_MINUTES=1440), joined to trusted pattern/horizons only.
-- Includes RUN_ID (from DETAILS:run_id or GENERATED_AT), IS_TRUSTED, TRUST_REASON, SCORE.
-- One row per RECOMMENDATION_ID — pick best horizon per rec via qualify.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS as
with joined as (
    select
        r.RECOMMENDATION_ID,
        r.PATTERN_ID,
        r.SYMBOL,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        r.SIGNAL_TS,
        r.GENERATED_AT,
        r.SCORE,
        r.DETAILS,
        r.RUN_ID,
        t.HORIZON_BARS,
        t.N_SIGNALS,
        t.HIT_RATE,
        t.AVG_RETURN,
        t.SHARPE_LIKE,
        t.IS_TRUSTED,
        t.TRUST_REASON
    from MIP.MART.V_SIGNALS_LATEST_TS r
    join MIP.MART.V_TRUSTED_PATTERN_HORIZONS t
      on t.PATTERN_ID = r.PATTERN_ID
     and t.MARKET_TYPE = r.MARKET_TYPE
     and t.INTERVAL_MINUTES = r.INTERVAL_MINUTES
     and t.IS_TRUSTED = true
)
select
    RECOMMENDATION_ID,
    PATTERN_ID,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    SIGNAL_TS,
    GENERATED_AT,
    SCORE,
    DETAILS,
    RUN_ID,
    HORIZON_BARS,
    N_SIGNALS,
    HIT_RATE,
    AVG_RETURN,
    SHARPE_LIKE,
    IS_TRUSTED,
    TRUST_REASON,
    null::number as LEADERBOARD_RANK
from joined
qualify row_number() over (
    partition by RECOMMENDATION_ID
    order by SHARPE_LIKE desc nulls last, HIT_RATE desc nulls last, AVG_RETURN desc nulls last
) = 1;

-- ------------------------------------------------------------------------------
-- Bonus: V_TRUSTED_TOP10
-- Top 10 trusted (pattern, horizon) combos by SHARPE_LIKE for brief anchor.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRUSTED_TOP10 as
select *
from MIP.MART.V_TRUSTED_PATTERN_HORIZONS
where IS_TRUSTED
qualify row_number() over (order by SHARPE_LIKE desc nulls last) <= 10;

-- ------------------------------------------------------------------------------
-- Smoke / acceptance queries (run after deploy)
-- ------------------------------------------------------------------------------
-- select * from MIP.MART.V_TRAINING_LEADERBOARD limit 20;
-- select * from MIP.MART.V_TRUSTED_PATTERN_HORIZONS where IS_TRUSTED order by SHARPE_LIKE desc limit 50;
-- select count(*) as trusted_count from MIP.MART.V_TRUSTED_PATTERN_HORIZONS where IS_TRUSTED;
-- select * from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS limit 50;

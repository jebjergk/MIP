-- 035_mart_training_views.sql
-- Purpose: Base join of signals + outcomes + pattern metadata, training KPIs, and leaderboard rankings.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ------------------------------------------------------------------------------
-- V_SIGNAL_OUTCOMES_BASE
-- One row per (recommendation_id, horizon_bars). Clean join of LOG + OUTCOMES + PATTERN_DEFINITION.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_SIGNAL_OUTCOMES_BASE as
select
    r.RECOMMENDATION_ID,
    o.HORIZON_BARS,
    r.PATTERN_ID,
    p.NAME as PATTERN_NAME,
    r.MARKET_TYPE,
    r.SYMBOL,
    r.INTERVAL_MINUTES,
    r.TS as TS,
    o.REALIZED_RETURN as RETURN_REALIZED,
    o.HIT_FLAG as HIT_FLAG
from MIP.APP.RECOMMENDATION_LOG r
join MIP.APP.RECOMMENDATION_OUTCOMES o
  on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
join MIP.APP.PATTERN_DEFINITION p
  on p.PATTERN_ID = r.PATTERN_ID;

-- ------------------------------------------------------------------------------
-- V_TRAINING_KPIS
-- Aggregated metrics by (pattern_id, market_type, interval_minutes, horizon_bars).
-- Uses EVAL_STATUS = 'SUCCESS' and non-null REALIZED_RETURN only.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRAINING_KPIS as
select
    r.PATTERN_ID,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    o.HORIZON_BARS,
    count(*) as N_SIGNALS,
    avg(case when o.HIT_FLAG then 1 else 0 end) as HIT_RATE,
    avg(o.REALIZED_RETURN) as AVG_RETURN,
    median(o.REALIZED_RETURN) as MEDIAN_RETURN,
    stddev_samp(o.REALIZED_RETURN) as STDDEV_RETURN,
    min(o.REALIZED_RETURN) as MIN_RETURN,
    max(o.REALIZED_RETURN) as MAX_RETURN,
    avg(o.REALIZED_RETURN) / nullif(stddev_samp(o.REALIZED_RETURN), 0) as SHARPE_LIKE,
    max(r.TS) as LAST_SIGNAL_TS
from MIP.APP.RECOMMENDATION_OUTCOMES o
join MIP.APP.RECOMMENDATION_LOG r
  on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
where o.EVAL_STATUS = 'SUCCESS'
  and o.REALIZED_RETURN is not null
group by
    r.PATTERN_ID,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    o.HORIZON_BARS;

-- ------------------------------------------------------------------------------
-- V_TRAINING_LEADERBOARD
-- Same KPIs as V_TRAINING_KPIS but ranked: Top 10 by HIT_RATE, SHARPE_LIKE, AVG_RETURN.
-- rank_category: 'HIT_RATE' | 'SHARPE_LIKE' | 'AVG_RETURN'
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRAINING_LEADERBOARD as
with kpis as (
    select * from MIP.MART.V_TRAINING_KPIS
),
ranked as (
    select
        *,
        'HIT_RATE' as RANK_CATEGORY,
        row_number() over (order by HIT_RATE desc nulls last) as RN
    from kpis
    union all
    select
        *,
        'SHARPE_LIKE',
        row_number() over (order by SHARPE_LIKE desc nulls last)
    from kpis
    union all
    select
        *,
        'AVG_RETURN',
        row_number() over (order by AVG_RETURN desc nulls last)
    from kpis
)
select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    N_SIGNALS,
    HIT_RATE,
    AVG_RETURN,
    MEDIAN_RETURN,
    STDDEV_RETURN,
    MIN_RETURN,
    MAX_RETURN,
    SHARPE_LIKE,
    LAST_SIGNAL_TS,
    RANK_CATEGORY,
    RN as RANK_NUMBER
from ranked
where RN <= 10;

-- ------------------------------------------------------------------------------
-- Acceptance tests (SQL checks) — run after deploy; use in PR description.
-- ------------------------------------------------------------------------------
-- select * from MIP.MART.V_TRAINING_KPIS limit 20;
-- select * from MIP.MART.V_TRAINING_LEADERBOARD limit 50;
-- should be > 0 if outcomes exist
-- select count(*) from MIP.MART.V_SIGNAL_OUTCOMES_BASE;

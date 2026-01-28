-- 035_mart_training_views.sql
-- Purpose: Base join of signals + outcomes, training KPIs, and leaderboard.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ------------------------------------------------------------------------------
-- V_SIGNAL_OUTCOMES_BASE
-- One row per (recommendation_id, horizon_bars). LOG join OUTCOMES on RECOMMENDATION_ID only.
-- All LOG columns, all OUTCOMES columns, plus derived: hit_int, is_success, hold_minutes.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_SIGNAL_OUTCOMES_BASE as
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
    o.HORIZON_BARS,
    o.ENTRY_TS,
    o.EXIT_TS,
    o.ENTRY_PRICE,
    o.EXIT_PRICE,
    o.REALIZED_RETURN,
    o.DIRECTION,
    o.HIT_FLAG,
    o.HIT_RULE,
    o.MIN_RETURN_THRESHOLD,
    o.EVAL_STATUS,
    o.CALCULATED_AT,
    iff(o.HIT_FLAG, 1, 0) as HIT_INT,
    (o.EVAL_STATUS = 'SUCCESS') as IS_SUCCESS,
    iff(o.EXIT_TS is null, null, datediff('minute', o.ENTRY_TS, o.EXIT_TS)) as HOLD_MINUTES
from MIP.APP.RECOMMENDATION_LOG r
join MIP.APP.RECOMMENDATION_OUTCOMES o
  on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID;

-- ------------------------------------------------------------------------------
-- V_TRAINING_KPIS
-- Aggregated by (PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS).
-- Success-only metrics use FILTER (WHERE is_success).
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRAINING_KPIS as
select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    count(*) as N_SIGNALS,
    count_if(IS_SUCCESS) as N_SUCCESS,
    avg(HIT_INT) filter (where IS_SUCCESS) as HIT_RATE_SUCCESS,
    avg(REALIZED_RETURN) filter (where IS_SUCCESS) as AVG_RETURN_SUCCESS,
    median(REALIZED_RETURN) filter (where IS_SUCCESS) as MEDIAN_RETURN_SUCCESS,
    stddev(REALIZED_RETURN) filter (where IS_SUCCESS) as STDDEV_RETURN_SUCCESS,
    avg(abs(REALIZED_RETURN)) filter (where IS_SUCCESS) as AVG_ABS_RETURN_SUCCESS,
    avg(REALIZED_RETURN) filter (where IS_SUCCESS)
        / nullif(stddev(REALIZED_RETURN) filter (where IS_SUCCESS), 0) as SHARPE_LIKE_SUCCESS,
    max(SIGNAL_TS) as LAST_SIGNAL_TS
from MIP.MART.V_SIGNAL_OUTCOMES_BASE
group by
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS;

-- ------------------------------------------------------------------------------
-- V_TRAINING_LEADERBOARD
-- V_TRAINING_KPIS filtered to n_success >= 30 (min_success_signals).
-- Rank by: sharpe_like_success desc, hit_rate_success desc, avg_return_success desc.
-- Views cannot parameterize; 30 is documented as the default.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRAINING_LEADERBOARD as
select *
from MIP.MART.V_TRAINING_KPIS
where N_SUCCESS >= 30;

-- ------------------------------------------------------------------------------
-- Acceptance tests — run after deploy; use in PR description.
-- ------------------------------------------------------------------------------
-- select * from MIP.MART.V_SIGNAL_OUTCOMES_BASE order by CALCULATED_AT desc limit 20;
-- select * from MIP.MART.V_TRAINING_KPIS order by sharpe_like_success desc nulls last limit 25;
-- select * from MIP.MART.V_TRAINING_LEADERBOARD limit 50;

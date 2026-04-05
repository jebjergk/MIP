-- 037_mart_short_momentum_research_views.sql
-- Purpose: Research-only views including SHORT signals (not production trust paths).

use role MIP_ADMIN_ROLE;
use database MIP;

-- ------------------------------------------------------------------------------
-- V_SIGNAL_OUTCOMES_BASE_RESEARCH
-- LOG join OUTCOMES; all SIGNAL_DIRECTION; REALIZED_RETURN from outcomes (direction-aware eval).
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_SIGNAL_OUTCOMES_BASE_RESEARCH as
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
    case
        when trim(coalesce(r.SIGNAL_DIRECTION, '')) = 'SHORT' then 'SHORT'
        else 'LONG'
    end as SIGNAL_DIRECTION,
    o.HORIZON_BARS,
    o.ENTRY_TS,
    o.EXIT_TS,
    o.ENTRY_PRICE,
    o.EXIT_PRICE,
    o.REALIZED_RETURN,
    o.DIRECTION as OUTCOME_DIRECTION,
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
-- V_TRAINING_KPIS_RESEARCH
-- Same aggregates as V_TRAINING_KPIS, grouped by SIGNAL_DIRECTION.
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRAINING_KPIS_RESEARCH as
select
    'CURRENT' as TRAINING_VERSION,
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    SIGNAL_DIRECTION,
    count(*) as N_SIGNALS,
    count_if(IS_SUCCESS) as N_SUCCESS,
    avg(case when IS_SUCCESS then HIT_INT end) as HIT_RATE_SUCCESS,
    avg(case when IS_SUCCESS then REALIZED_RETURN end) as AVG_RETURN_SUCCESS,
    median(case when IS_SUCCESS then REALIZED_RETURN end) as MEDIAN_RETURN_SUCCESS,
    stddev(case when IS_SUCCESS then REALIZED_RETURN end) as STDDEV_RETURN_SUCCESS,
    avg(case when IS_SUCCESS then abs(REALIZED_RETURN) end) as AVG_ABS_RETURN_SUCCESS,
    avg(case when IS_SUCCESS then REALIZED_RETURN end)
        / nullif(stddev(case when IS_SUCCESS then REALIZED_RETURN end), 0) as SHARPE_LIKE_SUCCESS,
    max(SIGNAL_TS) as LAST_SIGNAL_TS
from MIP.MART.V_SIGNAL_OUTCOMES_BASE_RESEARCH
group by
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    SIGNAL_DIRECTION;

-- ------------------------------------------------------------------------------
-- V_TRAINING_LEADERBOARD_RESEARCH
-- Research leaderboard with N_SUCCESS >= 30 per (pattern, horizon, direction).
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_TRAINING_LEADERBOARD_RESEARCH as
select *
from MIP.MART.V_TRAINING_KPIS_RESEARCH
where N_SUCCESS >= 30;

-- ------------------------------------------------------------------------------
-- V_SHORT_RESEARCH_EVIDENCE_STAGE
-- Coarse SHORT-only counts for Training Status (not trust).
-- ------------------------------------------------------------------------------
create or replace view MIP.MART.V_SHORT_RESEARCH_EVIDENCE_STAGE as
select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    count_if(IS_SUCCESS) as N_SHORT_SUCCESS,
    count(*) as N_SHORT_TOTAL,
    avg(case when IS_SUCCESS then REALIZED_RETURN end) as AVG_RETURN_SHORT_SUCCESS,
    case
        when count_if(IS_SUCCESS) >= 30 then 'RICH'
        when count_if(IS_SUCCESS) >= 10 then 'EMERGING'
        when count(*) > 0 then 'SPARSE'
        else 'NONE'
    end as EVIDENCE_STAGE
from MIP.MART.V_SIGNAL_OUTCOMES_BASE_RESEARCH
where SIGNAL_DIRECTION = 'SHORT'
group by
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS;

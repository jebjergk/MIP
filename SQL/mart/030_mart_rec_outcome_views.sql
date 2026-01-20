-- 030_mart_rec_outcome_views.sql
-- Purpose:
--   Coverage and performance views for recommendation outcomes

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.REC_OUTCOME_COVERAGE as
select
    r.PATTERN_ID,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    o.HORIZON_BARS,
    count(*) as N_TOTAL,
    count_if(o.EVAL_STATUS = 'SUCCESS') as N_SUCCESS,
    count_if(o.EVAL_STATUS = 'SUCCESS') / nullif(count(*), 0) as COVERAGE_RATE,
    min(case when o.EVAL_STATUS != 'SUCCESS' then o.ENTRY_TS end) as OLDEST_NOT_READY_ENTRY_TS,
    max(case when o.EVAL_STATUS != 'SUCCESS' then o.ENTRY_TS end) as NEWEST_NOT_READY_ENTRY_TS,
    max(case when o.EVAL_STATUS = 'SUCCESS' then o.ENTRY_TS end) as LATEST_MATURED_ENTRY_TS
from MIP.APP.RECOMMENDATION_OUTCOMES o
join MIP.APP.RECOMMENDATION_LOG r
  on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
group by
    r.PATTERN_ID,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    o.HORIZON_BARS;

create or replace view MIP.MART.REC_OUTCOME_PERF as
select
    r.PATTERN_ID,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    o.HORIZON_BARS,
    count(*) as N,
    avg(o.REALIZED_RETURN) as AVG_RETURN,
    median(o.REALIZED_RETURN) as MEDIAN_RETURN,
    stddev_samp(o.REALIZED_RETURN) as STDDEV_RETURN,
    min(o.REALIZED_RETURN) as MIN_RETURN,
    max(o.REALIZED_RETURN) as MAX_RETURN,
    avg(case when o.HIT_FLAG then 1 else 0 end) as HIT_RATE,
    avg(case when o.REALIZED_RETURN > 0 then o.REALIZED_RETURN end) as AVG_WIN,
    avg(case when o.REALIZED_RETURN < 0 then o.REALIZED_RETURN end) as AVG_LOSS,
    corr(r.SCORE, o.REALIZED_RETURN) as SCORE_RETURN_CORR
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

create or replace view MIP.MART.REC_PATTERN_TRUST_RANKING as
select
    c.PATTERN_ID,
    c.MARKET_TYPE,
    c.INTERVAL_MINUTES,
    c.HORIZON_BARS,
    c.N_TOTAL,
    c.N_SUCCESS,
    c.COVERAGE_RATE,
    c.OLDEST_NOT_READY_ENTRY_TS,
    c.NEWEST_NOT_READY_ENTRY_TS,
    c.LATEST_MATURED_ENTRY_TS,
    p.N as PERF_N,
    p.AVG_RETURN,
    p.MEDIAN_RETURN,
    p.STDDEV_RETURN,
    p.MIN_RETURN,
    p.MAX_RETURN,
    p.HIT_RATE,
    p.AVG_WIN,
    p.AVG_LOSS,
    p.SCORE_RETURN_CORR,
    (coalesce(c.COVERAGE_RATE, 0)
        * coalesce(p.HIT_RATE, 0)
        * ln(1 + coalesce(p.N, 0))) as TRUST_SCORE
from MIP.MART.REC_OUTCOME_COVERAGE c
left join MIP.MART.REC_OUTCOME_PERF p
  on p.PATTERN_ID = c.PATTERN_ID
 and p.MARKET_TYPE = c.MARKET_TYPE
 and p.INTERVAL_MINUTES = c.INTERVAL_MINUTES
 and p.HORIZON_BARS = c.HORIZON_BARS;

create or replace view MIP.MART.V_TRUSTED_SIGNALS as
select
    c.PATTERN_ID,
    c.MARKET_TYPE,
    c.INTERVAL_MINUTES,
    c.HORIZON_BARS,
    c.N_SUCCESS,
    c.COVERAGE_RATE,
    p.AVG_RETURN,
    case
        when c.N_SUCCESS >= 20
         and c.COVERAGE_RATE >= 0.6
         and p.AVG_RETURN > 0
        then true
        else false
    end as IS_TRUSTED
from MIP.MART.REC_OUTCOME_COVERAGE c
join MIP.MART.REC_OUTCOME_PERF p
  on p.PATTERN_ID = c.PATTERN_ID
 and p.MARKET_TYPE = c.MARKET_TYPE
 and p.INTERVAL_MINUTES = c.INTERVAL_MINUTES
 and p.HORIZON_BARS = c.HORIZON_BARS;

create or replace view MIP.MART.V_PORTFOLIO_SIGNALS as
select
    rl.RECOMMENDATION_ID,
    rl.TS,
    rl.SYMBOL,
    rl.MARKET_TYPE,
    rl.INTERVAL_MINUTES,
    rl.PATTERN_ID,
    rl.SCORE,
    ts.HORIZON_BARS
from MIP.APP.RECOMMENDATION_LOG rl
join MIP.MART.V_TRUSTED_SIGNALS ts
  on ts.PATTERN_ID = rl.PATTERN_ID
 and ts.MARKET_TYPE = rl.MARKET_TYPE
 and ts.INTERVAL_MINUTES = rl.INTERVAL_MINUTES
where rl.INTERVAL_MINUTES = 1440
  and ts.IS_TRUSTED = true
qualify row_number() over (
    partition by rl.RECOMMENDATION_ID, ts.HORIZON_BARS
    order by rl.TS desc
) = 1;

create or replace view MIP.MART.SCORE_CALIBRATION as
with scored as (
    select
        r.PATTERN_ID,
        r.MARKET_TYPE,
        o.HORIZON_BARS,
        r.SCORE,
        o.REALIZED_RETURN,
        o.HIT_FLAG,
        ntile(10) over (
            partition by r.PATTERN_ID, r.MARKET_TYPE, o.HORIZON_BARS
            order by r.SCORE
        ) as SCORE_DECILE
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
    where o.EVAL_STATUS = 'SUCCESS'
      and o.REALIZED_RETURN is not null
)
select
    PATTERN_ID,
    MARKET_TYPE,
    HORIZON_BARS,
    SCORE_DECILE,
    count(*) as N,
    avg(REALIZED_RETURN) as AVG_RETURN,
    median(REALIZED_RETURN) as MEDIAN_RETURN,
    avg(case when HIT_FLAG then 1 else 0 end) as HIT_RATE
from scored
group by
    PATTERN_ID,
    MARKET_TYPE,
    HORIZON_BARS,
    SCORE_DECILE;

create or replace view MIP.MART.V_SIGNALS_WITH_EXPECTED_RETURN as
with scored as (
    select
        r.RECOMMENDATION_ID,
        r.PATTERN_ID,
        r.MARKET_TYPE,
        o.HORIZON_BARS,
        r.SCORE,
        ntile(10) over (
            partition by r.PATTERN_ID, r.MARKET_TYPE, o.HORIZON_BARS
            order by r.SCORE
        ) as SCORE_DECILE
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
)
select
    s.RECOMMENDATION_ID,
    s.HORIZON_BARS,
    s.SCORE,
    s.SCORE_DECILE,
    c.AVG_RETURN as EXPECTED_RETURN
from scored s
left join MIP.MART.SCORE_CALIBRATION c
  on c.PATTERN_ID = s.PATTERN_ID
 and c.MARKET_TYPE = s.MARKET_TYPE
 and c.HORIZON_BARS = s.HORIZON_BARS
 and c.SCORE_DECILE = s.SCORE_DECILE;

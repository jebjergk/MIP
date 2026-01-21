-- v_score_calibration.sql
-- Purpose: Score calibration deciles by pattern and horizon

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_SCORE_CALIBRATION as
with scored as (
    select
        r.PATTERN_ID,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        o.HORIZON_BARS,
        r.SCORE,
        o.REALIZED_RETURN,
        o.HIT_FLAG,
        ntile(10) over (
            partition by r.PATTERN_ID, r.MARKET_TYPE, r.INTERVAL_MINUTES, o.HORIZON_BARS
            order by r.SCORE
        ) as SCORE_DECILE
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
    where o.EVAL_STATUS = 'SUCCESS'
      and r.SCORE is not null
      and o.REALIZED_RETURN is not null
)
select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    SCORE_DECILE,
    count(*) as N,
    avg(REALIZED_RETURN) as AVG_RETURN,
    median(REALIZED_RETURN) as MEDIAN_RETURN,
    avg(case when HIT_FLAG is null then null else iff(HIT_FLAG, 1, 0) end) as HIT_RATE,
    min(SCORE) as SCORE_MIN,
    max(SCORE) as SCORE_MAX
from scored
group by
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    SCORE_DECILE;

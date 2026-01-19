-- /sql/app/090_recommendation_outcome_kpis.sql
-- Purpose: Aggregate recommendation outcomes by pattern_id for KPI reporting

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.APP.V_PATTERN_KPIS as
select
    r.PATTERN_ID,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    o.HORIZON_BARS,
    count(*) as SAMPLE_COUNT,
    avg(case when o.HIT_FLAG then 1 else 0 end) as HIT_RATE,
    avg(o.REALIZED_RETURN) as AVG_FORWARD_RETURN,
    median(o.REALIZED_RETURN) as MEDIAN_FORWARD_RETURN,
    min(o.REALIZED_RETURN) as MIN_FORWARD_RETURN,
    max(o.REALIZED_RETURN) as MAX_FORWARD_RETURN,
    max(o.CALCULATED_AT) as LAST_CALCULATED_AT
from MIP.APP.RECOMMENDATION_OUTCOMES o
join MIP.APP.RECOMMENDATION_LOG r
  on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
where o.REALIZED_RETURN is not null
  and o.EVAL_STATUS = 'SUCCESS'
group by r.PATTERN_ID, r.MARKET_TYPE, r.INTERVAL_MINUTES, o.HORIZON_BARS;

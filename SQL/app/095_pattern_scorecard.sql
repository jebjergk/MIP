-- /sql/app/095_pattern_scorecard.sql
-- Purpose: Aggregate recommendation outcomes into a pattern training scorecard

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.APP.V_PATTERN_SCORECARD as
select
    r.PATTERN_ID,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    count(*) as SAMPLE_COUNT,
    avg(case when o.HIT_FLAG then 1 else 0 end) as HIT_RATE,
    avg(o.REALIZED_RETURN) as AVG_FORWARD_RETURN,
    median(o.REALIZED_RETURN) as MEDIAN_FORWARD_RETURN,
    min(o.REALIZED_RETURN) as MIN_FORWARD_RETURN,
    max(o.REALIZED_RETURN) as MAX_FORWARD_RETURN,
    max(o.ENTRY_TS)::date as LAST_SIGNAL_DATE,
    case
        when count(*) >= 30
            and avg(case when o.HIT_FLAG then 1 else 0 end) >= 0.55
            and avg(o.REALIZED_RETURN) > 0
            then 'TRUSTED'
        when count(*) >= 20
            then 'WATCH'
        else 'EXPERIMENTAL'
    end as PATTERN_STATUS
from MIP.APP.RECOMMENDATION_OUTCOMES o
join MIP.APP.RECOMMENDATION_LOG r
  on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
where o.REALIZED_RETURN is not null
  and o.EVAL_STATUS = 'SUCCESS'
  and o.HORIZON_BARS = 5
  and o.ENTRY_TS >= dateadd(day, -90, current_date())
group by r.PATTERN_ID, r.MARKET_TYPE, r.INTERVAL_MINUTES;

-- /sql/app/095_pattern_scorecard.sql
-- Purpose: Aggregate recommendation outcomes into a pattern training scorecard

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.APP.V_PATTERN_SCORECARD as
select
    o.PATTERN_ID,
    o.MARKET_TYPE,
    o.INTERVAL_MINUTES,
    count(*) as SAMPLE_COUNT,
    avg(case when o.HIT then 1 else 0 end) as HIT_RATE,
    avg(o.FORWARD_RETURN) as AVG_FORWARD_RETURN,
    median(o.FORWARD_RETURN) as MEDIAN_FORWARD_RETURN,
    min(o.FORWARD_RETURN) as MIN_FORWARD_RETURN,
    max(o.FORWARD_RETURN) as MAX_FORWARD_RETURN,
    max(o.REC_TS)::date as LAST_SIGNAL_DATE,
    case
        when count(*) >= 30
            and avg(case when o.HIT then 1 else 0 end) >= 0.55
            and avg(o.FORWARD_RETURN) > 0
            then 'TRUSTED'
        when count(*) >= 20
            then 'WATCH'
        else 'EXPERIMENTAL'
    end as PATTERN_STATUS
from MIP.APP.RECOMMENDATION_OUTCOMES o
where o.FORWARD_RETURN is not null
  and o.HORIZON_DAYS = 5
  and o.REC_TS >= dateadd(day, -90, current_date())
group by o.PATTERN_ID, o.MARKET_TYPE, o.INTERVAL_MINUTES;

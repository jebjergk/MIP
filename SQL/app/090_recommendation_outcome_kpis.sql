-- /sql/app/090_recommendation_outcome_kpis.sql
-- Purpose: Aggregate recommendation outcomes by pattern_id

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.APP.VW_RECOMMENDATION_OUTCOME_KPIS as
select
    r.PATTERN_ID,
    o.HORIZON_DAYS,
    count(*) as OUTCOME_COUNT,
    avg(case when o.RETURN_FORWARD > 0 then 1 else 0 end) as HIT_RATE,
    avg(o.RETURN_FORWARD) as AVG_FORWARD_RETURN,
    min(o.RETURN_FORWARD) as MIN_FORWARD_RETURN,
    max(o.RETURN_FORWARD) as MAX_FORWARD_RETURN
from MIP.APP.RECOMMENDATION_OUTCOMES o
join MIP.APP.RECOMMENDATION_LOG r
  on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
where o.RETURN_FORWARD is not null
group by r.PATTERN_ID, o.HORIZON_DAYS;

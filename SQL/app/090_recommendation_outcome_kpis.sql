-- /sql/app/090_recommendation_outcome_kpis.sql
-- Purpose: Aggregate recommendation outcomes by pattern_id for KPI reporting

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.APP.V_PATTERN_KPIS as
select
    o.PATTERN_ID,
    o.MARKET_TYPE,
    o.INTERVAL_MINUTES,
    o.HORIZON_DAYS,
    count(*) as SAMPLE_COUNT,
    avg(case when o.HIT then 1 else 0 end) as HIT_RATE,
    avg(o.FORWARD_RETURN) as AVG_FORWARD_RETURN,
    median(o.FORWARD_RETURN) as MEDIAN_FORWARD_RETURN,
    min(o.FORWARD_RETURN) as MIN_FORWARD_RETURN,
    max(o.FORWARD_RETURN) as MAX_FORWARD_RETURN,
    max(o.CALCULATED_AT) as LAST_CALCULATED_AT
from MIP.APP.RECOMMENDATION_OUTCOMES o
where o.FORWARD_RETURN is not null
group by o.PATTERN_ID, o.MARKET_TYPE, o.INTERVAL_MINUTES, o.HORIZON_DAYS;

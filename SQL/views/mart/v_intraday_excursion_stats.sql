-- v_intraday_excursion_stats.sql
-- Purpose: Per-pattern aggregation of max favorable/adverse excursion.
-- Shows whether winners run or losers gap — useful for stop-loss/take-profit design.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_INTRADAY_EXCURSION_STATS as
with patterns as (
    select PATTERN_ID, NAME, PATTERN_TYPE
    from MIP.APP.PATTERN_DEFINITION
)
select
    o.PATTERN_ID,
    p.NAME as PATTERN_NAME,
    p.PATTERN_TYPE,
    o.MARKET_TYPE,
    o.INTERVAL_MINUTES,
    o.HORIZON_BARS,
    count_if(EVAL_STATUS = 'SUCCESS') as N_EVALUATED,
    avg(MAX_FAVORABLE_EXCURSION) as AVG_MFE,
    avg(MAX_ADVERSE_EXCURSION) as AVG_MAE,
    median(MAX_FAVORABLE_EXCURSION) as MEDIAN_MFE,
    median(MAX_ADVERSE_EXCURSION) as MEDIAN_MAE,
    max(MAX_FAVORABLE_EXCURSION) as MAX_MFE,
    min(MAX_ADVERSE_EXCURSION) as WORST_MAE,
    avg(case when NET_HIT_FLAG then MAX_FAVORABLE_EXCURSION end) as AVG_MFE_WINNERS,
    avg(case when not NET_HIT_FLAG then MAX_ADVERSE_EXCURSION end) as AVG_MAE_LOSERS,
    case when avg(MAX_ADVERSE_EXCURSION) != 0
         then abs(avg(MAX_FAVORABLE_EXCURSION) / nullif(avg(MAX_ADVERSE_EXCURSION), 0))
         else null end as MFE_MAE_RATIO
from MIP.MART.V_INTRADAY_OUTCOMES_FEE_ADJUSTED o
left join patterns p on p.PATTERN_ID = o.PATTERN_ID
where o.EVAL_STATUS = 'SUCCESS'
  and (o.MAX_FAVORABLE_EXCURSION is not null or o.MAX_ADVERSE_EXCURSION is not null)
group by o.PATTERN_ID, p.NAME, p.PATTERN_TYPE,
         o.MARKET_TYPE, o.INTERVAL_MINUTES, o.HORIZON_BARS;

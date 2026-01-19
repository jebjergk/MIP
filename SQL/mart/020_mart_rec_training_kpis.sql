-- 020_mart_rec_training_kpis.sql
-- Purpose:
--   Training KPI view for recommendation outcomes by pattern/market/interval/horizon

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.REC_TRAINING_KPIS as
with base as (
    select
        r.PATTERN_ID,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        o.HORIZON_BARS,
        o.REALIZED_RETURN,
        o.HIT_FLAG,
        o.CALCULATED_AT,
        o.ENTRY_TS,
        case when o.REALIZED_RETURN > 0 then 1 else 0 end as WIN_FLAG,
        case when o.REALIZED_RETURN < 0 then 1 else 0 end as LOSS_FLAG
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
    where o.EVAL_STATUS = 'SUCCESS'
      and o.REALIZED_RETURN is not null
),
loss_groups as (
    select
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS,
        LOSS_FLAG,
        sum(case when LOSS_FLAG = 0 then 1 else 0 end) over (
            partition by PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS
            order by ENTRY_TS, CALCULATED_AT
            rows between unbounded preceding and current row
        ) as LOSS_GROUP
    from base
),
loss_streaks as (
    select
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS,
        max(case when LOSS_FLAG = 1 then LOSS_STREAK else 0 end) as MAX_LOSS_STREAK
    from (
        select
            PATTERN_ID,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            HORIZON_BARS,
            LOSS_FLAG,
            LOSS_GROUP,
            count_if(LOSS_FLAG = 1) over (
                partition by PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS, LOSS_GROUP
            ) as LOSS_STREAK
        from loss_groups
    )
    group by PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS
)
select
    b.PATTERN_ID,
    b.MARKET_TYPE,
    b.INTERVAL_MINUTES,
    b.HORIZON_BARS,
    count(*) as N,
    avg(case when b.HIT_FLAG then 1 else 0 end) as HIT_RATE,
    avg(b.REALIZED_RETURN) as AVG_RETURN,
    median(b.REALIZED_RETURN) as MEDIAN_RETURN,
    avg(case when b.WIN_FLAG = 1 then b.REALIZED_RETURN end) as AVG_WIN,
    avg(case when b.LOSS_FLAG = 1 then b.REALIZED_RETURN end) as AVG_LOSS,
    (avg(case when b.WIN_FLAG = 1 then b.REALIZED_RETURN end)
        * avg(case when b.HIT_FLAG then 1 else 0 end))
      + (avg(case when b.LOSS_FLAG = 1 then b.REALIZED_RETURN end)
        * (1 - avg(case when b.HIT_FLAG then 1 else 0 end))) as EXPECTANCY,
    stddev_samp(b.REALIZED_RETURN) as RETURN_STDDEV,
    coalesce(ls.MAX_LOSS_STREAK, 0) as MAX_LOSS_STREAK,
    count(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp()) then 1 end) as N_30D,
    avg(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp())
        then case when b.HIT_FLAG then 1 else 0 end end) as HIT_RATE_30D,
    avg(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp())
        then b.REALIZED_RETURN end) as AVG_RETURN_30D,
    median(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp())
        then b.REALIZED_RETURN end) as MEDIAN_RETURN_30D,
    avg(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp()) and b.WIN_FLAG = 1
        then b.REALIZED_RETURN end) as AVG_WIN_30D,
    avg(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp()) and b.LOSS_FLAG = 1
        then b.REALIZED_RETURN end) as AVG_LOSS_30D,
    (avg(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp()) and b.WIN_FLAG = 1
        then b.REALIZED_RETURN end)
        * avg(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp())
            then case when b.HIT_FLAG then 1 else 0 end end))
      + (avg(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp()) and b.LOSS_FLAG = 1
        then b.REALIZED_RETURN end)
        * (1 - avg(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp())
            then case when b.HIT_FLAG then 1 else 0 end end))) as EXPECTANCY_30D,
    stddev_samp(case when b.CALCULATED_AT >= dateadd('day', -30, current_timestamp())
        then b.REALIZED_RETURN end) as RETURN_STDDEV_30D,
    count(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp()) then 1 end) as N_90D,
    avg(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp())
        then case when b.HIT_FLAG then 1 else 0 end end) as HIT_RATE_90D,
    avg(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp())
        then b.REALIZED_RETURN end) as AVG_RETURN_90D,
    median(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp())
        then b.REALIZED_RETURN end) as MEDIAN_RETURN_90D,
    avg(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp()) and b.WIN_FLAG = 1
        then b.REALIZED_RETURN end) as AVG_WIN_90D,
    avg(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp()) and b.LOSS_FLAG = 1
        then b.REALIZED_RETURN end) as AVG_LOSS_90D,
    (avg(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp()) and b.WIN_FLAG = 1
        then b.REALIZED_RETURN end)
        * avg(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp())
            then case when b.HIT_FLAG then 1 else 0 end end))
      + (avg(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp()) and b.LOSS_FLAG = 1
        then b.REALIZED_RETURN end)
        * (1 - avg(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp())
            then case when b.HIT_FLAG then 1 else 0 end end))) as EXPECTANCY_90D,
    stddev_samp(case when b.CALCULATED_AT >= dateadd('day', -90, current_timestamp())
        then b.REALIZED_RETURN end) as RETURN_STDDEV_90D
from base b
left join loss_streaks ls
  on ls.PATTERN_ID = b.PATTERN_ID
 and ls.MARKET_TYPE = b.MARKET_TYPE
 and ls.INTERVAL_MINUTES = b.INTERVAL_MINUTES
 and ls.HORIZON_BARS = b.HORIZON_BARS
group by
    b.PATTERN_ID,
    b.MARKET_TYPE,
    b.INTERVAL_MINUTES,
    b.HORIZON_BARS,
    ls.MAX_LOSS_STREAK;

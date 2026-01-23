-- recommendation_log_update_smoke.sql
-- Smoke test for recommendation log update at latest market bar ts

set market_type = (
    select coalesce(
        (select MARKET_TYPE from MIP.APP.INGEST_UNIVERSE where coalesce(IS_ENABLED, true) limit 1),
        (select MARKET_TYPE from MIP.MART.MARKET_BARS limit 1)
    )
);
set interval_minutes = 1440;

call MIP.APP.SP_PIPELINE_GENERATE_RECOMMENDATIONS($market_type, $interval_minutes);

with latest_ts as (
    select max(TS) as latest_ts
    from MIP.MART.MARKET_BARS
    where MARKET_TYPE = $market_type
      and INTERVAL_MINUTES = $interval_minutes
), recs as (
    select count(*) as rec_count
    from MIP.APP.RECOMMENDATION_LOG r
    join latest_ts l
      on r.TS = l.latest_ts
    where r.MARKET_TYPE = $market_type
      and r.INTERVAL_MINUTES = $interval_minutes
)
select
    $market_type as market_type,
    $interval_minutes as interval_minutes,
    latest_ts.latest_ts,
    recs.rec_count
from latest_ts, recs;

with latest_returns as (
    select max(TS) as latest_ts
    from MIP.MART.MARKET_RETURNS
    where MARKET_TYPE = $market_type
      and INTERVAL_MINUTES = $interval_minutes
), latest_bars as (
    select max(TS) as latest_ts
    from MIP.MART.MARKET_BARS
    where MARKET_TYPE = $market_type
      and INTERVAL_MINUTES = $interval_minutes
), recs_on_returns as (
    select count(*) as rec_count
    from MIP.APP.RECOMMENDATION_LOG r
    join latest_returns l
      on r.TS = l.latest_ts
    where r.MARKET_TYPE = $market_type
      and r.INTERVAL_MINUTES = $interval_minutes
), recs_on_bars as (
    select count(*) as rec_count
    from MIP.APP.RECOMMENDATION_LOG r
    join latest_bars l
      on r.TS = l.latest_ts
    where r.MARKET_TYPE = $market_type
      and r.INTERVAL_MINUTES = $interval_minutes
)
select
    $market_type as market_type,
    $interval_minutes as interval_minutes,
    latest_returns.latest_ts as latest_returns_ts,
    recs_on_returns.rec_count as recs_at_returns_ts,
    latest_bars.latest_ts as latest_bars_ts,
    recs_on_bars.rec_count as recs_at_bars_ts
from latest_returns, recs_on_returns, latest_bars, recs_on_bars;

select
    EVENT_TS,
    STATUS,
    ROWS_AFFECTED,
    DETAILS
from MIP.APP.MIP_AUDIT_LOG
where EVENT_NAME = 'SP_GENERATE_MOMENTUM_RECS'
order by EVENT_TS desc
limit 10;

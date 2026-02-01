use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();

set from_date = '2025-10-15';
set to_date   = '2025-11-15';
set run_portfolios = false;
set run_briefs = false;

call MIP.APP.SP_REPLAY_TIME_TRAVEL(to_date($from_date), to_date($to_date), $run_portfolios, $run_briefs);

--delete from mip.app.mip_audit_log where event_ts::date = '2026-02-01';

call MIP.APP.SP_REPLAY_TIME_TRAVEL(
  to_date($from_date),
  to_date($to_date),
  $run_portfolios,
  $run_briefs
);

select count(*) as outcomes_last_2h
from MIP.APP.RECOMMENDATION_OUTCOMES
where calculated_at >= dateadd(hour, -2, current_timestamp());

set replay_batch_id = 'a1c99fc3-49d3-4be7-a649-47adc1b6f591';
set from_date = '2025-10-15';
set to_date   = '2025-11-15';


select count(*) as outcomes_last_2h
from MIP.APP.RECOMMENDATION_OUTCOMES
where calculated_at >= dateadd(hour, -2, current_timestamp());

select count(*) as outcomes_total
from MIP.APP.RECOMMENDATION_OUTCOMES;

select ts::date as d, market_type, count(*) as recs
from MIP.APP.RECOMMENDATION_LOG
where ts::date between to_date($from_date) and to_date($to_date)
group by 1,2
order by 1,2;

select market_type, count(*) as recs
from MIP.APP.RECOMMENDATION_LOG
where ts::date between to_date($from_date) and to_date($to_date)
group by 1
order by 1;

select pattern_id, symbol, market_type, interval_minutes, ts, count(*) as n
from MIP.APP.RECOMMENDATION_LOG
where ts::date between to_date($from_date) and to_date($to_date)
group by 1,2,3,4,5
having count(*) > 1
order by n desc;

select recommendation_id, horizon_bars, count(*) as n
from MIP.APP.RECOMMENDATION_OUTCOMES
where calculated_at >= dateadd(hour, -6, current_timestamp()) -- replay window
group by 1,2
having count(*) > 1
order by n desc;

select
  min(ts) as min_rec_ts,
  max(ts) as max_rec_ts,
  count(*) as total_recs
from MIP.APP.RECOMMENDATION_LOG
where ts::date between to_date($from_date) and to_date($to_date);


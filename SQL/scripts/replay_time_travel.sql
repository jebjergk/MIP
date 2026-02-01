-- replay_time_travel.sql
-- One-off historical replay (time travel) excluding ingestion.
-- Loops day-by-day from :from_date to :to_date; runs returns refresh, recommendations,
-- evaluation, and optionally portfolio + briefs. Does NOT call ingestion.
-- Logs REPLAY events via MIP.APP.SP_LOG_EVENT.
--
-- P0 — Automatic "max available end date": to_date defaults to max(ts)::date in MARKET_BARS.
-- P0 — Optional "full-universe start date": set use_full_universe_start = true to set from_date
--      to the date when all symbols have begun (max of per-symbol min(ts)).
--
-- Usage (set variables then run this script, or run as a single call):
--   set interval_minutes = 1440;
--   set from_date = '2024-01-01';                    -- optional; default last 7 days or full-universe start
--   set to_date   = '2024-01-31';                     -- optional; default max available end date
--   set use_full_universe_start = false;              -- optional; true = from_date = when all symbols have data
--   set run_portfolios = false;
--   set run_briefs     = false;
--   call MIP.APP.SP_REPLAY_TIME_TRAVEL($from_date, $to_date, $run_portfolios, $run_briefs);
--
-- Acceptance: Outcomes count increases for older dates; re-running the same range does not create duplicates.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Interval for bar universe (default 1440 = daily)
set interval_minutes = coalesce($interval_minutes, 1440);

-- P0 — to_date: default to max available end date in MARKET_BARS for this interval
set to_date = coalesce($to_date, (
  select max(ts)::date
  from MIP.MART.MARKET_BARS
  where interval_minutes = $interval_minutes
));

-- P0 — Optional full-universe start: date when all symbols have begun (max of per-symbol min(ts))
set from_date_full_universe = (
  select max(min_ts)::date
  from (
    select symbol, min(ts) as min_ts
    from MIP.MART.MARKET_BARS
    where interval_minutes = $interval_minutes
    group by 1
  )
);
set use_full_universe_start = coalesce($use_full_universe_start, false);
set from_date = iff($use_full_universe_start, $from_date_full_universe, coalesce($from_date, dateadd(day, -7, current_date())));

set run_portfolios = coalesce($run_portfolios, false);
set run_briefs     = coalesce($run_briefs, false);

call MIP.APP.SP_REPLAY_TIME_TRAVEL(
    to_date($from_date),
    to_date($to_date),
    $run_portfolios,
    $run_briefs
);


-- Controlled dates:

set interval_minutes = 1440;

set use_full_universe_start = true;

set from_date = '2025-08-22';
set to_date   = '2025-08-29';

set run_portfolios = false;
set run_briefs     = false;

call MIP.APP.SP_REPLAY_TIME_TRAVEL(
  to_date($from_date),
  to_date($to_date),
  $run_portfolios,
  $run_briefs
);

-- check outcome
select count(*) as outcomes_total
from MIP.APP.RECOMMENDATION_OUTCOMES;

select ts::date as d, market_type, count(*) as recs
from MIP.APP.RECOMMENDATION_LOG
where ts::date between to_date($from_date) and to_date($to_date)
group by 1,2
order by 1,2;
select pattern_id, symbol, market_type, interval_minutes, ts, count(*) as n
from MIP.APP.RECOMMENDATION_LOG
where ts::date between to_date($from_date) and to_date($to_date)
group by 1,2,3,4,5
having count(*) > 1;

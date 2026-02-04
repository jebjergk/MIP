-- Acceptance checks: pipeline run with rate limit (no new bars) should have
-- PIPELINE.STATUS = SUCCESS_WITH_SKIPS, DETAILS:has_new_bars = false,
-- and downstream PIPELINE_STEP rows either absent or SKIPPED_NO_NEW_BARS.
-- Run after a rate-limited pipeline run (e.g. hit AlphaVantage rate limit).

use role MIP_ADMIN_ROLE;
use database MIP;

-- Last pipeline run with rate limit and no new bars (skipped downstream)
with last_rate_limit_pipeline as (
  select run_id, status, details
  from MIP.APP.MIP_AUDIT_LOG
  where event_type = 'PIPELINE'
    and event_name = 'SP_RUN_DAILY_PIPELINE'
    and details:"pipeline_status_reason"::string = 'RATE_LIMIT'
    and details:"has_new_bars"::boolean = false
  qualify row_number() over (order by event_ts desc) = 1
)
select
  p.run_id,
  p.status                                      as pipeline_status,
  p.details:"has_new_bars"::boolean             as has_new_bars,
  p.details:"latest_market_bars_ts_before"::timestamp_ntz as ts_before,
  p.details:"latest_market_bars_ts_after"::timestamp_ntz  as ts_after,
  (p.status = 'SUCCESS_WITH_SKIPS')             as status_ok,
  (p.details:"has_new_bars"::boolean = false)   as has_new_bars_ok
from last_rate_limit_pipeline p;
-- Expect: status_ok = true, has_new_bars_ok = true

-- Downstream PIPELINE_STEP rows for that run: all skipped (RECOMMENDATIONS, EVALUATION, PORTFOLIO_SIMULATION)
with last_rate_limit_pipeline as (
  select run_id
  from MIP.APP.MIP_AUDIT_LOG
  where event_type = 'PIPELINE'
    and event_name = 'SP_RUN_DAILY_PIPELINE'
    and details:"pipeline_status_reason"::string = 'RATE_LIMIT'
    and details:"has_new_bars"::boolean = false
  qualify row_number() over (order by event_ts desc) = 1
)
select
  a.event_name,
  a.status,
  count(*) as cnt
from MIP.APP.MIP_AUDIT_LOG a
join last_rate_limit_pipeline p on a.parent_run_id = p.run_id
where a.event_type = 'PIPELINE_STEP'
  and a.event_name in ('RECOMMENDATIONS', 'EVALUATION', 'PORTFOLIO_SIMULATION')
group by 1, 2
order by 1, 2;
-- Expect: only status = 'SKIPPED_NO_NEW_BARS' (no SUCCESS rows for these steps)

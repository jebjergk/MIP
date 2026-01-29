-- morning_brief_idempotency_smoke.sql
-- Proves idempotency: two consecutive calls with same (portfolio_id, as_of_ts, run_id, agent_name)
-- result in exactly one row (updated, not duplicated).

use role MIP_ADMIN_ROLE;
use database MIP;

set portfolio_id = 1;
set as_of_ts = (select max(ts)::timestamp_ntz from MIP.MART.MARKET_BARS where interval_minutes = 1440);
set run_id = (
    select RUN_ID
    from MIP.APP.MIP_AUDIT_LOG
    where EVENT_TYPE = 'PIPELINE'
      and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
    order by EVENT_TS desc
    limit 1
);

-- Call twice
call MIP.APP.SP_WRITE_MORNING_BRIEF($portfolio_id, $as_of_ts, $run_id);
call MIP.APP.SP_WRITE_MORNING_BRIEF($portfolio_id, $as_of_ts, $run_id);

-- Must be exactly one row for the deterministic key
select count(*) as n
from MIP.AGENT_OUT.MORNING_BRIEF
where portfolio_id = $portfolio_id
  and as_of_ts = $as_of_ts
  and run_id = $run_id
  and coalesce(agent_name, '') = 'MORNING_BRIEF';

-- Expected: n = 1.

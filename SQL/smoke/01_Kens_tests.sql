use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;
call MIP.APP.SP_RUN_DAILY_PIPELINE();

select
  error_message,
  count(*) as n,
  min(event_ts) as first_seen,
  max(event_ts) as last_seen
from MIP.APP.MIP_AUDIT_LOG
where event_name = 'SP_RUN_DAILY_PIPELINE'
  and status = 'FAIL'
group by 1
order by n desc, last_seen desc;

-- HIGH-001: Smoke – proposals retrievable by canonical RUN_ID only (no SIGNAL_RUN_ID scoping)
-- Use latest pipeline run_id; expect >= 0 rows when filtering ORDER_PROPOSALS by RUN_ID_VARCHAR only.
set run_id = (select RUN_ID from MIP.APP.MIP_AUDIT_LOG where EVENT_NAME = 'SP_RUN_DAILY_PIPELINE' and STATUS = 'SUCCESS' order by EVENT_TS desc limit 1);
select
    'PROPOSALS_BY_RUN_ID' as smoke_check,
    count(*) as proposal_count
from MIP.AGENT_OUT.ORDER_PROPOSALS
where RUN_ID_VARCHAR = $run_id;

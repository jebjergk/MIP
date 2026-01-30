use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;
call MIP.APP.SP_RUN_DAILY_PIPELINE();

select * from MIP.AGENT_OUT.V_MORNING_BRIEF_SUMMARY;

use role MIP_ADMIN_ROLE;
use database MIP;

set test_run_id = (select uuid_string());
set as_of_ts = (select max(ts)::timestamp_ntz from MIP.MART.MARKET_BARS where interval_minutes=1440);
set portfolio_id = 1;

call MIP.APP.SP_WRITE_MORNING_BRIEF($portfolio_id, $as_of_ts, $test_run_id);

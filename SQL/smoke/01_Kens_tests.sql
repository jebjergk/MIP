use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();

set from_date = '2025-10-15';
set to_date   = '2025-10-22';

set from_date = '2025-10-15';
set to_date   = '2025-10-22';

set run_portfolios = false;
set run_briefs     = false;

delete from mip.app.mip_audit_log where event_ts::date = '2026-02-01';

call MIP.APP.SP_REPLAY_TIME_TRAVEL(
  to_date($from_date),
  to_date($to_date),
  $run_portfolios,
  $run_briefs
);

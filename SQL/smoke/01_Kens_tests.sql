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

select
  run_id,
  object_keys(brief:"attribution") as attribution_keys,
  brief:"attribution":"latest_run_id"::string as latest_run_id,
  brief:"attribution":"pipeline_run_id"::string as pipeline_run_id
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id = 'f758610b-e343-4274-a0d9-f775dc9c0f3b';

select * from MIP.AGENT_OUT.V_MORNING_BRIEF_SUMMARY order by as_of_ts;

create table if not exists MIP.AGENT_OUT.MORNING_BRIEF_LEGACY_ARCHIVE as
select *
from MIP.AGENT_OUT.MORNING_BRIEF
where 1=0;


insert into MIP.AGENT_OUT.MORNING_BRIEF_LEGACY_ARCHIVE
select *
from MIP.AGENT_OUT.MORNING_BRIEF
where not (
  agent_name = 'MORNING_BRIEF'
  and brief:"pipeline_run_id"::string = run_id
);

delete from MIP.AGENT_OUT.MORNING_BRIEF
where not (
  agent_name = 'MORNING_BRIEF'
  and brief:"pipeline_run_id"::string = run_id
);

select
  count(*) as total,
  count_if(agent_name = 'MORNING_BRIEF' and brief:"pipeline_run_id"::string = run_id) as canonical,
  count_if(not (agent_name = 'MORNING_BRIEF' and brief:"pipeline_run_id"::string = run_id)) as legacy
from MIP.AGENT_OUT.MORNING_BRIEF;

select
  count(*) as total,
  count_if(pipeline_run_id is null) as pipeline_run_id_nulls
from MIP.AGENT_OUT.V_MORNING_BRIEF_SUMMARY;

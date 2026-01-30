use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;
call MIP.APP.SP_RUN_DAILY_PIPELINE();

select
  portfolio_id,
  as_of_ts,
  agent_name,
  run_id,

  -- show both columns if they exist
  brief:"as_of_ts"::string               as brief_root_as_of_ts,
  brief:"attribution":"as_of_ts"::string as brief_attr_as_of_ts,

  brief_json:"as_of_ts"::string               as brief_json_root_as_of_ts,
  brief_json:"attribution":"as_of_ts"::string as brief_json_attr_as_of_ts

from MIP.AGENT_OUT.MORNING_BRIEF
where run_id = 'd10d4593-84a9-4738-bd3e-200ff23076d5'
  and portfolio_id = 1
  and agent_name = 'MORNING_BRIEF';
select count(*) as bad_rows
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id = 'd10d4593-84a9-4738-bd3e-200ff23076d5'
  and agent_name = 'MORNING_BRIEF'
  and (
    brief:"as_of_ts" is null
    or brief:"attribution":"as_of_ts" is not null
  );

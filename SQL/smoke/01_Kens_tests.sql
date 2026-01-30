use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;
call MIP.APP.SP_RUN_DAILY_PIPELINE();

select run_id, count(*) as brief_rows
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id in ($run_id_1, $run_id_2)
group by run_id
order by run_id;

select run_id_varchar as run_id, count(*) as proposal_rows
from MIP.AGENT_OUT.ORDER_PROPOSALS
where run_id_varchar in ($run_id_1, $run_id_2)
group by run_id_varchar
order by run_id;

select event_ts, event_name, event_type, status, details
from MIP.APP.MIP_AUDIT_LOG
where run_id = 'd10d4593-84a9-4738-bd3e-200ff23076d5'
order by event_ts;

select * from portfolio;

select portfolio_id, as_of_ts, agent_name,
       brief:"pipeline_run_id"::string as pipeline_run_id_in_json,
       brief:"as_of_ts"::string as as_of_ts_in_json
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id = 'd10d4593-84a9-4738-bd3e-200ff23076d5'
order by portfolio_id, agent_name, as_of_ts desc;

delete from MIP.AGENT_OUT.MORNING_BRIEF
where portfolio_id = 0;

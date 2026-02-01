use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;
call MIP.APP.SP_RUN_DAILY_PIPELINE();

select min(ts, max())

dfgbhdo 
desc table MIP.APP.PORTFOLIO;

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


set run_id = (
  select run_id
  from MIP.APP.MIP_AUDIT_LOG
  where event_name = 'SP_RUN_DAILY_PIPELINE'
    and event_type = 'PIPELINE'
  order by event_ts desc
  limit 1
);

select count(*) as n from MIP.AGENT_OUT.MORNING_BRIEF where portfolio_id = 0;
select count(*) as n from MIP.AGENT_OUT.ORDER_PROPOSALS where portfolio_id = 0;

select count(*) as n from MIP.APP.PORTFOLIO_TRADES where portfolio_id = 0;
select count(*) as n from MIP.APP.PORTFOLIO_POSITIONS where portfolio_id = 0;
select count(*) as n from MIP.APP.PORTFOLIO_DAILY where portfolio_id = 0;

-- sanity: make sure PORTFOLIO table truly has no 0
select count(*) as n from MIP.APP.PORTFOLIO where portfolio_id = 0;

SELECT
    SYMBOL,
    INTERVAL_MINUTES,
    COUNT(*) AS CNT,
    MIN(TS) AS MIN_TS,
    MAX(TS) AS MAX_TS
FROM MIP.MART.MARKET_BARS
GROUP BY SYMBOL, INTERVAL_MINUTES
ORDER BY SYMBOL, INTERVAL_MINUTES;

select run_id, agent_name, as_of_ts, pipeline_run_id
from MIP.AGENT_OUT.MORNING_BRIEF
where portfolio_id = 0
order by as_of_ts desc;

select event_ts, event_name, event_type, status, details
from MIP.APP.MIP_AUDIT_LOG
where run_id = '6b9c595e-5b75-4d41-a45f-ee9221d77909'
order by event_ts;

select event_ts, event_name, event_type, status
from MIP.APP.MIP_AUDIT_LOG
where run_id = '01c215ed-0105-fc98-0004-22da002b359e'
  and to_varchar(details) ilike '%"portfolio_id": 0%'
order by event_ts;

delete from MIP.AGENT_OUT.MORNING_BRIEF
where agent_name = 'AGENT_V0_MORNING_BRIEF'
   or portfolio_id <= 0;

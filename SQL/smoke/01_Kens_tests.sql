use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;
call MIP.APP.SP_RUN_DAILY_PIPELINE();

SET test_run_id = uuid_string();
SET portfolio_id = 1;

CALL MIP.APP.SP_WRITE_MORNING_BRIEF(
    $portfolio_id,
    (SELECT max(ts)::timestamp_ntz FROM MIP.MART.MARKET_BARS WHERE interval_minutes = 1440),
    $test_run_id
);

eXECUTE IMMEDIATE $$
DECLARE
    test_run_id   VARCHAR DEFAULT uuid_string();
    as_of_ts      TIMESTAMP_NTZ DEFAULT (SELECT max(ts)::timestamp_ntz FROM MIP.MART.MARKET_BARS WHERE interval_minutes = 1440);
    portfolio_id  NUMBER DEFAULT 1;
BEGIN
     CALL MIP.APP.SP_WRITE_MORNING_BRIEF(:portfolio_id, :as_of_ts, :test_run_id);
   select :test_run_id, :as_of_ts, :portfolio_id;
END;
$$;
select max(ts)::timestamp_ntz from MIP.MART.MARKET_BARS where interval_minutes=1440;
select  *
from MIP.AGENT_OUT.MORNING_BRIEF
where portfolio_id = 1
  and as_of_ts = (select max(ts)::timestamp_ntz from MIP.MART.MARKET_BARS where interval_minutes=1440)
  --and run_id = :test_run_id
  order by as_of_ts desc
  ;

  select
  run_id,
  object_keys(brief) as top_keys
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id in (
  'f1574b51-27ef-4c4f-96dd-863a93067577',
  'e2bc2038-104f-4975-aa17-c01100247f33',
  '75c3b9d3-d676-47ed-9ecd-86e719308110'
);

select
  run_id,
  brief:"pipeline_run_id"::string as pipeline_run_id
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id in (
  '75c3b9d3-d676-47ed-9ecd-86e719308110',
  'f1574b51-27ef-4c4f-96dd-863a93067577',
  'e2bc2038-104f-4975-aa17-c01100247f33'
);

select
  run_id,
  brief:"attribution" as attribution
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id = '75c3b9d3-d676-47ed-9ecd-86e719308110';

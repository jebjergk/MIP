-- Run the pipeline once before executing this smoke SQL.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) No self-parenting (any event type)
with last_pipeline as (
  select run_id
  from MIP.APP.MIP_AUDIT_LOG
  where event_type = 'PIPELINE'
    and event_name = 'SP_RUN_DAILY_PIPELINE'
    and status = 'SUCCESS'
  qualify row_number() over (order by event_ts desc) = 1
)
select
  a.event_type,
  a.event_name,
  count(*) as bad_rows
from MIP.APP.MIP_AUDIT_LOG a
join last_pipeline p
  on a.parent_run_id = p.run_id
where a.run_id = a.parent_run_id
group by 1, 2
order by bad_rows desc;
-- Expect: 0 rows (or 0 bad_rows)

-- 2) All non-PIPELINE events must attach to pipeline
with last_pipeline as (
  select run_id
  from MIP.APP.MIP_AUDIT_LOG
  where event_type = 'PIPELINE'
    and event_name = 'SP_RUN_DAILY_PIPELINE'
    and status = 'SUCCESS'
  qualify row_number() over (order by event_ts desc) = 1
)
select
  sum(case when a.event_type <> 'PIPELINE' and a.parent_run_id = p.run_id then 1 else 0 end) as child_rows_linked,
  sum(case when a.event_type <> 'PIPELINE' and a.parent_run_id is null then 1 else 0 end) as child_rows_missing_parent
from MIP.APP.MIP_AUDIT_LOG a
cross join last_pipeline p
where a.event_ts >= dateadd('hour', -2, current_timestamp());
-- Expect: child_rows_missing_parent = 0

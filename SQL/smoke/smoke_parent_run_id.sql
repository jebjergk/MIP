-- Run the pipeline once before executing this smoke SQL.

with last_pipeline as (
  select run_id
  from MIP.APP.MIP_AUDIT_LOG
  where event_type = 'PIPELINE' and event_name = 'SP_RUN_DAILY_PIPELINE' and status = 'SUCCESS'
  qualify row_number() over (order by event_ts desc) = 1
)
select
  sum(case when a.event_type <> 'PIPELINE' and a.parent_run_id = p.run_id then 1 else 0 end) as child_rows_linked,
  sum(case when a.event_type <> 'PIPELINE' and a.parent_run_id is null then 1 else 0 end) as child_rows_missing_parent
from MIP.APP.MIP_AUDIT_LOG a
cross join last_pipeline p
where a.event_ts >= dateadd('hour', -2, current_timestamp());

-- Expect: child_rows_missing_parent = 0

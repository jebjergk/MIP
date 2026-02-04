-- Run the pipeline once before executing this smoke SQL.

-- Test 1: No self-parenting for PIPELINE_STEP
with last_pipeline as (
  select run_id
  from MIP.APP.MIP_AUDIT_LOG
  where event_type='PIPELINE'
    and event_name='SP_RUN_DAILY_PIPELINE'
    and status='SUCCESS'
  qualify row_number() over (order by event_ts desc)=1
)
select count(*) as bad_rows
from MIP.APP.MIP_AUDIT_LOG a
join last_pipeline p
  on a.parent_run_id = p.run_id
where a.event_type='PIPELINE_STEP'
  and a.run_id = p.run_id;
-- Expect: 0

-- Test 2: No duplicate step rows at same scope
with last_pipeline as (
  select run_id
  from MIP.APP.MIP_AUDIT_LOG
  where event_type='PIPELINE'
    and event_name='SP_RUN_DAILY_PIPELINE'
    and status='SUCCESS'
  qualify row_number() over (order by event_ts desc)=1
),
scoped as (
  select
    a.event_name,
    a.status,
    a.details:step_name::string as step_name,
    coalesce(a.details:scope::string, 'UNKNOWN') as scope,
    coalesce(a.details:scope_key::string, 'NULL') as scope_key,
    count(*) as n
  from MIP.APP.MIP_AUDIT_LOG a
  join last_pipeline p
    on a.parent_run_id = p.run_id
  where a.event_type='PIPELINE_STEP'
  group by 1,2,3,4,5
)
select *
from scoped
where n > 1;
-- Expect: 0 rows

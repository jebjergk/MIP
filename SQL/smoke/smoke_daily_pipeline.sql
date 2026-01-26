-- smoke_daily_pipeline.sql
-- Smoke test: verify pipeline step audit coverage for latest run

set run_id = (
    select RUN_ID
    from MIP.APP.MIP_AUDIT_LOG
    where EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
    order by EVENT_TS desc
    limit 1
);

with expected_steps as (
    select column1 as step_name
    from values
        ('INGESTION'),
        ('RETURNS_REFRESH'),
        ('RECOMMENDATIONS'),
        ('EVALUATION'),
        ('TRUSTED_SIGNAL_REFRESH'),
        ('PORTFOLIO_SIMULATION'),
        ('PROPOSER'),
        ('EXECUTOR'),
        ('MORNING_BRIEF')
),
actual_steps as (
    select distinct EVENT_NAME
    from MIP.APP.MIP_AUDIT_LOG
    where RUN_ID = $run_id
      and EVENT_TYPE = 'PIPELINE_STEP'
)
select e.step_name as missing_step
from expected_steps e
left join actual_steps a
  on a.EVENT_NAME = e.step_name
where a.EVENT_NAME is null
order by e.step_name;

select
    EVENT_NAME,
    STATUS,
    count(*) as entry_count
from MIP.APP.MIP_AUDIT_LOG
where RUN_ID = $run_id
  and EVENT_TYPE = 'PIPELINE_STEP'
  and EVENT_NAME in (
      'INGESTION',
      'RETURNS_REFRESH',
      'RECOMMENDATIONS',
      'EVALUATION',
      'TRUSTED_SIGNAL_REFRESH',
      'PORTFOLIO_SIMULATION',
      'PROPOSER',
      'EXECUTOR',
      'MORNING_BRIEF'
  )
group by EVENT_NAME, STATUS
order by EVENT_NAME, STATUS;

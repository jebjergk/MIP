-- daily_pipeline_audit_smoke.sql
-- Smoke test for pipeline audit coverage

set pipeline_result = (call MIP.APP.SP_RUN_DAILY_PIPELINE());
set run_id = (select :pipeline_result:run_id::string);

select
    event_name,
    status,
    count(*) as entry_count
from MIP.APP.MIP_AUDIT_LOG
where run_id = $run_id
  and event_name in (
      'INGESTION',
      'RETURNS_REFRESH',
      'RECOMMENDATIONS',
      'EVALUATION',
      'PORTFOLIO_SIMULATION',
      'MORNING_BRIEF'
  )
group by event_name, status
order by event_name, status;

select
    event_name,
    details:market_type::string as market_type,
    details:portfolio_id::number as portfolio_id,
    count(*) as entry_count
from MIP.APP.MIP_AUDIT_LOG
where run_id = $run_id
  and event_name in ('RECOMMENDATIONS', 'PORTFOLIO_SIMULATION', 'MORNING_BRIEF')
group by event_name, market_type, portfolio_id
order by event_name, market_type, portfolio_id;

with enabled as (
    select PORTFOLIO_ID
    from MIP.APP.PORTFOLIO
    where STATUS = 'ACTIVE'
),
briefs as (
    select distinct PORTFOLIO_ID
    from MIP.AGENT_OUT.MORNING_BRIEF
    where PIPELINE_RUN_ID = $run_id
)
select
    count(*) as enabled_portfolios,
    count_if(b.PORTFOLIO_ID is not null) as briefs_written,
    count_if(b.PORTFOLIO_ID is null) as briefs_missing
from enabled e
left join briefs b
  on b.PORTFOLIO_ID = e.PORTFOLIO_ID;

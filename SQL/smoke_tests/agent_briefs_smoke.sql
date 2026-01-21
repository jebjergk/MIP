-- agent_briefs_smoke.sql
-- Smoke tests for agent brief views

with latest_run as (
    select
        portfolio_id,
        max(run_id) as run_id
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
    where portfolio_id = 1
    group by portfolio_id
)
select *
from MIP.MART.V_AGENT_SIGNAL_HEALTH_BRIEF
limit 10;

select b.*
from MIP.MART.V_AGENT_PORTFOLIO_RISK_BRIEF b
join latest_run r
  on r.portfolio_id = b.portfolio_id
 and r.run_id = b.run_id
where b.portfolio_id = 1;

select b.*
from MIP.MART.V_AGENT_ATTRIBUTION_BRIEF b
join latest_run r
  on r.portfolio_id = b.portfolio_id
 and r.run_id = b.run_id
where b.portfolio_id = 1;

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

select
    pattern_id,
    market_type,
    interval_minutes,
    horizon_bars,
    trust_label,
    recommended_action,
    reason,
    as_of_ts
from MIP.MART.V_TRUSTED_SIGNAL_POLICY
where trust_label = 'TRUSTED'
order by reason:avg_return desc
limit 20;

select
    pattern_id,
    market_type,
    interval_minutes,
    horizon_bars,
    trust_label,
    recommended_action,
    reason,
    as_of_ts
from MIP.MART.V_TRUSTED_SIGNAL_POLICY
where trust_label = 'WATCH'
order by reason:avg_return asc
limit 20;

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

select *
from MIP.MART.V_AGENT_DAILY_SIGNAL_BRIEF
order by AS_OF_TS desc
limit 20;

select *
from MIP.MART.V_AGENT_DAILY_RISK_BRIEF
order by AS_OF_TS desc
limit 20;

select *
from MIP.MART.V_AGENT_DAILY_ATTRIBUTION_BRIEF
order by AS_OF_TS desc
limit 20;

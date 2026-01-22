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

select top_contributors
from MIP.MART.V_AGENT_DAILY_ATTRIBUTION_BRIEF
where market_type = 'FX'
limit 1;

with base_min as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        MARKET_TYPE,
        SYMBOL,
        TOTAL_REALIZED_PNL
    from MIP.MART.V_PORTFOLIO_ATTRIBUTION
    qualify row_number() over (
        partition by PORTFOLIO_ID, RUN_ID, MARKET_TYPE
        order by TOTAL_REALIZED_PNL asc
    ) = 1
),
detractor_symbols as (
    select
        b.PORTFOLIO_ID,
        b.RUN_ID,
        b.MARKET_TYPE,
        f.value:symbol::string as SYMBOL
    from MIP.MART.V_AGENT_DAILY_ATTRIBUTION_BRIEF b,
    lateral flatten(input => b.TOP_DETRACTORS) f
)
select
    m.PORTFOLIO_ID,
    m.RUN_ID,
    m.MARKET_TYPE,
    m.SYMBOL as EXPECTED_MIN_SYMBOL,
    m.TOTAL_REALIZED_PNL as EXPECTED_MIN_PNL
from base_min m
left join detractor_symbols d
  on d.PORTFOLIO_ID = m.PORTFOLIO_ID
 and d.RUN_ID = m.RUN_ID
 and d.MARKET_TYPE = m.MARKET_TYPE
 and d.SYMBOL = m.SYMBOL
where d.SYMBOL is null;

-- agents_input_views_smoke.sql
-- Smoke tests for agent input views

select *
from MIP.MART.V_PORTFOLIO_RUN_KPIS
limit 10;

select *
from MIP.MART.V_PORTFOLIO_RUN_EVENTS
limit 10;

select *
from MIP.MART.V_PORTFOLIO_ATTRIBUTION
limit 10;

select *
from MIP.MART.V_PORTFOLIO_ATTRIBUTION_BY_PATTERN
limit 10;

-- Top degraders based on total_return shift
with run_returns as (
    select
        portfolio_id,
        run_id,
        to_ts,
        total_return,
        lag(total_return) over (
            partition by portfolio_id
            order by to_ts
        ) as prior_total_return
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
)
select
    portfolio_id,
    run_id,
    to_ts,
    total_return,
    total_return - prior_total_return as delta_total_return
from run_returns
where prior_total_return is not null
order by delta_total_return asc
limit 10;

-- Top degraders based on avg_daily_return shift
with run_returns as (
    select
        portfolio_id,
        run_id,
        to_ts,
        avg_daily_return,
        lag(avg_daily_return) over (
            partition by portfolio_id
            order by to_ts
        ) as prior_avg_daily_return
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
)
select
    portfolio_id,
    run_id,
    to_ts,
    avg_daily_return,
    avg_daily_return - prior_avg_daily_return as delta_avg_daily_return
from run_returns
where prior_avg_daily_return is not null
order by delta_avg_daily_return asc
limit 10;

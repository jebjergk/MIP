-- kpi_views_smoke.sql
-- Smoke tests for KPI views

-- Latest run KPIs for portfolio_id=1
select *
from MIP.MART.V_PORTFOLIO_RUN_KPIS
where PORTFOLIO_ID = 1
order by TO_TS desc
limit 1;

-- Attribution totals vs run PnL (final equity minus starting cash)
select
    k.PORTFOLIO_ID,
    k.RUN_ID,
    sum(a.TOTAL_REALIZED_PNL) as ATTRIBUTION_PNL,
    k.FINAL_EQUITY - k.STARTING_CASH as RUN_PNL,
    (sum(a.TOTAL_REALIZED_PNL) - (k.FINAL_EQUITY - k.STARTING_CASH)) as PNL_DIFF
from MIP.MART.V_PORTFOLIO_RUN_KPIS k
left join MIP.MART.V_PORTFOLIO_ATTRIBUTION a
  on a.PORTFOLIO_ID = k.PORTFOLIO_ID
 and a.RUN_ID = k.RUN_ID
where k.PORTFOLIO_ID = 1
group by
    k.PORTFOLIO_ID,
    k.RUN_ID,
    k.FINAL_EQUITY,
    k.STARTING_CASH
order by k.TO_TS desc;

-- Top 10 best signal buckets by avg_return (minimum sample size)
select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    N_SUCCESS,
    AVG_RETURN
from MIP.MART.V_SIGNAL_OUTCOME_KPIS
where N_SUCCESS >= 20
order by AVG_RETURN desc
limit 10;

-- Score calibration monotonicity table
select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    SCORE_DECILE,
    AVG_RETURN,
    N
from MIP.MART.V_SCORE_CALIBRATION
order by
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    SCORE_DECILE;

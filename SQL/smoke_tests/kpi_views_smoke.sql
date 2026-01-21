-- kpi_views_smoke.sql
-- Smoke tests for KPI views

-- Latest run KPIs for portfolio_id=1
select *
from MIP.MART.V_PORTFOLIO_RUN_KPIS
where PORTFOLIO_ID = 1
order by TO_TS desc
limit 1;

-- Deduped trading days vs distinct timestamps and KPI daily stats for portfolio_id=1
with base_counts as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        count(*) as ROWS_TOTAL,
        count(distinct TS) as DISTINCT_TS
    from MIP.APP.PORTFOLIO_DAILY
    where PORTFOLIO_ID = 1
    group by
        PORTFOLIO_ID,
        RUN_ID
),
kpis as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        TRADING_DAYS,
        DAILY_VOLATILITY,
        AVG_DAILY_RETURN,
        AVG_EQ_RETURN,
        MAX_MARKET_RETURN_RAW,
        MAX_MARKET_RETURN
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
    where PORTFOLIO_ID = 1
)
select
    b.PORTFOLIO_ID,
    b.RUN_ID,
    b.ROWS_TOTAL,
    b.DISTINCT_TS,
    k.TRADING_DAYS,
    k.DAILY_VOLATILITY,
    k.AVG_DAILY_RETURN as AVG_MARKET_RETURN,
    k.AVG_EQ_RETURN,
    k.MAX_MARKET_RETURN_RAW,
    k.MAX_MARKET_RETURN
from base_counts b
left join kpis k
  on k.PORTFOLIO_ID = b.PORTFOLIO_ID
 and k.RUN_ID = b.RUN_ID
order by b.RUN_ID desc;

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
from MIP.MART.V_TRUST_METRICS
order by AVG_RETURN desc
limit 10;

-- Score calibration monotonicity table (single bucket)
with bucket as (
    select
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS
    from MIP.MART.V_TRUST_METRICS
    order by AVG_RETURN desc
    limit 1
)
select
    c.PATTERN_ID,
    c.MARKET_TYPE,
    c.INTERVAL_MINUTES,
    c.HORIZON_BARS,
    c.SCORE_DECILE,
    c.AVG_RETURN,
    c.N
from MIP.MART.V_SCORE_CALIBRATION c
join bucket b
  on b.PATTERN_ID = c.PATTERN_ID
 and b.MARKET_TYPE = c.MARKET_TYPE
 and b.INTERVAL_MINUTES = c.INTERVAL_MINUTES
 and b.HORIZON_BARS = c.HORIZON_BARS
order by
    c.PATTERN_ID,
    c.MARKET_TYPE,
    c.INTERVAL_MINUTES,
    c.HORIZON_BARS,
    c.SCORE_DECILE;

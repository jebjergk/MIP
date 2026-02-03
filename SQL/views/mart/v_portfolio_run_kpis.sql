-- v_portfolio_run_kpis.sql
-- Purpose: Run-level portfolio KPIs by (portfolio_id, run_id)

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_RUN_KPIS as
with daily_dedup as (
    select d.*
    from MIP.APP.PORTFOLIO_DAILY d
    left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e
      on e.PORTFOLIO_ID = d.PORTFOLIO_ID
    where e.EPISODE_ID is null or d.TS >= e.START_TS
    qualify row_number() over (
        partition by d.PORTFOLIO_ID, d.RUN_ID, d.TS
        order by d.CREATED_AT desc, d.TS desc
    ) = 1
),
daily_base as (
    select
        d.PORTFOLIO_ID,
        d.RUN_ID,
        d.TS,
        d.CASH,
        d.EQUITY_VALUE,
        d.TOTAL_EQUITY,
        d.OPEN_POSITIONS,
        d.PEAK_EQUITY,
        d.DRAWDOWN,
        p.STARTING_CASH,
        coalesce(prof.DRAWDOWN_STOP_PCT, 0.10) as DRAWDOWN_STOP_PCT,
        lag(d.TOTAL_EQUITY) over (
            partition by d.PORTFOLIO_ID, d.RUN_ID
            order by d.TS
        ) as PREV_TOTAL_EQUITY,
        lag(d.EQUITY_VALUE) over (
            partition by d.PORTFOLIO_ID, d.RUN_ID
            order by d.TS
        ) as PREV_EQUITY_VALUE,
        lag(d.CASH) over (
            partition by d.PORTFOLIO_ID, d.RUN_ID
            order by d.TS
        ) as PREV_CASH
    from daily_dedup d
    join MIP.APP.PORTFOLIO p
      on p.PORTFOLIO_ID = d.PORTFOLIO_ID
    left join MIP.APP.PORTFOLIO_PROFILE prof
      on prof.PROFILE_ID = p.PROFILE_ID
),
daily_calc as (
    select
        *,
        case
            when PREV_TOTAL_EQUITY is null then null
            else (TOTAL_EQUITY / nullif(PREV_TOTAL_EQUITY, 0)) - 1
        end as EQUITY_RETURN,
        case
            when PREV_TOTAL_EQUITY is null then null
            else TOTAL_EQUITY - PREV_TOTAL_EQUITY
        end as EQUITY_PNL,
        case
            when PREV_TOTAL_EQUITY is null then null
            else (EQUITY_VALUE - PREV_EQUITY_VALUE) / nullif(PREV_TOTAL_EQUITY, 0)
        end as MARKET_RETURN,
        case
            when PREV_EQUITY_VALUE is not null
             and PREV_EQUITY_VALUE <> 0
            then (EQUITY_VALUE - PREV_EQUITY_VALUE) / nullif(PREV_TOTAL_EQUITY, 0)
            else null
        end as MARKET_RETURN_KPI,
        case
            when PREV_TOTAL_EQUITY is null then null
            else (CASH - PREV_CASH) / nullif(PREV_TOTAL_EQUITY, 0)
        end as CAPITAL_FLOW_RETURN,
        case
            when PREV_TOTAL_EQUITY is null then null
            else EQUITY_VALUE - PREV_EQUITY_VALUE
        end as MARKET_PNL,
        case
            when PREV_TOTAL_EQUITY is null then null
            else ((EQUITY_VALUE - PREV_EQUITY_VALUE) / nullif(PREV_TOTAL_EQUITY, 0))
                 + ((CASH - PREV_CASH) / nullif(PREV_TOTAL_EQUITY, 0))
        end as TOTAL_RETURN_RECON
    from daily_base
),
agg as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        min(TS) as FROM_TS,
        max(TS) as TO_TS,
        count(*) as TRADING_DAYS,
        max(STARTING_CASH) as STARTING_CASH,
        max_by(TOTAL_EQUITY, TS) as FINAL_EQUITY,
        max(DRAWDOWN) as MAX_DRAWDOWN,
        max(PEAK_EQUITY) as PEAK_EQUITY,
        min(TOTAL_EQUITY) as MIN_EQUITY,
        stddev_samp(MARKET_RETURN_KPI) as DAILY_VOLATILITY,
        avg(MARKET_RETURN_KPI) as AVG_DAILY_RETURN,
        avg(MARKET_RETURN_KPI) as AVG_MARKET_RETURN,
        avg(EQUITY_RETURN) as AVG_EQ_RETURN,
        max(MARKET_RETURN) as MAX_MARKET_RETURN_RAW,
        max(MARKET_RETURN_KPI) as MAX_MARKET_RETURN,
        avg(EQUITY_RETURN - TOTAL_RETURN_RECON) as AVG_RETURN_RECON_DIFF,
        count_if(MARKET_RETURN > 0) as WIN_DAYS,
        count_if(MARKET_RETURN < 0) as LOSS_DAYS,
        avg(case when MARKET_RETURN > 0 then MARKET_PNL end) as AVG_WIN_PNL,
        avg(case when MARKET_RETURN < 0 then MARKET_PNL end) as AVG_LOSS_PNL,
        avg(OPEN_POSITIONS) as AVG_OPEN_POSITIONS,
        count_if(OPEN_POSITIONS > 0) / nullif(count(*), 0) as TIME_IN_MARKET,
        min(case when DRAWDOWN >= DRAWDOWN_STOP_PCT then TS end) as DRAWDOWN_STOP_TS
    from daily_calc
    group by
        PORTFOLIO_ID,
        RUN_ID
)
select
    PORTFOLIO_ID,
    RUN_ID,
    FROM_TS,
    TO_TS,
    TRADING_DAYS,
    STARTING_CASH,
    FINAL_EQUITY,
    (FINAL_EQUITY / nullif(STARTING_CASH, 0) - 1) as TOTAL_RETURN,
    MAX_DRAWDOWN,
    PEAK_EQUITY,
    MIN_EQUITY,
    DAILY_VOLATILITY,
    AVG_DAILY_RETURN,
    AVG_MARKET_RETURN as AVG_MARKET_RETURN,
    AVG_EQ_RETURN,
    MAX_MARKET_RETURN_RAW,
    MAX_MARKET_RETURN,
    AVG_RETURN_RECON_DIFF,
    WIN_DAYS,
    LOSS_DAYS,
    AVG_WIN_PNL,
    AVG_LOSS_PNL,
    AVG_OPEN_POSITIONS,
    TIME_IN_MARKET,
    DRAWDOWN_STOP_TS
from agg;

-- v_portfolio_run_kpis.sql
-- Purpose: Run-level portfolio KPIs by (portfolio_id, run_id)

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_RUN_KPIS as
with daily_dedup as (
    select *
    from MIP.APP.PORTFOLIO_DAILY
    qualify row_number() over (
        partition by PORTFOLIO_ID, RUN_ID, TS
        order by CREATED_AT desc, TS desc
    ) = 1
),
daily_calc as (
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
        case
            when lag(d.TOTAL_EQUITY) over (
                partition by d.PORTFOLIO_ID, d.RUN_ID
                order by d.TS
            ) is null then null
            else (d.TOTAL_EQUITY / nullif(lag(d.TOTAL_EQUITY) over (
                partition by d.PORTFOLIO_ID, d.RUN_ID
                order by d.TS
            ), 0)) - 1
        end as EQUITY_RETURN,
        case
            when lag(d.TOTAL_EQUITY) over (
                partition by d.PORTFOLIO_ID, d.RUN_ID
                order by d.TS
            ) is null then null
            else d.TOTAL_EQUITY - lag(d.TOTAL_EQUITY) over (
                partition by d.PORTFOLIO_ID, d.RUN_ID
                order by d.TS
            )
        end as EQUITY_PNL
    from daily_dedup d
    join MIP.APP.PORTFOLIO p
      on p.PORTFOLIO_ID = d.PORTFOLIO_ID
    left join MIP.APP.PORTFOLIO_PROFILE prof
      on prof.PROFILE_ID = p.PROFILE_ID
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
        stddev_samp(EQUITY_RETURN) as DAILY_VOLATILITY,
        avg(EQUITY_RETURN) as AVG_DAILY_RETURN,
        count_if(EQUITY_PNL > 0) as WIN_DAYS,
        count_if(EQUITY_PNL < 0) as LOSS_DAYS,
        avg(case when EQUITY_PNL > 0 then EQUITY_PNL end) as AVG_WIN_PNL,
        avg(case when EQUITY_PNL < 0 then EQUITY_PNL end) as AVG_LOSS_PNL,
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
    WIN_DAYS,
    LOSS_DAYS,
    AVG_WIN_PNL,
    AVG_LOSS_PNL,
    AVG_OPEN_POSITIONS,
    TIME_IN_MARKET,
    DRAWDOWN_STOP_TS
from agg;

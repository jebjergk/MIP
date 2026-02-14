-- v_parallel_world_regret.sql
-- Purpose: Computes rolling regret per scenario — how much better the scenario
-- consistently performs vs actual. Regret = max(scenario_pnl - actual_pnl, 0).
-- Rolling 20-day cumulative regret identifies scenarios that *persistently* outperform,
-- powering the "regret heatmap" in the UI and informing future agent reasoning.
--
-- Sources:
--   MIP.MART.V_PARALLEL_WORLD_DIFF  — per-scenario deltas

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PARALLEL_WORLD_REGRET (
    PORTFOLIO_ID,
    AS_OF_TS,
    SCENARIO_ID,
    SCENARIO_NAME,
    SCENARIO_TYPE,
    -- Daily metrics
    PNL_DELTA,
    DAILY_REGRET,
    -- Rolling metrics (20-day window)
    ROLLING_REGRET_20D,
    ROLLING_AVG_DELTA_20D,
    ROLLING_OUTPERFORM_COUNT_20D,
    ROLLING_DAYS_20D,
    -- Lifetime metrics
    CUMULATIVE_REGRET,
    CUMULATIVE_DELTA,
    TOTAL_DAYS,
    OUTPERFORM_PCT
) as
with daily_regret as (
    select
        PORTFOLIO_ID,
        AS_OF_TS,
        SCENARIO_ID,
        SCENARIO_NAME,
        SCENARIO_TYPE,
        PNL_DELTA,
        greatest(PNL_DELTA, 0) as DAILY_REGRET
    from MIP.MART.V_PARALLEL_WORLD_DIFF
)
select
    PORTFOLIO_ID,
    AS_OF_TS,
    SCENARIO_ID,
    SCENARIO_NAME,
    SCENARIO_TYPE,
    PNL_DELTA,
    DAILY_REGRET,
    -- Rolling 20-day regret
    sum(DAILY_REGRET) over (
        partition by PORTFOLIO_ID, SCENARIO_ID
        order by AS_OF_TS
        rows between 19 preceding and current row
    ) as ROLLING_REGRET_20D,
    avg(PNL_DELTA) over (
        partition by PORTFOLIO_ID, SCENARIO_ID
        order by AS_OF_TS
        rows between 19 preceding and current row
    ) as ROLLING_AVG_DELTA_20D,
    sum(case when PNL_DELTA > 0 then 1 else 0 end) over (
        partition by PORTFOLIO_ID, SCENARIO_ID
        order by AS_OF_TS
        rows between 19 preceding and current row
    ) as ROLLING_OUTPERFORM_COUNT_20D,
    count(*) over (
        partition by PORTFOLIO_ID, SCENARIO_ID
        order by AS_OF_TS
        rows between 19 preceding and current row
    ) as ROLLING_DAYS_20D,
    -- Lifetime cumulative
    sum(DAILY_REGRET) over (
        partition by PORTFOLIO_ID, SCENARIO_ID
        order by AS_OF_TS
    ) as CUMULATIVE_REGRET,
    sum(PNL_DELTA) over (
        partition by PORTFOLIO_ID, SCENARIO_ID
        order by AS_OF_TS
    ) as CUMULATIVE_DELTA,
    row_number() over (
        partition by PORTFOLIO_ID, SCENARIO_ID
        order by AS_OF_TS
    ) as TOTAL_DAYS,
    round(
        sum(case when PNL_DELTA > 0 then 1.0 else 0.0 end) over (
            partition by PORTFOLIO_ID, SCENARIO_ID
            order by AS_OF_TS
        ) / nullif(row_number() over (
            partition by PORTFOLIO_ID, SCENARIO_ID
            order by AS_OF_TS
        ), 0) * 100, 1
    ) as OUTPERFORM_PCT
from daily_regret;

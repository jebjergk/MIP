-- v_pw_tuning_surface.sql
-- Purpose: Aggregates sweep simulation results into a "tuning surface" —
-- per-parameter-point performance metrics for charting.
-- Identifies optimal point and minimal safe tweak per family/portfolio.
--
-- Sources:
--   MIP.APP.PARALLEL_WORLD_RESULT     — raw simulation outcomes
--   MIP.APP.PARALLEL_WORLD_SCENARIO   — scenario definitions (sweep metadata)
--   MIP.MART.V_PARALLEL_WORLD_DIFF    — actual vs counterfactual deltas

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PW_TUNING_SURFACE (
    PORTFOLIO_ID,
    SWEEP_FAMILY,
    SCENARIO_ID,
    SCENARIO_NAME,
    DISPLAY_NAME,
    SWEEP_ORDER,
    PARAM_VALUE,
    -- Aggregate performance
    OBSERVATION_DAYS,
    TOTAL_PNL_DELTA,
    AVG_DAILY_PNL_DELTA,
    WIN_RATE_PCT,
    WIN_DAYS,
    LOSE_DAYS,
    MAX_DAILY_GAIN,
    MAX_DAILY_LOSS,
    AVG_TRADES_DELTA,
    -- Classification
    IS_OPTIMAL,
    IS_MINIMAL_SAFE_TWEAK,
    IS_CURRENT_SETTING
) as
with sweep_diffs as (
    -- Get daily deltas for sweep scenarios
    select
        d.PORTFOLIO_ID,
        s.SWEEP_FAMILY,
        s.SCENARIO_ID,
        s.NAME as SCENARIO_NAME,
        s.DISPLAY_NAME,
        s.SWEEP_ORDER,
        -- Extract the primary parameter value from PARAMS_JSON
        case s.SWEEP_FAMILY
            when 'ZSCORE_SWEEP'  then s.PARAMS_JSON:min_zscore_delta::number(18,4)
            when 'RETURN_SWEEP'  then s.PARAMS_JSON:min_return_delta::number(18,6)
            when 'SIZING_SWEEP'  then s.PARAMS_JSON:position_pct_multiplier::number(18,4)
            when 'TIMING_SWEEP'  then s.PARAMS_JSON:entry_delay_bars::number
        end as PARAM_VALUE,
        d.AS_OF_TS,
        d.PNL_DELTA,
        d.TRADES_DELTA
    from MIP.MART.V_PARALLEL_WORLD_DIFF d
    join MIP.APP.PARALLEL_WORLD_SCENARIO s
      on s.SCENARIO_ID = d.SCENARIO_ID
    where s.IS_SWEEP = true
      and s.IS_ACTIVE = true
      and s.SWEEP_FAMILY is not null
),
aggregated as (
    select
        PORTFOLIO_ID,
        SWEEP_FAMILY,
        SCENARIO_ID,
        SCENARIO_NAME,
        DISPLAY_NAME,
        SWEEP_ORDER,
        PARAM_VALUE,
        count(*) as OBSERVATION_DAYS,
        round(sum(PNL_DELTA), 2) as TOTAL_PNL_DELTA,
        round(avg(PNL_DELTA), 4) as AVG_DAILY_PNL_DELTA,
        round(
            sum(case when PNL_DELTA > 0 then 1.0 else 0.0 end)
            / nullif(count(*), 0) * 100, 1
        ) as WIN_RATE_PCT,
        sum(case when PNL_DELTA > 0 then 1 else 0 end) as WIN_DAYS,
        sum(case when PNL_DELTA < 0 then 1 else 0 end) as LOSE_DAYS,
        round(max(PNL_DELTA), 2) as MAX_DAILY_GAIN,
        round(min(PNL_DELTA), 2) as MAX_DAILY_LOSS,
        round(avg(TRADES_DELTA), 2) as AVG_TRADES_DELTA
    from sweep_diffs
    group by PORTFOLIO_ID, SWEEP_FAMILY, SCENARIO_ID, SCENARIO_NAME,
             DISPLAY_NAME, SWEEP_ORDER, PARAM_VALUE
),
ranked as (
    select
        a.*,
        -- Is this the zero/neutral point (current setting)?
        case
            when SWEEP_FAMILY in ('ZSCORE_SWEEP', 'RETURN_SWEEP') and PARAM_VALUE = 0 then true
            when SWEEP_FAMILY = 'SIZING_SWEEP' and PARAM_VALUE = 1.0 then true
            when SWEEP_FAMILY = 'TIMING_SWEEP' and PARAM_VALUE = 0 then true
            else false
        end as IS_CURRENT_SETTING,
        -- Best cumulative delta per family (optimal point)
        row_number() over (
            partition by PORTFOLIO_ID, SWEEP_FAMILY
            order by TOTAL_PNL_DELTA desc
        ) as rn_best,
        -- Minimal safe tweak: smallest absolute distance from current that improves
        row_number() over (
            partition by PORTFOLIO_ID, SWEEP_FAMILY
            order by
                case
                    when TOTAL_PNL_DELTA > 0
                     and abs(PARAM_VALUE - case
                            when SWEEP_FAMILY in ('ZSCORE_SWEEP', 'RETURN_SWEEP') then 0
                            when SWEEP_FAMILY = 'SIZING_SWEEP' then 1.0
                            when SWEEP_FAMILY = 'TIMING_SWEEP' then 0
                        end) > 0
                    then abs(PARAM_VALUE - case
                            when SWEEP_FAMILY in ('ZSCORE_SWEEP', 'RETURN_SWEEP') then 0
                            when SWEEP_FAMILY = 'SIZING_SWEEP' then 1.0
                            when SWEEP_FAMILY = 'TIMING_SWEEP' then 0
                        end)
                    else 999
                end asc
        ) as rn_safe
    from aggregated a
)
select
    PORTFOLIO_ID,
    SWEEP_FAMILY,
    SCENARIO_ID,
    SCENARIO_NAME,
    DISPLAY_NAME,
    SWEEP_ORDER,
    PARAM_VALUE,
    OBSERVATION_DAYS,
    TOTAL_PNL_DELTA,
    AVG_DAILY_PNL_DELTA,
    WIN_RATE_PCT,
    WIN_DAYS,
    LOSE_DAYS,
    MAX_DAILY_GAIN,
    MAX_DAILY_LOSS,
    AVG_TRADES_DELTA,
    (rn_best = 1 and TOTAL_PNL_DELTA > 0) as IS_OPTIMAL,
    (rn_safe = 1 and TOTAL_PNL_DELTA > 0 and not IS_CURRENT_SETTING) as IS_MINIMAL_SAFE_TWEAK,
    IS_CURRENT_SETTING
from ranked;

-- v_pw_regime_sensitivity.sql
-- Purpose: Tags each day with a volatility regime (QUIET / NORMAL / VOLATILE)
-- based on rolling portfolio PnL standard deviation, then computes per-regime
-- performance for each sweep parameter point.
-- Flags scenarios that only work in one regime ("regime fragile").
--
-- Sources:
--   MIP.APP.PORTFOLIO_DAILY           — daily portfolio metrics (for volatility)
--   MIP.APP.PARALLEL_WORLD_RESULT     — sweep simulation results
--   MIP.APP.PARALLEL_WORLD_SCENARIO   — scenario definitions
--   MIP.MART.V_PARALLEL_WORLD_DIFF    — actual vs counterfactual deltas

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PW_REGIME_SENSITIVITY (
    PORTFOLIO_ID,
    AS_OF_TS,
    REGIME,
    SWEEP_FAMILY,
    SCENARIO_ID,
    DISPLAY_NAME,
    PARAM_VALUE,
    -- Per-regime metrics
    REGIME_DAYS,
    REGIME_PNL_DELTA,
    REGIME_AVG_DELTA,
    REGIME_WIN_RATE_PCT,
    -- Cross-regime summary (latest per scenario)
    TOTAL_DAYS,
    QUIET_WIN_PCT,
    NORMAL_WIN_PCT,
    VOLATILE_WIN_PCT,
    IS_REGIME_FRAGILE
) as
with daily_vol as (
    -- Compute rolling 10-day stddev of daily PnL per portfolio
    select
        PORTFOLIO_ID,
        TS as AS_OF_TS,
        DAILY_PNL,
        stddev(DAILY_PNL) over (
            partition by PORTFOLIO_ID
            order by TS
            rows between 9 preceding and current row
        ) as ROLLING_VOL
    from MIP.APP.PORTFOLIO_DAILY
),
regime_tagged as (
    -- Tag each day with a regime based on percentile of rolling volatility
    select
        PORTFOLIO_ID,
        AS_OF_TS,
        ROLLING_VOL,
        case
            when ROLLING_VOL is null then 'NORMAL'
            when ntile(3) over (partition by PORTFOLIO_ID order by ROLLING_VOL) = 1 then 'QUIET'
            when ntile(3) over (partition by PORTFOLIO_ID order by ROLLING_VOL) = 2 then 'NORMAL'
            else 'VOLATILE'
        end as REGIME
    from daily_vol
),
sweep_daily as (
    select
        d.PORTFOLIO_ID,
        d.AS_OF_TS,
        r.REGIME,
        s.SWEEP_FAMILY,
        s.SCENARIO_ID,
        s.DISPLAY_NAME,
        case s.SWEEP_FAMILY
            when 'ZSCORE_SWEEP'  then s.PARAMS_JSON:min_zscore_delta::number(18,4)
            when 'RETURN_SWEEP'  then s.PARAMS_JSON:min_return_delta::number(18,6)
            when 'SIZING_SWEEP'  then s.PARAMS_JSON:position_pct_multiplier::number(18,4)
            when 'TIMING_SWEEP'  then s.PARAMS_JSON:entry_delay_bars::number
        end as PARAM_VALUE,
        d.PNL_DELTA
    from MIP.MART.V_PARALLEL_WORLD_DIFF d
    join MIP.APP.PARALLEL_WORLD_SCENARIO s on s.SCENARIO_ID = d.SCENARIO_ID
    join regime_tagged r on r.PORTFOLIO_ID = d.PORTFOLIO_ID and r.AS_OF_TS::date = d.AS_OF_TS::date
    where s.IS_SWEEP = true and s.IS_ACTIVE = true
),
regime_agg as (
    select
        PORTFOLIO_ID,
        REGIME,
        SWEEP_FAMILY,
        SCENARIO_ID,
        DISPLAY_NAME,
        PARAM_VALUE,
        count(*) as REGIME_DAYS,
        round(sum(PNL_DELTA), 2) as REGIME_PNL_DELTA,
        round(avg(PNL_DELTA), 4) as REGIME_AVG_DELTA,
        round(
            sum(case when PNL_DELTA > 0 then 1.0 else 0.0 end)
            / nullif(count(*), 0) * 100, 1
        ) as REGIME_WIN_RATE_PCT
    from sweep_daily
    group by PORTFOLIO_ID, REGIME, SWEEP_FAMILY, SCENARIO_ID, DISPLAY_NAME, PARAM_VALUE
),
cross_regime as (
    -- Compute per-scenario cross-regime summary
    select
        PORTFOLIO_ID,
        SWEEP_FAMILY,
        SCENARIO_ID,
        sum(REGIME_DAYS) as TOTAL_DAYS,
        max(case when REGIME = 'QUIET' then REGIME_WIN_RATE_PCT end) as QUIET_WIN_PCT,
        max(case when REGIME = 'NORMAL' then REGIME_WIN_RATE_PCT end) as NORMAL_WIN_PCT,
        max(case when REGIME = 'VOLATILE' then REGIME_WIN_RATE_PCT end) as VOLATILE_WIN_PCT,
        -- Fragile if only wins in one regime (>50% win rate in only one bucket)
        case
            when (
                (coalesce(max(case when REGIME = 'QUIET' then REGIME_WIN_RATE_PCT end), 0) > 50)::int +
                (coalesce(max(case when REGIME = 'NORMAL' then REGIME_WIN_RATE_PCT end), 0) > 50)::int +
                (coalesce(max(case when REGIME = 'VOLATILE' then REGIME_WIN_RATE_PCT end), 0) > 50)::int
            ) = 1 then true
            else false
        end as IS_REGIME_FRAGILE
    from regime_agg
    group by PORTFOLIO_ID, SWEEP_FAMILY, SCENARIO_ID
)
select
    ra.PORTFOLIO_ID,
    null::timestamp_ntz as AS_OF_TS,
    ra.REGIME,
    ra.SWEEP_FAMILY,
    ra.SCENARIO_ID,
    ra.DISPLAY_NAME,
    ra.PARAM_VALUE,
    ra.REGIME_DAYS,
    ra.REGIME_PNL_DELTA,
    ra.REGIME_AVG_DELTA,
    ra.REGIME_WIN_RATE_PCT,
    cr.TOTAL_DAYS,
    cr.QUIET_WIN_PCT,
    cr.NORMAL_WIN_PCT,
    cr.VOLATILE_WIN_PCT,
    cr.IS_REGIME_FRAGILE
from regime_agg ra
join cross_regime cr
  on ra.PORTFOLIO_ID = cr.PORTFOLIO_ID
  and ra.SWEEP_FAMILY = cr.SWEEP_FAMILY
  and ra.SCENARIO_ID = cr.SCENARIO_ID;

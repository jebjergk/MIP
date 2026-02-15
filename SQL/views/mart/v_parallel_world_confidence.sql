-- v_parallel_world_confidence.sql
-- Purpose: Classifies each scenario's signal strength as NOISE / WEAK / EMERGING / STRONG
-- based on rolling consistency, win-rate, magnitude, and persistence.
-- This is deterministic (no LLM) and feeds both the UI and the future meta-agent.
--
-- Sources:
--   MIP.MART.V_PARALLEL_WORLD_REGRET  — rolling and cumulative metrics per scenario

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PARALLEL_WORLD_CONFIDENCE (
    PORTFOLIO_ID,
    AS_OF_TS,
    SCENARIO_ID,
    SCENARIO_NAME,
    SCENARIO_DISPLAY_NAME,
    SCENARIO_TYPE,
    -- Source metrics
    TOTAL_DAYS,
    OUTPERFORM_PCT,
    CUMULATIVE_DELTA,
    CUMULATIVE_REGRET,
    ROLLING_AVG_DELTA_20D,
    ROLLING_OUTPERFORM_COUNT_20D,
    ROLLING_DAYS_20D,
    -- Classification
    CONFIDENCE_CLASS,
    CONFIDENCE_REASON,
    -- Recommendation strength (for UI labels)
    RECOMMENDATION_STRENGTH
) as
with latest as (
    -- Latest row per (portfolio, scenario) — most recent day's metrics
    select *
    from MIP.MART.V_PARALLEL_WORLD_REGRET
    qualify row_number() over (
        partition by PORTFOLIO_ID, SCENARIO_ID
        order by AS_OF_TS desc
    ) = 1
)
select
    PORTFOLIO_ID,
    AS_OF_TS,
    SCENARIO_ID,
    SCENARIO_NAME,
    SCENARIO_DISPLAY_NAME,
    SCENARIO_TYPE,
    -- Source metrics
    TOTAL_DAYS,
    OUTPERFORM_PCT,
    CUMULATIVE_DELTA,
    CUMULATIVE_REGRET,
    ROLLING_AVG_DELTA_20D,
    ROLLING_OUTPERFORM_COUNT_20D,
    ROLLING_DAYS_20D,
    -- Classification logic
    case
        -- Not enough data to judge
        when TOTAL_DAYS < 3
            then 'NOISE'
        -- Trivial magnitude — even if "winning", it's meaningless
        when abs(CUMULATIVE_DELTA) < 1.0
            then 'NOISE'
        -- Low win-rate: scenario rarely beats actual
        when OUTPERFORM_PCT < 40
            then 'NOISE'
        -- Strong: high win-rate, sufficient history, consistently positive
        when OUTPERFORM_PCT > 70
             and TOTAL_DAYS >= 10
             and CUMULATIVE_DELTA > 0
             and ROLLING_AVG_DELTA_20D > 0
            then 'STRONG'
        -- Emerging: decent win-rate, delta trending positive
        when OUTPERFORM_PCT >= 55
             and TOTAL_DAYS >= 5
             and CUMULATIVE_DELTA > 0
             and ROLLING_AVG_DELTA_20D > 0
            then 'EMERGING'
        -- Weak: some signal but not convincing
        when OUTPERFORM_PCT >= 40
             and TOTAL_DAYS >= 3
            then 'WEAK'
        -- Fallback
        else 'NOISE'
    end as CONFIDENCE_CLASS,

    -- Human-readable reason
    case
        when TOTAL_DAYS < 3
            then 'Too few days of data (' || TOTAL_DAYS || ')'
        when abs(CUMULATIVE_DELTA) < 1.0
            then 'Cumulative impact is negligible ($' || round(abs(CUMULATIVE_DELTA), 2) || ')'
        when OUTPERFORM_PCT < 40
            then 'Wins only ' || round(OUTPERFORM_PCT, 0) || '% of the time'
        when OUTPERFORM_PCT > 70 and TOTAL_DAYS >= 10 and CUMULATIVE_DELTA > 0 and ROLLING_AVG_DELTA_20D > 0
            then 'Wins ' || round(OUTPERFORM_PCT, 0) || '% over ' || TOTAL_DAYS || ' days, avg +$' || round(ROLLING_AVG_DELTA_20D, 2) || '/day'
        when OUTPERFORM_PCT >= 55 and TOTAL_DAYS >= 5 and CUMULATIVE_DELTA > 0 and ROLLING_AVG_DELTA_20D > 0
            then 'Wins ' || round(OUTPERFORM_PCT, 0) || '% over ' || TOTAL_DAYS || ' days — emerging pattern'
        when OUTPERFORM_PCT >= 40 and TOTAL_DAYS >= 3
            then 'Wins ' || round(OUTPERFORM_PCT, 0) || '% but not yet consistent'
        else 'Insufficient evidence'
    end as CONFIDENCE_REASON,

    -- Map to recommendation strength for the UI
    case
        when TOTAL_DAYS < 3 or abs(CUMULATIVE_DELTA) < 1.0 or OUTPERFORM_PCT < 40
            then 'NOT_ACTIONABLE'
        when OUTPERFORM_PCT > 70 and TOTAL_DAYS >= 10 and CUMULATIVE_DELTA > 0 and ROLLING_AVG_DELTA_20D > 0
            then 'STRONG_SIGNAL'
        when OUTPERFORM_PCT >= 55 and TOTAL_DAYS >= 5 and CUMULATIVE_DELTA > 0 and ROLLING_AVG_DELTA_20D > 0
            then 'CANDIDATE'
        when OUTPERFORM_PCT >= 40 and TOTAL_DAYS >= 3
            then 'EXPERIMENTAL'
        else 'NOT_ACTIONABLE'
    end as RECOMMENDATION_STRENGTH

from latest;

-- v_parallel_world_regret_attribution.sql
-- Purpose: Attributes cumulative regret to each SCENARIO_TYPE (Timing, Threshold, Sizing, Baseline)
-- and identifies the dominant regret driver per portfolio. Feeds the "what should we improve first?"
-- question for both the UI and future meta-agent.
--
-- Sources:
--   MIP.MART.V_PARALLEL_WORLD_CONFIDENCE  — latest confidence per scenario

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PARALLEL_WORLD_REGRET_ATTRIBUTION (
    PORTFOLIO_ID,
    SCENARIO_TYPE,
    SCENARIO_COUNT,
    -- Aggregated metrics across all scenarios of this type
    AVG_OUTPERFORM_PCT,
    TOTAL_CUMULATIVE_DELTA,
    TOTAL_CUMULATIVE_REGRET,
    AVG_CUMULATIVE_DELTA,
    AVG_ROLLING_AVG_DELTA_20D,
    MAX_CUMULATIVE_DELTA,
    BEST_SCENARIO_NAME,
    BEST_SCENARIO_DISPLAY_NAME,
    BEST_CONFIDENCE_CLASS,
    -- Per-portfolio ranking
    REGRET_RANK,
    IS_DOMINANT_DRIVER,
    TYPE_LABEL
) as
with type_agg as (
    select
        PORTFOLIO_ID,
        SCENARIO_TYPE,
        count(*) as SCENARIO_COUNT,
        round(avg(OUTPERFORM_PCT), 1) as AVG_OUTPERFORM_PCT,
        round(sum(CUMULATIVE_DELTA), 2) as TOTAL_CUMULATIVE_DELTA,
        round(sum(CUMULATIVE_REGRET), 2) as TOTAL_CUMULATIVE_REGRET,
        round(avg(CUMULATIVE_DELTA), 2) as AVG_CUMULATIVE_DELTA,
        round(avg(ROLLING_AVG_DELTA_20D), 2) as AVG_ROLLING_AVG_DELTA_20D,
        round(max(CUMULATIVE_DELTA), 2) as MAX_CUMULATIVE_DELTA
    from MIP.MART.V_PARALLEL_WORLD_CONFIDENCE
    group by PORTFOLIO_ID, SCENARIO_TYPE
),
best_per_type as (
    -- Best scenario within each type (highest cumulative delta)
    select
        PORTFOLIO_ID,
        SCENARIO_TYPE,
        SCENARIO_NAME as BEST_SCENARIO_NAME,
        SCENARIO_DISPLAY_NAME as BEST_SCENARIO_DISPLAY_NAME,
        CONFIDENCE_CLASS as BEST_CONFIDENCE_CLASS
    from MIP.MART.V_PARALLEL_WORLD_CONFIDENCE
    qualify row_number() over (
        partition by PORTFOLIO_ID, SCENARIO_TYPE
        order by CUMULATIVE_DELTA desc
    ) = 1
),
ranked as (
    select
        t.*,
        b.BEST_SCENARIO_NAME,
        b.BEST_SCENARIO_DISPLAY_NAME,
        b.BEST_CONFIDENCE_CLASS,
        row_number() over (
            partition by t.PORTFOLIO_ID
            order by t.TOTAL_CUMULATIVE_REGRET desc
        ) as REGRET_RANK
    from type_agg t
    join best_per_type b
      on t.PORTFOLIO_ID = b.PORTFOLIO_ID
      and t.SCENARIO_TYPE = b.SCENARIO_TYPE
)
select
    PORTFOLIO_ID,
    SCENARIO_TYPE,
    SCENARIO_COUNT,
    AVG_OUTPERFORM_PCT,
    TOTAL_CUMULATIVE_DELTA,
    TOTAL_CUMULATIVE_REGRET,
    AVG_CUMULATIVE_DELTA,
    AVG_ROLLING_AVG_DELTA_20D,
    MAX_CUMULATIVE_DELTA,
    BEST_SCENARIO_NAME,
    BEST_SCENARIO_DISPLAY_NAME,
    BEST_CONFIDENCE_CLASS,
    REGRET_RANK,
    (REGRET_RANK = 1) as IS_DOMINANT_DRIVER,
    case SCENARIO_TYPE
        when 'THRESHOLD' then 'Signal Filter'
        when 'SIZING'    then 'Position Size'
        when 'TIMING'    then 'Entry Timing'
        when 'BASELINE'  then 'Baseline'
        else initcap(SCENARIO_TYPE)
    end as TYPE_LABEL
from ranked;

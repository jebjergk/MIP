-- v_parallel_world_policy_diagnostics.sql
-- Purpose: Combines confidence, regret attribution, and stability into a per-portfolio
-- "Policy Health" summary. Answers: "What should we improve first?" and "How healthy
-- are our current rules?" Feeds the Policy Health card in the UI and the future meta-agent.
--
-- Sources:
--   MIP.MART.V_PARALLEL_WORLD_CONFIDENCE           — per-scenario signal strength
--   MIP.MART.V_PARALLEL_WORLD_REGRET_ATTRIBUTION   — per-type regret ranking

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PARALLEL_WORLD_POLICY_DIAGNOSTICS (
    PORTFOLIO_ID,
    -- Scenario coverage
    TOTAL_SCENARIOS,
    STRONG_SIGNALS,
    EMERGING_SIGNALS,
    WEAK_SIGNALS,
    NOISE_SIGNALS,
    -- Overall policy health
    POLICY_HEALTH,
    POLICY_HEALTH_REASON,
    -- Dominant regret driver
    DOMINANT_DRIVER_TYPE,
    DOMINANT_DRIVER_LABEL,
    DOMINANT_DRIVER_REGRET,
    DOMINANT_DRIVER_BEST_SCENARIO,
    DOMINANT_DRIVER_BEST_CONFIDENCE,
    -- Top recommendation
    TOP_RECOMMENDATION,
    TOP_RECOMMENDATION_TYPE,
    -- Stability score (0-100, higher = more stable / less opportunity for change)
    STABILITY_SCORE,
    STABILITY_LABEL
) as
with scenario_counts as (
    select
        PORTFOLIO_ID,
        count(*) as TOTAL_SCENARIOS,
        count_if(CONFIDENCE_CLASS = 'STRONG') as STRONG_SIGNALS,
        count_if(CONFIDENCE_CLASS = 'EMERGING') as EMERGING_SIGNALS,
        count_if(CONFIDENCE_CLASS = 'WEAK') as WEAK_SIGNALS,
        count_if(CONFIDENCE_CLASS = 'NOISE') as NOISE_SIGNALS
    from MIP.MART.V_PARALLEL_WORLD_CONFIDENCE
    group by PORTFOLIO_ID
),
dominant as (
    select
        PORTFOLIO_ID,
        SCENARIO_TYPE as DOMINANT_DRIVER_TYPE,
        TYPE_LABEL as DOMINANT_DRIVER_LABEL,
        TOTAL_CUMULATIVE_REGRET as DOMINANT_DRIVER_REGRET,
        BEST_SCENARIO_DISPLAY_NAME as DOMINANT_DRIVER_BEST_SCENARIO,
        BEST_CONFIDENCE_CLASS as DOMINANT_DRIVER_BEST_CONFIDENCE
    from MIP.MART.V_PARALLEL_WORLD_REGRET_ATTRIBUTION
    where IS_DOMINANT_DRIVER = true
),
top_rec as (
    -- Best non-NOISE scenario by cumulative delta (highest opportunity)
    select
        PORTFOLIO_ID,
        coalesce(SCENARIO_DISPLAY_NAME, SCENARIO_NAME) as TOP_RECOMMENDATION,
        SCENARIO_TYPE as TOP_RECOMMENDATION_TYPE
    from MIP.MART.V_PARALLEL_WORLD_CONFIDENCE
    where CONFIDENCE_CLASS != 'NOISE'
    qualify row_number() over (
        partition by PORTFOLIO_ID
        order by
            case CONFIDENCE_CLASS
                when 'STRONG' then 1
                when 'EMERGING' then 2
                when 'WEAK' then 3
                else 4
            end,
            CUMULATIVE_DELTA desc
    ) = 1
)
select
    s.PORTFOLIO_ID,
    s.TOTAL_SCENARIOS,
    s.STRONG_SIGNALS,
    s.EMERGING_SIGNALS,
    s.WEAK_SIGNALS,
    s.NOISE_SIGNALS,

    -- Policy Health classification
    case
        when s.STRONG_SIGNALS >= 2
            then 'NEEDS_ATTENTION'
        when s.STRONG_SIGNALS = 1
            then 'REVIEW_SUGGESTED'
        when s.EMERGING_SIGNALS >= 2
            then 'MONITOR'
        when s.EMERGING_SIGNALS = 1 or s.WEAK_SIGNALS >= 3
            then 'WATCH'
        else 'HEALTHY'
    end as POLICY_HEALTH,

    case
        when s.STRONG_SIGNALS >= 2
            then s.STRONG_SIGNALS || ' scenarios consistently outperform your current rules'
        when s.STRONG_SIGNALS = 1
            then '1 scenario shows strong, consistent outperformance'
        when s.EMERGING_SIGNALS >= 2
            then s.EMERGING_SIGNALS || ' scenarios showing emerging patterns worth monitoring'
        when s.EMERGING_SIGNALS = 1 or s.WEAK_SIGNALS >= 3
            then 'Some early signals detected — keep watching'
        else 'No scenarios reliably beat your current approach'
    end as POLICY_HEALTH_REASON,

    -- Dominant regret driver
    d.DOMINANT_DRIVER_TYPE,
    d.DOMINANT_DRIVER_LABEL,
    d.DOMINANT_DRIVER_REGRET,
    d.DOMINANT_DRIVER_BEST_SCENARIO,
    d.DOMINANT_DRIVER_BEST_CONFIDENCE,

    -- Top recommendation
    t.TOP_RECOMMENDATION,
    t.TOP_RECOMMENDATION_TYPE,

    -- Stability Score (0-100)
    -- 100 = all noise (perfectly stable, no improvement opportunities)
    -- 0 = all strong (highly unstable, many improvements available)
    round(
        (s.NOISE_SIGNALS * 100.0 + s.WEAK_SIGNALS * 60.0 + s.EMERGING_SIGNALS * 30.0 + s.STRONG_SIGNALS * 0.0)
        / nullif(s.TOTAL_SCENARIOS, 0),
        0
    ) as STABILITY_SCORE,

    case
        when round(
            (s.NOISE_SIGNALS * 100.0 + s.WEAK_SIGNALS * 60.0 + s.EMERGING_SIGNALS * 30.0 + s.STRONG_SIGNALS * 0.0)
            / nullif(s.TOTAL_SCENARIOS, 0), 0
        ) >= 85 then 'Very Stable'
        when round(
            (s.NOISE_SIGNALS * 100.0 + s.WEAK_SIGNALS * 60.0 + s.EMERGING_SIGNALS * 30.0 + s.STRONG_SIGNALS * 0.0)
            / nullif(s.TOTAL_SCENARIOS, 0), 0
        ) >= 60 then 'Stable'
        when round(
            (s.NOISE_SIGNALS * 100.0 + s.WEAK_SIGNALS * 60.0 + s.EMERGING_SIGNALS * 30.0 + s.STRONG_SIGNALS * 0.0)
            / nullif(s.TOTAL_SCENARIOS, 0), 0
        ) >= 40 then 'Moderate'
        else 'Volatile'
    end as STABILITY_LABEL

from scenario_counts s
left join dominant d on s.PORTFOLIO_ID = d.PORTFOLIO_ID
left join top_rec t on s.PORTFOLIO_ID = t.PORTFOLIO_ID;

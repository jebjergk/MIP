-- v_proposal_pw_portfolio_context.sql
-- Portfolio-level Parallel Worlds rollup for proposal-time soft scoring (no per-symbol PW).
-- Latest AS_OF_TS per PORTFOLIO_ID from V_PARALLEL_WORLD_CONFIDENCE; aggregates across scenarios.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PROPOSAL_PW_PORTFOLIO_CONTEXT (
    PORTFOLIO_ID,
    PW_AS_OF_TS,
    PW_SCENARIO_COUNT,
    PW_SUPPORTING_SCENARIOS,
    PW_AVG_OUTPERFORM_PCT,
    PW_AVG_CUMULATIVE_REGRET,
    PW_BEST_CASE_DELTA,
    PW_WORST_CASE_DELTA,
    PW_SCENARIO_SPREAD,
    PW_DOMINANT_CONFIDENCE_CLASS
) as
with latest_ts as (
    select
        PORTFOLIO_ID,
        max(AS_OF_TS) as mx_ts
    from MIP.MART.V_PARALLEL_WORLD_CONFIDENCE
    group by PORTFOLIO_ID
),
scen as (
    select
        c.PORTFOLIO_ID,
        c.AS_OF_TS,
        c.SCENARIO_ID,
        c.CONFIDENCE_CLASS,
        c.OUTPERFORM_PCT,
        c.CUMULATIVE_REGRET,
        c.CUMULATIVE_DELTA
    from MIP.MART.V_PARALLEL_WORLD_CONFIDENCE c
    inner join latest_ts t
      on t.PORTFOLIO_ID = c.PORTFOLIO_ID
     and t.mx_ts = c.AS_OF_TS
)
select
    PORTFOLIO_ID,
    max(AS_OF_TS) as PW_AS_OF_TS,
    count(*)::number as PW_SCENARIO_COUNT,
    count_if(CONFIDENCE_CLASS in ('STRONG', 'EMERGING'))::number as PW_SUPPORTING_SCENARIOS,
    avg(OUTPERFORM_PCT) as PW_AVG_OUTPERFORM_PCT,
    avg(CUMULATIVE_REGRET) as PW_AVG_CUMULATIVE_REGRET,
    max(CUMULATIVE_DELTA) as PW_BEST_CASE_DELTA,
    min(CUMULATIVE_DELTA) as PW_WORST_CASE_DELTA,
    (max(CUMULATIVE_DELTA) - min(CUMULATIVE_DELTA)) as PW_SCENARIO_SPREAD,
    max_by(CONFIDENCE_CLASS, OUTPERFORM_PCT) as PW_DOMINANT_CONFIDENCE_CLASS
from scen
group by PORTFOLIO_ID;

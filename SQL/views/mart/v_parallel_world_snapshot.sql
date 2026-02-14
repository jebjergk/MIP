-- v_parallel_world_snapshot.sql
-- Purpose: Composes a deterministic JSON snapshot for Parallel Worlds narrative generation.
-- Assembles facts from diff view, regret view, and decision traces into a single VARIANT
-- per (PORTFOLIO_ID, AS_OF_TS). This is the input for Cortex narrative generation.
--
-- Sources:
--   MIP.MART.V_PARALLEL_WORLD_DIFF    — per-scenario deltas
--   MIP.MART.V_PARALLEL_WORLD_REGRET  — rolling regret metrics
--   MIP.APP.PARALLEL_WORLD_RESULT     — raw results (for ACTUAL traces)
--   MIP.APP.PARALLEL_WORLD_SCENARIO   — scenario metadata

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PARALLEL_WORLD_SNAPSHOT (
    RUN_ID,
    PORTFOLIO_ID,
    AS_OF_TS,
    SNAPSHOT_JSON
) as
with
-- Latest run per portfolio-day
latest_run as (
    select
        RUN_ID,
        PORTFOLIO_ID,
        AS_OF_TS,
        row_number() over (partition by PORTFOLIO_ID, AS_OF_TS::date order by CREATED_AT desc) as rn
    from MIP.APP.PARALLEL_WORLD_RESULT
    where SCENARIO_ID = 0  -- ACTUAL row
),

-- Scenario diffs aggregated
scenario_diffs as (
    select
        d.RUN_ID,
        d.PORTFOLIO_ID,
        d.AS_OF_TS,
        array_agg(
            object_construct(
                'scenario_id', d.SCENARIO_ID,
                'scenario_name', d.SCENARIO_NAME,
                'scenario_type', d.SCENARIO_TYPE,
                'pnl_delta', round(d.PNL_DELTA, 2),
                'equity_delta', round(d.EQUITY_DELTA, 2),
                'return_pct_delta', round(d.RETURN_PCT_DELTA, 6),
                'trades_delta', d.TRADES_DELTA,
                'outperformed', d.OUTPERFORMED,
                'cf_pnl', round(d.CF_PNL, 2),
                'cf_equity', round(d.CF_EQUITY, 2),
                'cf_trades', d.CF_TRADES
            )
        ) within group (order by d.PNL_DELTA desc) as SCENARIOS
    from MIP.MART.V_PARALLEL_WORLD_DIFF d
    join latest_run lr on lr.RUN_ID = d.RUN_ID and lr.PORTFOLIO_ID = d.PORTFOLIO_ID and lr.rn = 1
    group by d.RUN_ID, d.PORTFOLIO_ID, d.AS_OF_TS
),

-- Best and worst scenarios
best_worst as (
    select
        d.RUN_ID,
        d.PORTFOLIO_ID,
        d.AS_OF_TS,
        max_by(d.SCENARIO_NAME, d.PNL_DELTA) as BEST_SCENARIO,
        max(d.PNL_DELTA)                   as BEST_PNL_DELTA,
        min_by(d.SCENARIO_NAME, d.PNL_DELTA) as WORST_SCENARIO,
        min(d.PNL_DELTA)                   as WORST_PNL_DELTA,
        count_if(d.OUTPERFORMED)           as SCENARIOS_OUTPERFORMED,
        count(*)                         as TOTAL_SCENARIOS
    from MIP.MART.V_PARALLEL_WORLD_DIFF d
    join latest_run lr on lr.RUN_ID = d.RUN_ID and lr.PORTFOLIO_ID = d.PORTFOLIO_ID and lr.rn = 1
    group by d.RUN_ID, d.PORTFOLIO_ID, d.AS_OF_TS
),

-- Actual world summary
actual_summary as (
    select
        r.RUN_ID,
        r.PORTFOLIO_ID,
        r.AS_OF_TS,
        r.PNL_SIMULATED as ACTUAL_PNL,
        r.END_EQUITY_SIMULATED as ACTUAL_EQUITY,
        r.TRADES_SIMULATED as ACTUAL_TRADES,
        r.OPEN_POSITIONS_END as ACTUAL_POSITIONS,
        r.RESULT_JSON as ACTUAL_DECISION_TRACE
    from MIP.APP.PARALLEL_WORLD_RESULT r
    join latest_run lr on lr.RUN_ID = r.RUN_ID and lr.PORTFOLIO_ID = r.PORTFOLIO_ID and lr.rn = 1
    where r.SCENARIO_ID = 0
),

-- Regret summary (latest day per scenario)
regret_summary as (
    select
        rg.PORTFOLIO_ID,
        array_agg(
            object_construct(
                'scenario_name', rg.SCENARIO_NAME,
                'scenario_type', rg.SCENARIO_TYPE,
                'rolling_regret_20d', round(rg.ROLLING_REGRET_20D, 2),
                'rolling_avg_delta_20d', round(rg.ROLLING_AVG_DELTA_20D, 2),
                'outperform_pct', rg.OUTPERFORM_PCT,
                'cumulative_regret', round(rg.CUMULATIVE_REGRET, 2)
            )
        ) within group (order by rg.ROLLING_REGRET_20D desc) as REGRET_BY_SCENARIO
    from MIP.MART.V_PARALLEL_WORLD_REGRET rg
    join latest_run lr on lr.PORTFOLIO_ID = rg.PORTFOLIO_ID and lr.rn = 1
    where rg.AS_OF_TS::date = lr.AS_OF_TS::date
    group by rg.PORTFOLIO_ID
)

select
    a.RUN_ID,
    a.PORTFOLIO_ID,
    a.AS_OF_TS,
    object_construct(
        'actual', object_construct(
            'pnl', round(a.ACTUAL_PNL, 2),
            'equity', round(a.ACTUAL_EQUITY, 2),
            'trades', a.ACTUAL_TRADES,
            'positions', a.ACTUAL_POSITIONS,
            'decision_trace', a.ACTUAL_DECISION_TRACE
        ),
        'scenarios', coalesce(sd.SCENARIOS, array_construct()),
        'summary', object_construct(
            'best_scenario', bw.BEST_SCENARIO,
            'best_pnl_delta', round(bw.BEST_PNL_DELTA, 2),
            'worst_scenario', bw.WORST_SCENARIO,
            'worst_pnl_delta', round(bw.WORST_PNL_DELTA, 2),
            'scenarios_outperformed', bw.SCENARIOS_OUTPERFORMED,
            'total_scenarios', bw.TOTAL_SCENARIOS
        ),
        'regret', coalesce(rgs.REGRET_BY_SCENARIO, array_construct()),
        'metadata', object_construct(
            'run_id', a.RUN_ID,
            'as_of_ts', a.AS_OF_TS::string,
            'portfolio_id', a.PORTFOLIO_ID
        )
    ) as SNAPSHOT_JSON
from actual_summary a
left join scenario_diffs sd
  on sd.RUN_ID = a.RUN_ID and sd.PORTFOLIO_ID = a.PORTFOLIO_ID
left join best_worst bw
  on bw.RUN_ID = a.RUN_ID and bw.PORTFOLIO_ID = a.PORTFOLIO_ID
left join regret_summary rgs
  on rgs.PORTFOLIO_ID = a.PORTFOLIO_ID;

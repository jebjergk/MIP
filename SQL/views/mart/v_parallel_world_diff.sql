-- v_parallel_world_diff.sql
-- Purpose: Computes deltas between each COUNTERFACTUAL scenario and the ACTUAL world
-- for the same (RUN_ID, PORTFOLIO_ID, AS_OF_TS). Provides PNL, return, drawdown,
-- trade count, and equity deltas. Includes gating reason flags extracted from the
-- ACTUAL world's decision trace.
--
-- Sources:
--   MIP.APP.PARALLEL_WORLD_RESULT     — simulation results (ACTUAL + COUNTERFACTUAL)
--   MIP.APP.PARALLEL_WORLD_SCENARIO   — scenario metadata

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PARALLEL_WORLD_DIFF (
    RUN_ID,
    PORTFOLIO_ID,
    AS_OF_TS,
    EPISODE_ID,
    SCENARIO_ID,
    SCENARIO_NAME,
    SCENARIO_DISPLAY_NAME,
    SCENARIO_TYPE,
    -- Actual metrics
    ACTUAL_PNL,
    ACTUAL_RETURN_PCT,
    ACTUAL_EQUITY,
    ACTUAL_TRADES,
    ACTUAL_POSITIONS,
    ACTUAL_DRAWDOWN,
    -- Counterfactual metrics
    CF_PNL,
    CF_RETURN_PCT,
    CF_EQUITY,
    CF_TRADES,
    CF_POSITIONS,
    CF_DRAWDOWN,
    -- Deltas (counterfactual - actual)
    PNL_DELTA,
    RETURN_PCT_DELTA,
    EQUITY_DELTA,
    TRADES_DELTA,
    POSITIONS_DELTA,
    DRAWDOWN_DELTA,
    -- Classification
    OUTPERFORMED,
    -- Gate flags from ACTUAL decision trace
    RISK_STATUS,
    ENTRIES_BLOCKED,
    CAPACITY_STATUS,
    -- Raw decision traces
    ACTUAL_RESULT_JSON,
    CF_RESULT_JSON
) as
select
    cf.RUN_ID,
    cf.PORTFOLIO_ID,
    cf.AS_OF_TS,
    cf.EPISODE_ID,
    cf.SCENARIO_ID,
    s.NAME                                          as SCENARIO_NAME,
    coalesce(s.DISPLAY_NAME, s.NAME)                as SCENARIO_DISPLAY_NAME,
    s.SCENARIO_TYPE,
    -- Actual
    act.PNL_SIMULATED                               as ACTUAL_PNL,
    act.RETURN_PCT_SIMULATED                        as ACTUAL_RETURN_PCT,
    act.END_EQUITY_SIMULATED                        as ACTUAL_EQUITY,
    act.TRADES_SIMULATED                            as ACTUAL_TRADES,
    act.OPEN_POSITIONS_END                          as ACTUAL_POSITIONS,
    act.MAX_DRAWDOWN_PCT_SIMULATED                  as ACTUAL_DRAWDOWN,
    -- Counterfactual
    cf.PNL_SIMULATED                                as CF_PNL,
    cf.RETURN_PCT_SIMULATED                         as CF_RETURN_PCT,
    cf.END_EQUITY_SIMULATED                         as CF_EQUITY,
    cf.TRADES_SIMULATED                             as CF_TRADES,
    cf.OPEN_POSITIONS_END                           as CF_POSITIONS,
    cf.MAX_DRAWDOWN_PCT_SIMULATED                   as CF_DRAWDOWN,
    -- Deltas
    cf.PNL_SIMULATED - act.PNL_SIMULATED            as PNL_DELTA,
    cf.RETURN_PCT_SIMULATED - act.RETURN_PCT_SIMULATED as RETURN_PCT_DELTA,
    cf.END_EQUITY_SIMULATED - act.END_EQUITY_SIMULATED as EQUITY_DELTA,
    cf.TRADES_SIMULATED - act.TRADES_SIMULATED      as TRADES_DELTA,
    cf.OPEN_POSITIONS_END - act.OPEN_POSITIONS_END  as POSITIONS_DELTA,
    cf.MAX_DRAWDOWN_PCT_SIMULATED - act.MAX_DRAWDOWN_PCT_SIMULATED as DRAWDOWN_DELTA,
    -- Classification
    iff(cf.PNL_SIMULATED > act.PNL_SIMULATED, true, false) as OUTPERFORMED,
    -- Gate flags from ACTUAL trace
    act.RESULT_JSON:decision_trace[0]:risk_status::varchar   as RISK_STATUS,
    act.RESULT_JSON:decision_trace[0]:entries_blocked::boolean as ENTRIES_BLOCKED,
    act.RESULT_JSON:decision_trace[1]:status::varchar         as CAPACITY_STATUS,
    -- Raw traces
    act.RESULT_JSON                                 as ACTUAL_RESULT_JSON,
    cf.RESULT_JSON                                  as CF_RESULT_JSON
from MIP.APP.PARALLEL_WORLD_RESULT cf
join MIP.APP.PARALLEL_WORLD_RESULT act
  on act.RUN_ID       = cf.RUN_ID
 and act.PORTFOLIO_ID = cf.PORTFOLIO_ID
 and act.AS_OF_TS     = cf.AS_OF_TS
 and act.SCENARIO_ID  = 0                           -- ACTUAL sentinel
join MIP.APP.PARALLEL_WORLD_SCENARIO s
  on s.SCENARIO_ID = cf.SCENARIO_ID
where cf.WORLD_KEY = 'COUNTERFACTUAL';

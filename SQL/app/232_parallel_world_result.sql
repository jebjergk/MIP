-- 232_parallel_world_result.sql
-- Purpose: Simulation results for Parallel Worlds — one row per (run, portfolio, day, scenario).
-- WORLD_KEY distinguishes ACTUAL (the real outcome) from COUNTERFACTUAL (simulated alternative).
-- RESULT_JSON contains the full decision trace DAG, simulated trades, and equity curve points.
-- Composite PK ensures idempotent MERGE on re-runs.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.PARALLEL_WORLD_RESULT (
    RUN_ID                      varchar(64)   not null,
    PORTFOLIO_ID                number        not null,
    AS_OF_TS                    timestamp_ntz not null,
    SCENARIO_ID                 number        not null,
    WORLD_KEY                   varchar(32)   not null,  -- ACTUAL | COUNTERFACTUAL
    EPISODE_ID                  number,
    TRADES_SIMULATED            number        default 0,
    PNL_SIMULATED               number(18,4)  default 0,
    RETURN_PCT_SIMULATED        number(18,8)  default 0,
    MAX_DRAWDOWN_PCT_SIMULATED  number(18,8)  default 0,
    END_EQUITY_SIMULATED        number(18,4),
    CASH_END_SIMULATED          number(18,4),
    OPEN_POSITIONS_END          number        default 0,
    RESULT_JSON                 variant,               -- Full decision trace DAG + details
    CREATED_AT                  timestamp_ntz default current_timestamp(),

    constraint PK_PW_RESULT primary key (RUN_ID, PORTFOLIO_ID, AS_OF_TS, SCENARIO_ID)
);

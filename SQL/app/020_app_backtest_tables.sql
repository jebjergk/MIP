-- 020_app_backtest_tables.sql
-- Purpose: Backtest run and result tables for MIP

use role MIP_ADMIN_ROLE;
use database MIP;

-----------------------------
-- 1. BACKTEST_RUN
-----------------------------
create or replace table MIP.APP.BACKTEST_RUN (
    BACKTEST_RUN_ID      number autoincrement start 1 increment 1,
    CREATED_AT           timestamp_ntz default MIP.APP.F_NOW_BERLIN_NTZ(),
    MARKET_TYPE          string,
    INTERVAL_MINUTES     number,
    HORIZON_MINUTES      number,
    HIT_THRESHOLD        float,
    MISS_THRESHOLD       float,
    FROM_TS              timestamp_ntz,
    TO_TS                timestamp_ntz,
    NOTES                string,

    constraint PK_BACKTEST_RUN primary key (BACKTEST_RUN_ID)
);

-----------------------------
-- 2. BACKTEST_RESULT
-----------------------------
create or replace table MIP.APP.BACKTEST_RESULT (
    BACKTEST_RUN_ID   number,
    PATTERN_ID        number,
    SYMBOL            string,
    TRADE_COUNT       number,
    HIT_COUNT         number,
    MISS_COUNT        number,
    NEUTRAL_COUNT     number,
    HIT_RATE          float,
    AVG_RETURN        float,
    STD_RETURN        float,
    CUM_RETURN        float,
    DETAILS           variant,

    constraint FK_BACKTEST_RESULT_RUN
        foreign key (BACKTEST_RUN_ID)
        references MIP.APP.BACKTEST_RUN(BACKTEST_RUN_ID)
);

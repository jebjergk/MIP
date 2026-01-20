-- 020_app_backtest_tables.sql
-- Purpose: Backtest run and result tables for MIP

use role MIP_ADMIN_ROLE;
use database MIP;

-----------------------------
-- 1. BACKTEST_RUN
-----------------------------
create table if not exists MIP.APP.BACKTEST_RUN (
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

alter table MIP.APP.BACKTEST_RUN
    add column if not exists BACKTEST_RUN_ID number;
alter table MIP.APP.BACKTEST_RUN
    add column if not exists CREATED_AT timestamp_ntz default MIP.APP.F_NOW_BERLIN_NTZ();
alter table MIP.APP.BACKTEST_RUN
    add column if not exists MARKET_TYPE string;
alter table MIP.APP.BACKTEST_RUN
    add column if not exists INTERVAL_MINUTES number;
alter table MIP.APP.BACKTEST_RUN
    add column if not exists HORIZON_MINUTES number;
alter table MIP.APP.BACKTEST_RUN
    add column if not exists HIT_THRESHOLD float;
alter table MIP.APP.BACKTEST_RUN
    add column if not exists MISS_THRESHOLD float;
alter table MIP.APP.BACKTEST_RUN
    add column if not exists FROM_TS timestamp_ntz;
alter table MIP.APP.BACKTEST_RUN
    add column if not exists TO_TS timestamp_ntz;
alter table MIP.APP.BACKTEST_RUN
    add column if not exists NOTES string;

-----------------------------
-- 2. BACKTEST_RESULT
-----------------------------
create table if not exists MIP.APP.BACKTEST_RESULT (
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

alter table MIP.APP.BACKTEST_RESULT
    add column if not exists BACKTEST_RUN_ID number;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists PATTERN_ID number;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists SYMBOL string;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists TRADE_COUNT number;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists HIT_COUNT number;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists MISS_COUNT number;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists NEUTRAL_COUNT number;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists HIT_RATE float;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists AVG_RETURN float;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists STD_RETURN float;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists CUM_RETURN float;
alter table MIP.APP.BACKTEST_RESULT
    add column if not exists DETAILS variant;

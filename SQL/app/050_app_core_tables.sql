-- 050_app_core_tables.sql
-- Purpose: Core application tables for MIP patterns, recommendations, and outcomes

use role MIP_ADMIN_ROLE;
use database MIP;

-----------------------------
-- 1. PATTERN_DEFINITION
-----------------------------
create table if not exists MIP.APP.PATTERN_DEFINITION (
    PATTERN_ID    number        autoincrement,
    NAME          string        not null,
    DESCRIPTION   string,
    ENABLED       boolean       default true,
    CREATED_AT    timestamp_ntz default current_timestamp(),
    CREATED_BY    string        default current_user(),
    UPDATED_AT    timestamp_ntz,
    UPDATED_BY    string,
    IS_ACTIVE               string        default 'Y',
    LAST_TRAINED_AT         timestamp_ntz,
    LAST_BACKTEST_RUN_ID    number,
    LAST_TRADE_COUNT        number,
    LAST_HIT_RATE           float,
    LAST_CUM_RETURN         float,
    LAST_AVG_RETURN         float,
    LAST_STD_RETURN         float,
    PATTERN_SCORE           float,
    constraint PK_PATTERN_DEFINITION primary key (PATTERN_ID),
    constraint UQ_PATTERN_NAME unique (NAME)
);

-----------------------------
-- 2. RECOMMENDATION_LOG
-----------------------------
create table if not exists MIP.APP.RECOMMENDATION_LOG (
    RECOMMENDATION_ID number        autoincrement,
    PATTERN_ID        number        not null,
    SYMBOL            string        not null,
    MARKET_TYPE       string        not null,  -- 'STOCK' or 'FX'
    INTERVAL_MINUTES  number        not null,
    TS                timestamp_ntz not null,  -- bar timestamp from MART
    GENERATED_AT      timestamp_ntz default current_timestamp(),
    SCORE             number,                 -- e.g. return, z-score, etc.
    DETAILS           variant,                -- JSON with extra info
    constraint PK_RECOMMENDATION_LOG primary key (RECOMMENDATION_ID)
    -- We can add a foreign key later if desired:
    -- constraint FK_RECOMMENDATION_PATTERN
    --   foreign key (PATTERN_ID) references MIP.APP.PATTERN_DEFINITION(PATTERN_ID)
);

-----------------------------
-- 3. OUTCOME_EVALUATION (for later)
-----------------------------
create table if not exists MIP.APP.OUTCOME_EVALUATION (
    OUTCOME_ID        number        autoincrement,
    RECOMMENDATION_ID number        not null,
    EVALUATED_AT      timestamp_ntz default current_timestamp(),
    HORIZON_MINUTES   number,
    RETURN_REALIZED   number,       -- realized return over horizon
    OUTCOME_LABEL     string,       -- 'HIT', 'MISS', 'NEUTRAL', etc.
    DETAILS           variant,
    constraint PK_OUTCOME_EVALUATION primary key (OUTCOME_ID)
);

-----------------------------
-- 4. BACKTEST_RUN
-----------------------------
create table if not exists MIP.APP.BACKTEST_RUN (
    BACKTEST_RUN_ID   number        autoincrement,
    CREATED_AT        timestamp_ntz default current_timestamp(),
    MARKET_TYPE       string,
    INTERVAL_MINUTES  number,
    HORIZON_MINUTES   number,
    HIT_THRESHOLD     float,
    MISS_THRESHOLD    float,
    FROM_TS           timestamp_ntz,
    TO_TS             timestamp_ntz,
    NOTES             string,
    constraint PK_BACKTEST_RUN primary key (BACKTEST_RUN_ID)
);

-----------------------------
-- 5. BACKTEST_RESULT
-----------------------------
create table if not exists MIP.APP.BACKTEST_RESULT (
    BACKTEST_RUN_ID number,
    PATTERN_ID      number,
    SYMBOL          string,
    TRADE_COUNT     number,
    HIT_COUNT       number,
    MISS_COUNT      number,
    NEUTRAL_COUNT   number,
    HIT_RATE        float,
    AVG_RETURN      float,
    STD_RETURN      float,
    CUM_RETURN      float,
    DETAILS         variant,
    constraint FK_BACKTEST_RESULT_RUN foreign key (BACKTEST_RUN_ID) references MIP.APP.BACKTEST_RUN(BACKTEST_RUN_ID),
    constraint FK_BACKTEST_RESULT_PATTERN foreign key (PATTERN_ID) references MIP.APP.PATTERN_DEFINITION(PATTERN_ID)
);

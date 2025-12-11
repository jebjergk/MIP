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
    PARAMS_JSON   variant,
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

-- Seed core momentum patterns (idempotent)
merge into MIP.APP.PATTERN_DEFINITION t
using (
    select
        'MOMENTUM_DEMO' as NAME,
        'Demo momentum pattern' as DESCRIPTION,
        object_construct(
            'fast_window', 20,
            'slow_window', 3,
            'lookback_days', 1,
            'min_return', 0.002,
            'min_zscore', 1.0,
            'market_type', 'STOCK',
            'interval_minutes', 5
        ) as PARAMS_JSON,
        'Y' as IS_ACTIVE,
        true as ENABLED
    union all
    select
        'STOCK_MOMENTUM_FAST',
        'Stock momentum (fast, stricter)',
        object_construct(
            'fast_window', 20,
            'slow_window', 3,
            'lookback_days', 1,
            'min_return', 0.002,
            'min_zscore', 1.0,
            'market_type', 'STOCK',
            'interval_minutes', 5
        ),
        'Y',
        true
    union all
    select
        'STOCK_MOMENTUM_SLOW',
        'Stock momentum (slow, looser)',
        object_construct(
            'fast_window', 30,
            'slow_window', 2,
            'lookback_days', 3,
            'min_return', 0.001,
            'min_zscore', 0.75,
            'market_type', 'STOCK',
            'interval_minutes', 5
        ),
        'Y',
        true
    union all
    select
        'FX_MOMENTUM_DAILY',
        'FX momentum (daily)',
        object_construct(
            'fast_window', 10,
            'slow_window', 20,
            'lookback_days', 60,
            'min_return', 0.001,
            'min_zscore', 0.0,
            'market_type', 'FX',
            'interval_minutes', 1440
        ),
        'Y',
        true
) s
   on t.NAME = s.NAME
 when matched then update set
   t.DESCRIPTION = s.DESCRIPTION,
    t.PARAMS_JSON = coalesce(t.PARAMS_JSON, s.PARAMS_JSON),
    t.IS_ACTIVE   = coalesce(t.IS_ACTIVE, s.IS_ACTIVE),
    t.ENABLED     = coalesce(t.ENABLED, s.ENABLED)
 when not matched then insert (NAME, DESCRIPTION, PARAMS_JSON, IS_ACTIVE, ENABLED)
 values (s.NAME, s.DESCRIPTION, s.PARAMS_JSON, s.IS_ACTIVE, s.ENABLED);

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

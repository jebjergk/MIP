-- 340_early_exit_tables.sql
-- Purpose: Schema for the intraday early-exit layer.
-- Stores evaluation results, exit signals, and configuration for the two-stage
-- payoff+giveback exit policy applied to daily positions using 15-minute bars.

use role MIP_ADMIN_ROLE;
use database MIP;

-----------------------------
-- 1. EARLY_EXIT_LOG
-- One row per position per evaluation run.
-- Records whether payoff was reached, giveback detected, and the decision made.
-----------------------------
create table if not exists MIP.APP.EARLY_EXIT_LOG (
    LOG_ID              number        autoincrement,
    RUN_ID              varchar(64)   not null,
    PORTFOLIO_ID        number        not null,
    SYMBOL              varchar(20)   not null,
    MARKET_TYPE         varchar(20)   not null,
    ENTRY_TS            timestamp_ntz not null,
    ENTRY_PRICE         number(18,8)  not null,
    QUANTITY            number(18,8)  not null,
    COST_BASIS          number(18,8)  not null,
    HOLD_UNTIL_INDEX    number        not null,

    -- Timing
    BAR_CLOSE_TS        timestamp_ntz not null,
    DECISION_TS         timestamp_ntz not null default current_timestamp(),

    -- Target
    TARGET_RETURN       number(18,8),
    PAYOFF_MULTIPLIER   number(18,4),
    EFFECTIVE_TARGET    number(18,8),

    -- Stage A: Payoff
    CURRENT_PRICE       number(18,8),
    UNREALIZED_RETURN   number(18,8),
    MFE_RETURN          number(18,8),
    MFE_TS              timestamp_ntz,
    PAYOFF_REACHED      boolean       not null default false,
    PAYOFF_FIRST_HIT_TS timestamp_ntz,
    PAYOFF_HIT_AFTER_MINS number,

    -- Stage B: Giveback
    GIVEBACK_FROM_PEAK  number(18,8),
    GIVEBACK_PCT        number(18,8),
    NO_NEW_HIGH_BARS    number,
    GIVEBACK_TRIGGERED  boolean       not null default false,

    -- Decision
    EXIT_SIGNAL         boolean       not null default false,
    EXIT_PRICE          number(18,8),
    FEES_APPLIED        number(18,8),
    EARLY_EXIT_PNL      number(18,4),
    HOLD_TO_END_RETURN  number(18,8),
    HOLD_TO_END_PNL     number(18,4),
    PNL_DELTA           number(18,4),

    -- Execution
    MODE                varchar(10)   not null default 'SHADOW',
    EXECUTION_STATUS    varchar(20)   not null default 'SIGNAL_ONLY',
    REASON_CODES        variant,

    CREATED_AT          timestamp_ntz default current_timestamp(),

    constraint PK_EARLY_EXIT_LOG primary key (LOG_ID)
);

-----------------------------
-- 2. EARLY_EXIT_POSITION_STATE
-- Tracks per-position early-exit state across pipeline runs.
-- Persists first_hit_ts and MFE so we don't recompute from scratch each run.
-----------------------------
create table if not exists MIP.APP.EARLY_EXIT_POSITION_STATE (
    PORTFOLIO_ID        number        not null,
    SYMBOL              varchar(20)   not null,
    ENTRY_TS            timestamp_ntz not null,
    FIRST_HIT_TS        timestamp_ntz,
    FIRST_HIT_RETURN    number(18,8),
    MFE_RETURN          number(18,8),
    MFE_TS              timestamp_ntz,
    MAE_RETURN          number(18,8),
    MAE_TS              timestamp_ntz,
    LAST_EVALUATED_TS   timestamp_ntz,
    EARLY_EXIT_FIRED    boolean       default false,
    EARLY_EXIT_TS       timestamp_ntz,
    UPDATED_AT          timestamp_ntz default current_timestamp(),

    constraint PK_EARLY_EXIT_POS_STATE primary key (PORTFOLIO_ID, SYMBOL, ENTRY_TS)
);

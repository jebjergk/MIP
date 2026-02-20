-- 300_intraday_tables.sql
-- Purpose: Schema additions and new tables for the intraday subsystem.
-- Phase 1a infrastructure — no behavioral change to daily pipeline.
-- All additions are additive with safe defaults.

use role MIP_ADMIN_ROLE;
use database MIP;

------------------------------
-- 1. PATTERN_TYPE on PATTERN_DEFINITION
--    Enables dispatcher routing to the correct detector proc.
--    Default 'MOMENTUM' means all existing patterns are unaffected.
------------------------------
alter table if exists MIP.APP.PATTERN_DEFINITION
    add column if not exists PATTERN_TYPE varchar default 'MOMENTUM';

------------------------------
-- 2. Excursion columns on RECOMMENDATION_OUTCOMES
--    Track best/worst price during holding period for any pattern type.
------------------------------
alter table if exists MIP.APP.RECOMMENDATION_OUTCOMES
    add column if not exists MAX_FAVORABLE_EXCURSION number(18,8);

alter table if exists MIP.APP.RECOMMENDATION_OUTCOMES
    add column if not exists MAX_ADVERSE_EXCURSION number(18,8);

------------------------------
-- 3. INTRADAY_PIPELINE_RUN_LOG
--    One row per intraday pipeline execution.
------------------------------
create table if not exists MIP.APP.INTRADAY_PIPELINE_RUN_LOG (
    RUN_ID              varchar(64)   not null,
    INTERVAL_MINUTES    number        not null,
    STARTED_AT          timestamp_ntz not null,
    COMPLETED_AT        timestamp_ntz,
    STATUS              varchar(32),
    BARS_INGESTED       number,
    SIGNALS_GENERATED   number,
    OUTCOMES_EVALUATED  number,
    SYMBOLS_PROCESSED   number,
    DAILY_CONTEXT_USED  boolean       default false,
    COMPUTE_SECONDS     float,
    DETAILS             variant,
    CREATED_AT          timestamp_ntz default current_timestamp(),
    constraint PK_INTRADAY_PIPELINE_RUN_LOG primary key (RUN_ID)
);

------------------------------
-- 4. INTRADAY_FEE_CONFIG
--    Fee schedule for intraday outcome evaluation.
--    Separate from APP_CONFIG execution fees so intraday
--    can model different cost assumptions independently.
------------------------------
create table if not exists MIP.APP.INTRADAY_FEE_CONFIG (
    FEE_PROFILE     varchar(64)   not null default 'DEFAULT',
    FEE_BPS         number(10,4)  not null default 1.0,
    SLIPPAGE_BPS    number(10,4)  not null default 2.0,
    SPREAD_BPS      number(10,4)  not null default 1.0,
    MIN_FEE_USD     number(10,4)  default 0,
    IS_ACTIVE       boolean       default true,
    DESCRIPTION     varchar,
    UPDATED_AT      timestamp_ntz default current_timestamp(),
    constraint PK_INTRADAY_FEE_CONFIG primary key (FEE_PROFILE)
);

------------------------------
-- 5. INTRADAY_REPLAY_CONTEXT
--    Enables bar-by-bar replay for intraday pipeline reproducibility.
--    Mirrors APP.REPLAY_CONTEXT but scoped to intraday.
------------------------------
create table if not exists MIP.APP.INTRADAY_REPLAY_CONTEXT (
    RUN_ID            varchar(64)   not null,
    REPLAY_BATCH_ID   varchar(64),
    EFFECTIVE_TO_TS   timestamp_ntz not null,
    INTERVAL_MINUTES  number        not null,
    CREATED_AT        timestamp_ntz default current_timestamp()
);

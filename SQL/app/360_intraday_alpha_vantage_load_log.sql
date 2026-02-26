-- 360_intraday_alpha_vantage_load_log.sql
-- Purpose: Symbol-month checkpoint log for Alpha Vantage intraday backfill loads.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.INTRADAY_ALPHA_VANTAGE_LOAD_LOG (
    RUN_ID                varchar(64)   not null,
    SYMBOL                varchar(32)   not null,
    MARKET_TYPE           varchar(16)   not null,
    INTERVAL_MINUTES      number        not null,
    MONTH_YYYY_MM         varchar(7)    not null,
    STATUS                varchar(16)   not null default 'PENDING', -- PENDING|RUNNING|DONE|FAILED
    ATTEMPT_COUNT         number        not null default 0,
    STARTED_AT            timestamp_ntz,
    COMPLETED_AT          timestamp_ntz,
    ROWS_PARSED           number        default 0,
    ROWS_INSERTED         number        default 0,
    ROWS_UPDATED          number        default 0,
    API_URL               varchar,
    API_NOTE              varchar,
    ERROR_MESSAGE         varchar,
    PAYLOAD_TIMEZONE      varchar,
    PAYLOAD_LAST_REFRESH  varchar,
    REQUESTED_AT          timestamp_ntz default current_timestamp(),
    CREATED_AT            timestamp_ntz default current_timestamp(),
    UPDATED_AT            timestamp_ntz default current_timestamp(),
    DETAILS               variant,
    constraint PK_INTRADAY_ALPHA_VANTAGE_LOAD_LOG
        primary key (RUN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, MONTH_YYYY_MM)
);

alter table if exists MIP.APP.INTRADAY_ALPHA_VANTAGE_LOAD_LOG
    add column if not exists DETAILS variant;


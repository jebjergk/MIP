-- 185_agent_out_morning_brief.sql
-- Purpose: Persist morning brief outputs for agent consumption.
-- Portfolio briefs: PORTFOLIO_ID > 0, RUN_ID = pipeline run id, BRIEF = payload.
-- Agent briefs: PORTFOLIO_ID = 0, RUN_ID = pipeline run id (UUID), AGENT_NAME, STATUS, BRIEF_JSON, CREATED_AT; SIGNAL_RUN_ID nullable for backwards compat.

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.AGENT_OUT;

create table if not exists MIP.AGENT_OUT.MORNING_BRIEF (
    BRIEF_ID        number identity,
    AS_OF_TS        timestamp_ntz default current_timestamp(),
    PORTFOLIO_ID    number not null,
    RUN_ID          varchar(64) not null,   -- pipeline run id (UUID for agent; run id for portfolio)
    BRIEF           variant not null,
    PIPELINE_RUN_ID varchar(64),
    -- Agent v0 columns (nullable for backwards compat; populated when PORTFOLIO_ID = 0)
    AGENT_NAME      varchar(128),
    STATUS          varchar(64),
    BRIEF_JSON      variant,
    CREATED_AT      timestamp_ntz,
    SIGNAL_RUN_ID   varchar(64),            -- nullable; same as RUN_ID for agent rows (backwards compat)
    constraint PK_MORNING_BRIEF primary key (BRIEF_ID),
    constraint UQ_MORNING_BRIEF_PORTFOLIO_RUN unique (PORTFOLIO_ID, RUN_ID)
);

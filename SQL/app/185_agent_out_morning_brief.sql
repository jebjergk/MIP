-- 185_agent_out_morning_brief.sql
-- Purpose: Persist morning brief outputs for agent consumption.
-- Portfolio briefs: PORTFOLIO_ID > 0, RUN_ID = pipeline run id, BRIEF = payload.
-- Agent briefs: PORTFOLIO_ID = 0, RUN_ID = pipeline run id (UUID), MERGE key (AS_OF_TS, RUN_ID, AGENT_NAME).
-- RUN_ID nullable at first for backwards compat. SIGNAL_RUN_ID kept for compat; not used for joins.

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.AGENT_OUT;

create table if not exists MIP.AGENT_OUT.MORNING_BRIEF (
    BRIEF_ID        number identity,
    AS_OF_TS        timestamp_ntz default current_timestamp(),
    PORTFOLIO_ID    number not null,
    RUN_ID          varchar(64),             -- pipeline run id (UUID); nullable for backwards compat
    BRIEF           variant not null,
    PIPELINE_RUN_ID varchar(64),
    -- Agent v0 columns (nullable for backwards compat; populated when PORTFOLIO_ID = 0)
    AGENT_NAME      varchar(128),
    STATUS          varchar(64),
    BRIEF_JSON      variant,
    CREATED_AT      timestamp_ntz default current_timestamp(),  -- When brief was generated (for "latest" selection)
    UPDATED_AT      timestamp_ntz,                              -- When brief was last updated
    SIGNAL_RUN_ID   varchar(64),             -- nullable; populate if available; not used for joins
    constraint PK_MORNING_BRIEF primary key (BRIEF_ID),
    constraint UQ_MORNING_BRIEF_AS_OF_RUN_AGENT unique (PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME)
);

-- Migration for existing deployments: add columns if missing.
-- Note: Snowflake doesn't support ALTER COLUMN SET DEFAULT, so we rely on SP_WRITE_MORNING_BRIEF
-- to always set CREATED_AT explicitly. The default in CREATE TABLE only applies to fresh deployments.
alter table MIP.AGENT_OUT.MORNING_BRIEF add column if not exists CREATED_AT timestamp_ntz;
alter table MIP.AGENT_OUT.MORNING_BRIEF add column if not exists UPDATED_AT timestamp_ntz;

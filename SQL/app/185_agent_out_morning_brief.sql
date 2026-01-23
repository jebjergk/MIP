-- 185_agent_out_morning_brief.sql
-- Purpose: Persist morning brief outputs for agent consumption

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.AGENT_OUT;

create table if not exists MIP.AGENT_OUT.MORNING_BRIEF (
    BRIEF_ID        number identity,
    AS_OF_TS        timestamp_ntz default current_timestamp(),
    PORTFOLIO_ID    number not null,
    RUN_ID          string not null,
    BRIEF           variant not null,
    PIPELINE_RUN_ID string,
    constraint PK_MORNING_BRIEF primary key (BRIEF_ID),
    constraint UQ_MORNING_BRIEF_PORTFOLIO_RUN unique (PORTFOLIO_ID, RUN_ID)
);

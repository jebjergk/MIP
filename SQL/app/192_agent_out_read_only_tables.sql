-- 192_agent_out_read_only_tables.sql
-- Purpose: Read-only agent output pipeline tables (agent-generated briefs and run log).
-- Existing MORNING_BRIEF (portfolio brief) unchanged; AGENT_MORNING_BRIEF holds agent-generated briefs.

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.AGENT_OUT;

-- ------------------------------------------------------------------------------
-- AGENT_MORNING_BRIEF: one row per agent-generated morning brief (read-only output)
-- ------------------------------------------------------------------------------
create table if not exists MIP.AGENT_OUT.AGENT_MORNING_BRIEF (
    BRIEF_ID       number        identity,
    AS_OF_TS       timestamp_ntz not null,
    SIGNAL_RUN_ID  number        not null,
    AGENT_NAME     varchar(128)  not null default 'AGENT_V0_MORNING_BRIEF',
    STATUS         varchar(64)   not null,
    BRIEF_JSON     variant      not null,
    CREATED_AT     timestamp_ntz not null default current_timestamp(),
    constraint PK_AGENT_MORNING_BRIEF primary key (BRIEF_ID),
    constraint UQ_AGENT_MORNING_BRIEF_AS_OF_RUN_AGENT unique (AS_OF_TS, SIGNAL_RUN_ID, AGENT_NAME)
);

-- ------------------------------------------------------------------------------
-- AGENT_RUN_LOG: optional audit of agent runs (inputs, outputs, errors)
-- ------------------------------------------------------------------------------
create table if not exists MIP.AGENT_OUT.AGENT_RUN_LOG (
    RUN_ID          varchar(64)   not null,
    AGENT_NAME      varchar(128)  not null,
    AS_OF_TS        timestamp_ntz,
    SIGNAL_RUN_ID    number,
    STATUS          varchar(64)   not null,
    INPUTS_JSON     variant,
    OUTPUTS_JSON    variant,
    ERROR_MESSAGE   varchar(16384),
    CREATED_AT      timestamp_ntz not null default current_timestamp(),
    constraint PK_AGENT_RUN_LOG primary key (RUN_ID)
);

-- ------------------------------------------------------------------------------
-- Grants (consistent with bootstrap: read roles select, admin owns)
-- ------------------------------------------------------------------------------
grant select on all tables in schema MIP.AGENT_OUT to role MIP_APP_ROLE;
grant select on future tables in schema MIP.AGENT_OUT to role MIP_APP_ROLE;

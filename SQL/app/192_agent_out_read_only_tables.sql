-- 192_agent_out_read_only_tables.sql
-- Purpose: Read-only agent output pipeline tables (run log).
-- Agent-generated briefs are stored in MIP.AGENT_OUT.MORNING_BRIEF with PORTFOLIO_ID=0 (see SP_AGENT_GENERATE_MORNING_BRIEF).
-- AGENT_MORNING_BRIEF table was removed; use MORNING_BRIEF (PORTFOLIO_ID=0, RUN_ID = agent key) instead.

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.AGENT_OUT;

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
-- Grants: MIP_ADMIN_ROLE full control (ownership via bootstrap); MIP_APP_ROLE select + insert on agent tables
-- ------------------------------------------------------------------------------
grant select, insert on table MIP.AGENT_OUT.AGENT_RUN_LOG to role MIP_APP_ROLE;
grant select on all tables in schema MIP.AGENT_OUT to role MIP_APP_ROLE;
grant select on future tables in schema MIP.AGENT_OUT to role MIP_APP_ROLE;

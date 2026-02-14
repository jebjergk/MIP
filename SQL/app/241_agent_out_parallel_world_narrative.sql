-- 241_agent_out_parallel_world_narrative.sql
-- Purpose: AI-generated narrative for Parallel Worlds.
-- One row per (PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME).
-- NARRATIVE_TEXT: plain-text narrative from Snowflake Cortex COMPLETE().
-- NARRATIVE_JSON: structured headline + bullets parsed from the narrative.
-- MODEL_INFO: model name used for generation (audit trail).
-- SOURCE_FACTS_HASH: must match the snapshot's hash — proves narrative was grounded in facts.
-- MERGE key: (PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME) — reruns update, never duplicate.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.AGENT_OUT.PARALLEL_WORLD_NARRATIVE (
    NARRATIVE_ID        number identity,
    PORTFOLIO_ID        number        not null,
    AS_OF_TS            timestamp_ntz not null,
    RUN_ID              varchar(64)   not null,
    AGENT_NAME          varchar(128)  not null default 'PARALLEL_WORLDS',
    NARRATIVE_TEXT       string,
    NARRATIVE_JSON       variant,
    MODEL_INFO           varchar(256),
    SOURCE_FACTS_HASH    varchar(64),
    CREATED_AT           timestamp_ntz default current_timestamp(),

    constraint PK_PW_NARRATIVE primary key (NARRATIVE_ID),
    constraint UQ_PW_NARRATIVE unique (PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME)
);

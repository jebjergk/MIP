-- 240_agent_out_parallel_world_snapshot.sql
-- Purpose: Persisted snapshot for Parallel Worlds narrative generation.
-- One row per (PORTFOLIO_ID, AS_OF_TS, RUN_ID).
-- SNAPSHOT_JSON contains the assembled facts from V_PARALLEL_WORLD_SNAPSHOT.
-- SOURCE_FACTS_HASH = SHA2 of the serialised snapshot for auditing and narrative grounding.
-- MERGE key: (PORTFOLIO_ID, AS_OF_TS, RUN_ID) — reruns update, never duplicate.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.AGENT_OUT.PARALLEL_WORLD_SNAPSHOT (
    SNAPSHOT_ID         number identity,
    PORTFOLIO_ID        number        not null,
    AS_OF_TS            timestamp_ntz not null,
    RUN_ID              varchar(64)   not null,
    SNAPSHOT_JSON       variant       not null,
    SOURCE_FACTS_HASH   varchar(64),
    CREATED_AT          timestamp_ntz default current_timestamp(),

    constraint PK_PW_SNAPSHOT primary key (SNAPSHOT_ID),
    constraint UQ_PW_SNAPSHOT unique (PORTFOLIO_ID, AS_OF_TS, RUN_ID)
);

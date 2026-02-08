-- 200_agent_out_daily_digest_snapshot.sql
-- Purpose: Deterministic state snapshot for the Daily Intelligence Digest.
-- One row per (SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID).
-- SCOPE = 'PORTFOLIO' (per-portfolio) or 'GLOBAL' (system-wide, PORTFOLIO_ID = NULL).
-- SNAPSHOT_JSON contains facts, counters, deltas, and fired interest detectors.
-- SOURCE_FACTS_HASH = SHA2 of the serialised snapshot for auditing and narrative grounding.
-- MERGE key: (SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID) — reruns update, never duplicate.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT (
    SNAPSHOT_ID         number identity,
    SCOPE               varchar(16)     default 'PORTFOLIO',
    PORTFOLIO_ID        number,
    AS_OF_TS            timestamp_ntz   not null,
    RUN_ID              varchar(64)     not null,
    SNAPSHOT_JSON       variant         not null,
    SOURCE_FACTS_HASH   varchar(64),
    CREATED_AT          timestamp_ntz   default current_timestamp(),

    constraint PK_DIGEST_SNAPSHOT primary key (SNAPSHOT_ID),
    constraint UQ_DIGEST_SNAPSHOT unique (SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID)
);

-- Migration safety: add columns if missing on existing deployments.
alter table MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT add column if not exists CREATED_AT timestamp_ntz;
alter table MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT add column if not exists SCOPE varchar(16) default 'PORTFOLIO';

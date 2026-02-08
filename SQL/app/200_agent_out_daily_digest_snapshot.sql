-- 200_agent_out_daily_digest_snapshot.sql
-- Purpose: Deterministic state snapshot for the Daily Intelligence Digest.
-- One row per (PORTFOLIO_ID, AS_OF_TS, RUN_ID).
-- SNAPSHOT_JSON contains facts, counters, deltas, and fired interest detectors.
-- SOURCE_FACTS_HASH = SHA2 of the serialised snapshot for auditing and narrative grounding.
-- MERGE key: (PORTFOLIO_ID, AS_OF_TS, RUN_ID) — reruns update, never duplicate.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT (
    SNAPSHOT_ID         number identity,
    PORTFOLIO_ID        number          not null,
    AS_OF_TS            timestamp_ntz   not null,
    RUN_ID              varchar(64)     not null,
    SNAPSHOT_JSON       variant         not null,
    SOURCE_FACTS_HASH   varchar(64),
    CREATED_AT          timestamp_ntz   default current_timestamp(),

    constraint PK_DIGEST_SNAPSHOT primary key (SNAPSHOT_ID),
    constraint UQ_DIGEST_SNAPSHOT unique (PORTFOLIO_ID, AS_OF_TS, RUN_ID)
);

-- Migration safety: add columns if missing on existing deployments.
alter table MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT add column if not exists CREATED_AT timestamp_ntz;

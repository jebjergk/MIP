-- 210_agent_out_training_digest_snapshot.sql
-- Purpose: Deterministic training state snapshot for the Training Journey Digest.
-- One row per (SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID).
-- SCOPE = 'GLOBAL_TRAINING' (system-wide) or 'SYMBOL_TRAINING' (per-symbol).
-- SNAPSHOT_JSON contains training facts, maturity scores, threshold gaps, detectors.
-- SOURCE_FACTS_HASH = SHA2 of the serialised snapshot for auditing.
-- MERGE key: (SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID) — idempotent.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT (
    SNAPSHOT_ID         number identity,
    SCOPE               varchar(32)     not null,     -- GLOBAL_TRAINING | SYMBOL_TRAINING
    SYMBOL              varchar(32),                   -- NULL for GLOBAL_TRAINING
    MARKET_TYPE         varchar(32),                   -- NULL for GLOBAL_TRAINING
    AS_OF_TS            timestamp_ntz   not null,
    RUN_ID              varchar(64)     not null,
    SNAPSHOT_JSON       variant         not null,
    SOURCE_FACTS_HASH   varchar(64),
    CREATED_AT          timestamp_ntz   default current_timestamp(),

    constraint PK_TRAINING_DIGEST_SNAPSHOT primary key (SNAPSHOT_ID),
    constraint UQ_TRAINING_DIGEST_SNAPSHOT unique (SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID)
);

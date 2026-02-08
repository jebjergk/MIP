-- 211_agent_out_training_digest_narrative.sql
-- Purpose: AI-generated narrative for the Training Journey Digest.
-- One row per (SCOPE, SYMBOL, MARKET_TYPE, PATTERN_ID, AS_OF_TS, RUN_ID, AGENT_NAME).
-- SCOPE = 'GLOBAL_TRAINING' (system-wide) or 'SYMBOL_TRAINING' (per-symbol per-pattern).
-- NARRATIVE_TEXT: plain-text narrative from Snowflake Cortex COMPLETE().
-- NARRATIVE_JSON: structured headline + bullets parsed from the narrative.
-- MODEL_INFO: model name used for generation (audit trail).
-- SOURCE_FACTS_HASH: must match the snapshot's hash — proves grounding.
-- MERGE key: (SCOPE, SYMBOL, MARKET_TYPE, PATTERN_ID, AS_OF_TS, RUN_ID, AGENT_NAME) — idempotent.
--
-- MIGRATION (existing installations):
--   ALTER TABLE MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE ADD COLUMN PATTERN_ID NUMBER;
--   ALTER TABLE MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE DROP CONSTRAINT UQ_TRAINING_DIGEST_NARRATIVE;
--   ALTER TABLE MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE ADD CONSTRAINT UQ_TRAINING_DIGEST_NARRATIVE
--       UNIQUE (SCOPE, SYMBOL, MARKET_TYPE, PATTERN_ID, AS_OF_TS, RUN_ID, AGENT_NAME);
-- Or simply: DROP TABLE MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE; then rerun this DDL + pipeline.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE (
    NARRATIVE_ID        number identity,
    SCOPE               varchar(32)     not null,     -- GLOBAL_TRAINING | SYMBOL_TRAINING
    SYMBOL              varchar(32),                   -- NULL for GLOBAL_TRAINING
    MARKET_TYPE         varchar(32),                   -- NULL for GLOBAL_TRAINING
    PATTERN_ID          number,                        -- NULL for GLOBAL_TRAINING; pattern_id for SYMBOL_TRAINING
    AS_OF_TS            timestamp_ntz   not null,
    RUN_ID              varchar(64)     not null,
    AGENT_NAME          varchar(128)    not null default 'TRAINING_DIGEST',
    NARRATIVE_TEXT       string,
    NARRATIVE_JSON       variant,
    MODEL_INFO           varchar(256),
    SOURCE_FACTS_HASH    varchar(64),
    CREATED_AT           timestamp_ntz   default current_timestamp(),

    constraint PK_TRAINING_DIGEST_NARRATIVE primary key (NARRATIVE_ID),
    constraint UQ_TRAINING_DIGEST_NARRATIVE unique (SCOPE, SYMBOL, MARKET_TYPE, PATTERN_ID, AS_OF_TS, RUN_ID, AGENT_NAME)
);

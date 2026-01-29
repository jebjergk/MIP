-- 185c_alter_morning_brief_run_id_and_merge_key.sql
-- Purpose: Align MORNING_BRIEF to RUN_ID (UUID), MERGE key (AS_OF_TS, RUN_ID, AGENT_NAME). Run once on existing DBs.

use role MIP_ADMIN_ROLE;
use database MIP;

-- RUN_ID nullable for backwards compat (if column already exists as NOT NULL, allow null)
alter table MIP.AGENT_OUT.MORNING_BRIEF add column if not exists RUN_ID varchar(64);
alter table MIP.AGENT_OUT.MORNING_BRIEF alter column RUN_ID drop not null;  -- no-op or makes nullable

-- Add MERGE-key unique; drop old (PORTFOLIO_ID, RUN_ID) if present so agent can have multiple rows per run_id (different AS_OF_TS)
alter table MIP.AGENT_OUT.MORNING_BRIEF drop constraint if exists UQ_MORNING_BRIEF_PORTFOLIO_RUN;
alter table MIP.AGENT_OUT.MORNING_BRIEF add constraint UQ_MORNING_BRIEF_AS_OF_RUN_AGENT unique (PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME);

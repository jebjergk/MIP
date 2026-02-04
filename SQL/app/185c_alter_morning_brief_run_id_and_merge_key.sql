-- 185c_alter_morning_brief_run_id_and_merge_key.sql
-- Purpose: Align MORNING_BRIEF to RUN_ID (UUID), MERGE key (AS_OF_TS, RUN_ID, AGENT_NAME). Run once on existing DBs.
-- Fixes: "Numeric value '...' is not recognized" when pipeline compares PIPELINE_RUN_ID (NUMBER) to UUID string.

use role MIP_ADMIN_ROLE;
use database MIP;

-- PIPELINE_RUN_ID and RUN_ID must be varchar so pipeline UUID can be stored/compared (were NUMBER in some older deploys).
-- Fixes: "Numeric value 'a022327d-...' is not recognized" when pipeline/agent passes UUID.
alter table MIP.AGENT_OUT.MORNING_BRIEF alter column PIPELINE_RUN_ID set data type varchar(64);
alter table MIP.AGENT_OUT.MORNING_BRIEF add column if not exists RUN_ID varchar(64);
alter table MIP.AGENT_OUT.MORNING_BRIEF alter column RUN_ID set data type varchar(64);  -- in case RUN_ID existed as number
alter table MIP.AGENT_OUT.MORNING_BRIEF alter column RUN_ID drop not null;  -- no-op or makes nullable

-- Add MERGE-key unique; drop old (PORTFOLIO_ID, RUN_ID) if present so agent can have multiple rows per run_id (different AS_OF_TS)
alter table MIP.AGENT_OUT.MORNING_BRIEF drop constraint UQ_MORNING_BRIEF_PORTFOLIO_RUN;
alter table MIP.AGENT_OUT.MORNING_BRIEF add constraint UQ_MORNING_BRIEF_AS_OF_RUN_AGENT unique (PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME);

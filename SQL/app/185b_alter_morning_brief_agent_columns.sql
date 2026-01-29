-- 185b_alter_morning_brief_agent_columns.sql
-- Purpose: Add agent v0 columns to MORNING_BRIEF for RUN_ID (UUID)-keyed agent briefs. Run once on existing DBs.

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.AGENT_OUT.MORNING_BRIEF add column if not exists AGENT_NAME varchar(128);
alter table MIP.AGENT_OUT.MORNING_BRIEF add column if not exists STATUS varchar(64);
alter table MIP.AGENT_OUT.MORNING_BRIEF add column if not exists BRIEF_JSON variant;
alter table MIP.AGENT_OUT.MORNING_BRIEF add column if not exists CREATED_AT timestamp_ntz;
alter table MIP.AGENT_OUT.MORNING_BRIEF add column if not exists SIGNAL_RUN_ID varchar(64);

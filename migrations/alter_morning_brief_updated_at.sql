-- alter_morning_brief_updated_at.sql
-- Purpose: Add UPDATED_AT to MORNING_BRIEF for idempotent MERGE (WHEN MATCHED updates).

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.AGENT_OUT.MORNING_BRIEF add column if not exists UPDATED_AT timestamp_ntz;

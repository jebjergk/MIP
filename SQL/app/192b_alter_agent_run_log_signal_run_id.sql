-- 192b_alter_agent_run_log_signal_run_id.sql
-- Purpose: Change SIGNAL_RUN_ID from number to varchar(64) so pipeline run id (UUID) can be stored. Run once on existing DBs.

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.AGENT_OUT.AGENT_RUN_LOG alter column SIGNAL_RUN_ID set data type varchar(64);

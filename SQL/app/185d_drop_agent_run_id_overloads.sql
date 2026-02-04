-- 185d_drop_agent_run_id_overloads.sql
-- Purpose: Drop wrong overloads of agent procedures so pipeline UUID binds to VARCHAR (not NUMBER/VARIANT).
-- The pipeline calls SP_AGENT_RUN_ALL(:v_effective_to_ts, :v_run_id) with v_run_id = UUID string.
-- If SP_AGENT_RUN_ALL has (TIMESTAMP_NTZ, NUMBER), Snowflake picks it and fails converting UUID to number.
-- SP_AGENT_GENERATE_MORNING_BRIEF works when called directly because you call it with varchar; the pipeline goes through SP_AGENT_RUN_ALL.
--
-- Run once in Snowflake, then redeploy 193 and 195 so the (VARCHAR) version exists.

use role MIP_ADMIN_ROLE;
use database MIP;

-- List current overloads (run this first to see what exists):
-- show procedures like 'SP_AGENT%' in schema MIP.APP;

-- SP_AGENT_GENERATE_MORNING_BRIEF: keep only (TIMESTAMP_NTZ, VARCHAR). Drop NUMBER and VARIANT for second arg.
drop procedure if exists MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF(timestamp_ntz, number);
drop procedure if exists MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF(timestamp_ntz, variant);

-- SP_AGENT_RUN_ALL: keep only (TIMESTAMP_NTZ, VARCHAR). This is the one the pipeline calls with UUID.
drop procedure if exists MIP.APP.SP_AGENT_RUN_ALL(timestamp_ntz, number);
drop procedure if exists MIP.APP.SP_AGENT_RUN_ALL(timestamp_ntz, variant);

-- If Snowflake reports different type names, run: show procedures like 'SP_AGENT_RUN_ALL%' in schema MIP.APP;
-- and drop by the exact "Arguments" signature, e.g. drop procedure ... (TIMESTAMP_NTZ(9), NUMBER(38,0));

-- After this script, deploy so the correct overload exists:
--   MIP/SQL/app/193_sp_agent_generate_morning_brief.sql
--   MIP/SQL/app/195_sp_agent_run_all.sql
--
-- If the pipeline still fails with "Numeric value '...' is not recognized" and both procedures
-- already have (TIMESTAMP_NTZ, VARCHAR) and only one overload each, the cause is likely
-- AGENT_RUN_LOG.SIGNAL_RUN_ID still being NUMBER (the procedure inserts the UUID there).
-- Run: MIP/SQL/app/192b_alter_agent_run_log_signal_run_id.sql
--   (alter AGENT_RUN_LOG.SIGNAL_RUN_ID to varchar(64))

-- 407_drop_sim_execution_surface.sql
-- Purpose: Remove SIM execution SQL surfaces in IB-only operating mode.

use role MIP_ADMIN_ROLE;
use database MIP;

alter task if exists MIP.APP.TASK_RUN_SIM_OPEN_EXECUTION suspend;
drop task if exists MIP.APP.TASK_RUN_SIM_OPEN_EXECUTION;

drop procedure if exists MIP.APP.SP_RUN_SIM_OPEN_EXECUTION(number, string);
drop procedure if exists MIP.APP.SP_VALIDATE_AND_EXECUTE_PROPOSALS(number, string, string);

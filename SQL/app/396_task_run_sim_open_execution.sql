-- 396_task_run_sim_open_execution.sql
-- Purpose: Run simulation open-session committee+execution loop on weekdays.
-- Created suspended by default.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.APP.TASK_RUN_SIM_OPEN_EXECUTION
    warehouse = MIP_WH_XS
    schedule = 'USING CRON */5 9-15 * * MON-FRI America/New_York'
    user_task_timeout_ms = 300000
    comment = 'Simulation open executor: run committee+execution without human approval during market session.'
as
    call MIP.APP.SP_RUN_SIM_OPEN_EXECUTION(null, null);

alter task MIP.APP.TASK_RUN_SIM_OPEN_EXECUTION suspend;

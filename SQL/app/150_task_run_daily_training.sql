-- 150_task_run_daily_training.sql
-- Purpose: Schedule daily pipeline execution

use role accountadmin;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE MIP_ADMIN_ROLE;

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.APP.TASK_RUN_DAILY_PIPELINE
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 0 5 * * TUE-SAT Europe/Berlin'
as
    call MIP.APP.SP_RUN_DAILY_PIPELINE();

alter task MIP.APP.TASK_RUN_DAILY_PIPELINE resume;

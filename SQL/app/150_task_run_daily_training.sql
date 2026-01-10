-- 150_task_run_daily_training.sql
-- Purpose: Schedule daily training loop execution

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.APP.TASK_RUN_DAILY_TRAINING
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 0 7 * * * Europe/Berlin'
as
    call MIP.APP.SP_RUN_DAILY_TRAINING();

alter task MIP.APP.TASK_RUN_DAILY_TRAINING resume;

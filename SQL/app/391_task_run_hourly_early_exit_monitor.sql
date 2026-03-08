-- 391_task_run_hourly_early_exit_monitor.sql
-- Purpose: Hourly task for early-exit monitoring on daily positions within RTH window.
-- Runs ingest + early-exit evaluation via SP_RUN_HOURLY_EARLY_EXIT_MONITOR.
-- Created suspended by default.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.APP.TASK_RUN_HOURLY_EARLY_EXIT_MONITOR
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 5 10-15 * * MON-FRI America/New_York'
    user_task_timeout_ms = 300000
    comment = 'Hourly early-exit monitor: ingest configured bars and evaluate daily-position early exits.'
as
    call MIP.APP.SP_RUN_HOURLY_EARLY_EXIT_MONITOR();

alter task MIP.APP.TASK_RUN_HOURLY_EARLY_EXIT_MONITOR suspend;

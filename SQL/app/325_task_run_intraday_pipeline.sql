-- 325_task_run_intraday_pipeline.sql
-- Purpose: Snowflake task for hourly intraday pipeline execution.
-- CREATED SUSPENDED — only resume after activating the intraday subsystem.
-- Schedule: every hour during US market hours (14:30-21:00 UTC = 9:30am-4pm ET).
-- Mon-Fri only. Berlin timezone used for consistency with daily task.
-- The pipeline itself checks INTRADAY_ENABLED and exits gracefully if disabled.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.APP.TASK_RUN_INTRADAY_PIPELINE
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 30 15-21 * * MON-FRI Europe/Berlin'
    comment = 'Intraday learning pipeline: ingest hourly bars, detect patterns, evaluate outcomes. Feature-flagged via INTRADAY_ENABLED.'
    user_task_timeout_ms = 300000
as
    call MIP.APP.SP_RUN_INTRADAY_PIPELINE();

-- SUSPENDED by default — do not resume until intraday subsystem is activated.
alter task MIP.APP.TASK_RUN_INTRADAY_PIPELINE suspend;

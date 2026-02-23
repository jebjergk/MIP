-- 325_task_run_intraday_pipeline.sql
-- Purpose: Snowflake task for 15-minute intraday pipeline execution.
-- CREATED SUSPENDED — only resume after activating the intraday subsystem.
-- Schedule: every 15 minutes during NASDAQ hours (9:30am-4pm ET = 15:30-22:00 Berlin).
-- Runs at :02, :17, :32, :47 past the hour (2-min offset for bar data to settle).
-- Mon-Fri only. Berlin timezone used for consistency with daily task.
-- The pipeline itself checks INTRADAY_ENABLED and exits gracefully if disabled.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.APP.TASK_RUN_INTRADAY_PIPELINE
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 2,17,32,47 15-22 * * MON-FRI Europe/Berlin'
    comment = 'Intraday pipeline: ingest 15-min bars, detect patterns, evaluate early exits. Runs every 15 min during NASDAQ hours. Feature-flagged via INTRADAY_ENABLED.'
    user_task_timeout_ms = 300000
as
    call MIP.APP.SP_RUN_INTRADAY_PIPELINE();

-- SUSPENDED by default — do not resume until intraday subsystem is activated.
alter task MIP.APP.TASK_RUN_INTRADAY_PIPELINE suspend;

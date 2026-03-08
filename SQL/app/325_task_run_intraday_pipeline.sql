-- 325_task_run_intraday_pipeline.sql
-- Purpose: Snowflake task for 15-minute intraday pipeline execution.
-- INTRADAY PIPELINE IS CURRENTLY ON HOLD DUE TO COST; keep suspended.
-- Schedule: every 15 minutes during regular-session core window (10:00-15:47 ET).
-- Runs at :02, :17, :32, :47 past the hour (2-min offset for bar data to settle).
-- Mon-Fri only, in America/New_York timezone.
-- The pipeline itself checks INTRADAY_ENABLED and exits gracefully if disabled.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.APP.TASK_RUN_INTRADAY_PIPELINE
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 2,17,32,47 10-15 * * MON-FRI America/New_York'
    comment = 'Intraday pipeline (15-min) on hold for cost control. Keep suspended until explicitly reactivated. Feature-flagged via INTRADAY_ENABLED.'
    user_task_timeout_ms = 300000
as
    call MIP.APP.SP_RUN_INTRADAY_PIPELINE();

-- SUSPENDED by default — do not resume until intraday subsystem is activated.
alter task MIP.APP.TASK_RUN_INTRADAY_PIPELINE suspend;

-- 374_task_news_ingest.sql
-- Purpose: Weekday scheduled news context refresh task.
-- Schedule: every 30 minutes during weekday pre-open+RTH core window (08:00-15:30 ET).
-- Created suspended by default.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.NEWS.TASK_INGEST_RSS_NEWS
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 0,30 8-15 * * MON-FRI America/New_York'
    suspend_task_after_num_failures = 0
    user_task_timeout_ms = 900000
    comment = 'News context refresh task (ingest+map+compute). Runs every 30 minutes from 08:00 to 15:30 ET on Mon-Fri.'
as
    call MIP.NEWS.SP_REFRESH_NEWS_CONTEXT(null);

alter task MIP.NEWS.TASK_INGEST_RSS_NEWS suspend;

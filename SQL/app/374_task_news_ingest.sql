-- 374_task_news_ingest.sql
-- Purpose: Weekday scheduled news context refresh task.
-- Schedule: every 30 minutes (Mon-Fri).
-- Created suspended by default.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.NEWS.TASK_INGEST_RSS_NEWS
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 0,30 * * * MON-FRI Europe/Berlin'
    user_task_timeout_ms = 300000
    comment = 'News context refresh task (ingest+map+compute). Runs every 30 minutes Mon-Fri.'
as
    call MIP.NEWS.SP_REFRESH_NEWS_CONTEXT(null);

alter task MIP.NEWS.TASK_INGEST_RSS_NEWS suspend;

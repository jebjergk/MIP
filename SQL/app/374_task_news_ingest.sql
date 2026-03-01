-- 374_task_news_ingest.sql
-- Purpose: Phase 2 scheduled RSS ingestion task.
-- Schedule: every 30 minutes.
-- Created suspended by default.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.NEWS.TASK_INGEST_RSS_NEWS
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 0,30 * * * * Europe/Berlin'
    user_task_timeout_ms = 300000
    comment = 'News RSS ingestion task (context-only). Runs every 30 minutes.'
as
    call MIP.NEWS.SP_INGEST_RSS_NEWS(false, null);

alter task MIP.NEWS.TASK_INGEST_RSS_NEWS suspend;

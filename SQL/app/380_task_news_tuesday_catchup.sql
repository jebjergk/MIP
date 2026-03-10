-- 380_task_news_tuesday_catchup.sql
-- Purpose: Weekday pre-open kickoff refresh before market open.
-- Schedule: Weekdays 07:30 America/New_York.
-- Created suspended by default.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.NEWS.TASK_NEWS_TUESDAY_CATCHUP
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 30 7 * * MON-FRI America/New_York'
    suspend_task_after_num_failures = 0
    user_task_timeout_ms = 600000
    comment = 'Weekday pre-open kickoff refresh for decision-time news context.'
as
    call MIP.NEWS.SP_REFRESH_NEWS_CONTEXT(null);

alter task MIP.NEWS.TASK_NEWS_TUESDAY_CATCHUP suspend;

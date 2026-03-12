-- 399_task_news_precommittee_0900.sql
-- Purpose: Final pre-committee news refresh at 09:00 ET.
-- Created suspended by default.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.NEWS.TASK_NEWS_PRECOMMITTEE_0900
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 0 9 * * MON-FRI America/New_York'
    suspend_task_after_num_failures = 0
    user_task_timeout_ms = 900000
    comment = 'Final pre-committee news refresh at 09:00 ET on weekdays.'
as
    call MIP.NEWS.SP_REFRESH_NEWS_CONTEXT(null);

alter task MIP.NEWS.TASK_NEWS_PRECOMMITTEE_0900 suspend;

-- 380_task_news_tuesday_catchup.sql
-- Purpose: Tuesday pre-market catch-up refresh after weekend suspension.
-- Schedule: Tuesday 05:45 Europe/Berlin.
-- Created suspended by default.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.NEWS.TASK_NEWS_TUESDAY_CATCHUP
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 45 5 * * TUE Europe/Berlin'
    user_task_timeout_ms = 600000
    comment = 'Tuesday pre-market catch-up refresh for decision-time news context.'
as
    call MIP.NEWS.SP_REFRESH_NEWS_CONTEXT(null);

alter task MIP.NEWS.TASK_NEWS_TUESDAY_CATCHUP suspend;

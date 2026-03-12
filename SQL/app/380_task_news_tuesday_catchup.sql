-- 380_task_news_tuesday_catchup.sql
-- Purpose: Retire legacy catchup task.

use role MIP_ADMIN_ROLE;
use database MIP;

drop task if exists MIP.NEWS.TASK_NEWS_TUESDAY_CATCHUP;

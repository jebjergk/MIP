-- 378_task_news_info_state_daily.sql
-- Purpose: Phase 4 daily rollup task for news info-state metrics.
-- Created suspended by default.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace task MIP.NEWS.TASK_COMPUTE_NEWS_INFO_STATE_DAILY
    warehouse = MIP_WH_XS
    schedule = 'USING CRON 0 16 * * MON-FRI America/New_York'
    user_task_timeout_ms = 300000
    comment = 'Daily deterministic news info-state rollup for decision-time context at end of regular session.'
as
    call MIP.NEWS.SP_COMPUTE_INFO_STATE_DAILY(current_timestamp(), null);

alter task MIP.NEWS.TASK_COMPUTE_NEWS_INFO_STATE_DAILY suspend;

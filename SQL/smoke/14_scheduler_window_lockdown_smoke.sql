use role MIP_ADMIN_ROLE;
use database MIP;

-- Capture current task configuration.
show tasks in database MIP;

-- Smoke 1: targeted tasks exist and have expected schedules.
select
  'SCHED_EXPECTED' as check_name,
  "name" as NAME,
  "state" as STATE,
  "schedule" as SCHEDULE,
  iff(
    ("name" = 'TASK_RUN_DAILY_PIPELINE' and "schedule" = 'USING CRON 0 17 * * MON-FRI America/New_York') or
    ("name" = 'TASK_RUN_INTRADAY_PIPELINE' and "schedule" = 'USING CRON 2,17,32,47 10-15 * * MON-FRI America/New_York') or
    ("name" = 'TASK_RUN_HOURLY_EARLY_EXIT_MONITOR' and "schedule" = 'USING CRON 5 10-15 * * MON-FRI America/New_York') or
    ("name" = 'TASK_INGEST_RSS_NEWS' and "schedule" = 'USING CRON 0,30 8-15 * * MON-FRI America/New_York') or
    ("name" = 'TASK_NEWS_TUESDAY_CATCHUP' and "schedule" = 'USING CRON 30 7 * * MON-FRI America/New_York') or
    ("name" = 'TASK_COMPUTE_NEWS_INFO_STATE_DAILY' and "schedule" = 'USING CRON 0 16 * * MON-FRI America/New_York'),
    'PASS',
    'FAIL'
  ) as schedule_check
from table(result_scan(last_query_id()))
where "database_name" = 'MIP'
  and ("schema_name" = 'APP' or "schema_name" = 'NEWS')
  and "name" in (
    'TASK_RUN_DAILY_PIPELINE',
    'TASK_RUN_INTRADAY_PIPELINE',
    'TASK_RUN_HOURLY_EARLY_EXIT_MONITOR',
    'TASK_INGEST_RSS_NEWS',
    'TASK_NEWS_TUESDAY_CATCHUP',
    'TASK_COMPUTE_NEWS_INFO_STATE_DAILY'
  )
order by "schema_name", "name";

-- Smoke 2: intraday 15-minute task must remain suspended while intraday trading/training is on hold.
select
  'INTRADAY_TASK_MUST_BE_SUSPENDED' as check_name,
  count_if(NAME = 'TASK_RUN_INTRADAY_PIPELINE' and STATE <> 'suspended') as cnt
from table(result_scan(last_query_id(-1)));

-- Smoke 3: no started task is outside approved policy strings.
select
  'STARTED_TASK_OUT_OF_POLICY_COUNT' as check_name,
  count(*) as cnt
from table(result_scan(last_query_id(-2)))
where STATE = 'started'
  and (
    (NAME in ('TASK_RUN_DAILY_PIPELINE', 'TASK_RUN_HOURLY_EARLY_EXIT_MONITOR')
      and SCHEDULE not in (
        'USING CRON 0 17 * * MON-FRI America/New_York',
        'USING CRON 5 10-15 * * MON-FRI America/New_York'
      )
    ) or
    (NAME in ('TASK_INGEST_RSS_NEWS', 'TASK_NEWS_TUESDAY_CATCHUP', 'TASK_COMPUTE_NEWS_INFO_STATE_DAILY')
      and SCHEDULE not in (
        'USING CRON 0,30 8-15 * * MON-FRI America/New_York',
        'USING CRON 30 7 * * MON-FRI America/New_York',
        'USING CRON 0 16 * * MON-FRI America/New_York'
      )
    )
  );


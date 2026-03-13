-- v_news_feed_health.sql
-- Purpose: Committee-window source stability monitor for news ingestion.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_NEWS_FEED_HEALTH as
with cfg as (
    select
        coalesce(
            max(try_to_number(case when CONFIG_KEY = 'NEWS_FEED_HEALTH_STALE_MINUTES' then CONFIG_VALUE end)),
            90
        ) as STALE_MINUTES
    from MIP.APP.APP_CONFIG
    where CONFIG_KEY in ('NEWS_FEED_HEALTH_STALE_MINUTES')
),
slots as (
    select column1 as SLOT_LABEL
    from values ('07:00'), ('07:30'), ('08:00'), ('08:30'), ('09:00')
),
raw_today as (
    select
        r.SOURCE_ID,
        convert_timezone('UTC', 'America/New_York', r.INGESTED_AT) as INGESTED_AT_ET,
        r.INGESTED_AT,
        r.SYMBOL_HINT,
        case
            when date_part(hour, convert_timezone('UTC', 'America/New_York', r.INGESTED_AT)) = 7
                 and date_part(minute, convert_timezone('UTC', 'America/New_York', r.INGESTED_AT)) < 30 then '07:00'
            when date_part(hour, convert_timezone('UTC', 'America/New_York', r.INGESTED_AT)) = 7 then '07:30'
            when date_part(hour, convert_timezone('UTC', 'America/New_York', r.INGESTED_AT)) = 8
                 and date_part(minute, convert_timezone('UTC', 'America/New_York', r.INGESTED_AT)) < 30 then '08:00'
            when date_part(hour, convert_timezone('UTC', 'America/New_York', r.INGESTED_AT)) = 8 then '08:30'
            when date_part(hour, convert_timezone('UTC', 'America/New_York', r.INGESTED_AT)) = 9 then '09:00'
            else null
        end as ROUND_SLOT
    from MIP.NEWS.NEWS_RAW r
    where cast(convert_timezone('UTC', 'America/New_York', r.INGESTED_AT) as date) = cast(convert_timezone('America/New_York', current_timestamp()) as date)
      and cast(convert_timezone('UTC', 'America/New_York', r.INGESTED_AT) as time) >= to_time('07:00:00')
      and cast(convert_timezone('UTC', 'America/New_York', r.INGESTED_AT) as time) < to_time('09:30:00')
),
agg as (
    select
        SOURCE_ID,
        count(*) as ENTRIES_TODAY,
        count(distinct iff(SYMBOL_HINT is not null and trim(SYMBOL_HINT) <> '', upper(SYMBOL_HINT), null)) as SYMBOLS_COVERED_TODAY,
        max(INGESTED_AT) as LAST_INGESTED_AT_UTC,
        max(INGESTED_AT_ET) as LAST_INGESTED_AT_ET,
        count(distinct ROUND_SLOT) as ROUNDS_WITH_DATA,
        max(iff(ROUND_SLOT = '07:00', 1, 0)) as HAS_0700,
        max(iff(ROUND_SLOT = '07:30', 1, 0)) as HAS_0730,
        max(iff(ROUND_SLOT = '08:00', 1, 0)) as HAS_0800,
        max(iff(ROUND_SLOT = '08:30', 1, 0)) as HAS_0830,
        max(iff(ROUND_SLOT = '09:00', 1, 0)) as HAS_0900
    from raw_today
    where ROUND_SLOT is not null
    group by SOURCE_ID
)
select
    cast(convert_timezone('America/New_York', current_timestamp()) as date) as ET_DATE,
    s.SOURCE_ID,
    s.SOURCE_NAME,
    upper(coalesce(s.SOURCE_TYPE, 'GLOBAL_RSS')) as SOURCE_TYPE,
    coalesce(s.ENABLED_FLAG, true) as ENABLED_FLAG,
    coalesce(s.POLL_MINUTES, s.INGEST_CADENCE_MINUTES, 30) as POLL_MINUTES,
    coalesce(a.ENTRIES_TODAY, 0) as ENTRIES_TODAY,
    coalesce(a.SYMBOLS_COVERED_TODAY, 0) as SYMBOLS_COVERED_TODAY,
    a.LAST_INGESTED_AT_UTC,
    a.LAST_INGESTED_AT_ET,
    datediff('minute', a.LAST_INGESTED_AT_UTC, current_timestamp()) as LAST_INGEST_AGE_MINUTES,
    5 as ROUNDS_EXPECTED,
    coalesce(a.ROUNDS_WITH_DATA, 0) as ROUNDS_WITH_DATA,
    coalesce(a.ROUNDS_WITH_DATA, 0) / 5.0 as ROUND_SUCCESS_RATE,
    iff(coalesce(a.HAS_0700, 0) = 1, true, false) as ROUND_0700_OK,
    iff(coalesce(a.HAS_0730, 0) = 1, true, false) as ROUND_0730_OK,
    iff(coalesce(a.HAS_0800, 0) = 1, true, false) as ROUND_0800_OK,
    iff(coalesce(a.HAS_0830, 0) = 1, true, false) as ROUND_0830_OK,
    iff(coalesce(a.HAS_0900, 0) = 1, true, false) as ROUND_0900_OK,
    array_construct_compact(
        iff(coalesce(a.HAS_0700, 0) = 0, '07:00', null),
        iff(coalesce(a.HAS_0730, 0) = 0, '07:30', null),
        iff(coalesce(a.HAS_0800, 0) = 0, '08:00', null),
        iff(coalesce(a.HAS_0830, 0) = 0, '08:30', null),
        iff(coalesce(a.HAS_0900, 0) = 0, '09:00', null)
    ) as MISSING_ROUNDS,
    cfg.STALE_MINUTES as STALE_THRESHOLD_MINUTES,
    iff(
        a.LAST_INGESTED_AT_UTC is null,
        true,
        datediff('minute', a.LAST_INGESTED_AT_UTC, current_timestamp()) > cfg.STALE_MINUTES
    ) as IS_STALE,
    case
        when coalesce(a.ROUNDS_WITH_DATA, 0) = 0 then 'CRITICAL'
        when coalesce(a.ROUNDS_WITH_DATA, 0) <= 2 then 'DEGRADED'
        when (
            a.LAST_INGESTED_AT_UTC is null
            or datediff('minute', a.LAST_INGESTED_AT_UTC, current_timestamp()) > cfg.STALE_MINUTES
        ) then 'STALE'
        when coalesce(a.ROUNDS_WITH_DATA, 0) < 5 then 'WARN'
        else 'HEALTHY'
    end as HEALTH_STATUS
from MIP.NEWS.NEWS_SOURCE_REGISTRY s
cross join cfg
left join agg a
  on a.SOURCE_ID = s.SOURCE_ID
where s.ALLOWED_FLAG = true
  and s.IS_ACTIVE = true
  and coalesce(s.ENABLED_FLAG, true)
order by
    case HEALTH_STATUS
        when 'CRITICAL' then 1
        when 'DEGRADED' then 2
        when 'STALE' then 3
        when 'WARN' then 4
        else 5
    end,
    s.SOURCE_ID;

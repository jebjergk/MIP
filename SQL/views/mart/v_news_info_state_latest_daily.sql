-- v_news_info_state_latest_daily.sql
-- Purpose: Latest daily news context rows with staleness metadata.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_NEWS_INFO_STATE_LATEST_DAILY as
with cfg as (
    select
        coalesce(max(try_to_number(case when CONFIG_KEY = 'NEWS_STALENESS_THRESHOLD_MINUTES' then CONFIG_VALUE end)), 180) as STALENESS_MINUTES
    from MIP.APP.APP_CONFIG
),
latest as (
    select
        n.*,
        row_number() over (
            partition by n.AS_OF_DATE, n.SYMBOL, n.MARKET_TYPE
            order by n.SNAPSHOT_TS desc, n.CREATED_AT desc
        ) as RN
    from MIP.NEWS.NEWS_INFO_STATE_DAILY n
    where n.SNAPSHOT_TS <= current_timestamp()
)
select
    l.AS_OF_DATE,
    l.SYMBOL,
    l.MARKET_TYPE,
    l.NEWS_COUNT,
    l.UNIQUE_SOURCES,
    l.DEDUP_COUNT,
    l.NOVELTY_SCORE,
    l.BURST_SCORE,
    l.UNCERTAINTY_FLAG,
    l.TOP_HEADLINES,
    l.TRAILING_AVG_7D,
    l.TRAILING_STD_7D,
    l.Z_SCORE,
    l.NEWS_CONTEXT_BADGE,
    l.LAST_NEWS_PUBLISHED_AT,
    l.LAST_INGESTED_AT,
    l.SNAPSHOT_TS,
    datediff('minute', l.SNAPSHOT_TS, current_timestamp()) as NEWS_SNAPSHOT_AGE_MINUTES,
    iff(datediff('minute', l.SNAPSHOT_TS, current_timestamp()) > c.STALENESS_MINUTES, true, false) as NEWS_IS_STALE,
    c.STALENESS_MINUTES as NEWS_STALENESS_THRESHOLD_MINUTES,
    l.CREATED_AT,
    l.RUN_ID
from latest l
cross join cfg c
where l.RN = 1;

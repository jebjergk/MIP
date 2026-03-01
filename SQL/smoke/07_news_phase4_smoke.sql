-- 07_news_phase4_smoke.sql
-- Purpose: Phase 4 smoke for daily info-state metrics + latest view.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Deterministic test path.
call MIP.NEWS.SP_INGEST_RSS_NEWS(true, 3);
call MIP.NEWS.SP_MAP_NEWS_SYMBOLS(null);
set as_of_for_data = (
    select to_timestamp_tz(to_char(max(PUBLISHED_AT), 'YYYY-MM-DD HH24:MI:SS') || ' +00:00')
    from MIP.NEWS.NEWS_RAW
);
call MIP.NEWS.SP_COMPUTE_INFO_STATE_DAILY($as_of_for_data, null);

-- 1) Raw daily info-state rows.
select
    AS_OF_DATE,
    SYMBOL,
    MARKET_TYPE,
    NEWS_COUNT,
    UNIQUE_SOURCES,
    DEDUP_COUNT,
    NOVELTY_SCORE,
    BURST_SCORE,
    TRAILING_AVG_7D,
    TRAILING_STD_7D,
    Z_SCORE,
    NEWS_CONTEXT_BADGE,
    LAST_NEWS_PUBLISHED_AT,
    LAST_INGESTED_AT,
    SNAPSHOT_TS
from MIP.NEWS.NEWS_INFO_STATE_DAILY
order by SNAPSHOT_TS desc, SYMBOL, MARKET_TYPE
limit 100;

-- 2) Latest-daily MART view with stale flags.
select
    AS_OF_DATE,
    SYMBOL,
    MARKET_TYPE,
    NEWS_COUNT,
    NEWS_CONTEXT_BADGE,
    SNAPSHOT_TS,
    NEWS_SNAPSHOT_AGE_MINUTES,
    NEWS_IS_STALE,
    NEWS_STALENESS_THRESHOLD_MINUTES
from MIP.MART.V_NEWS_INFO_STATE_LATEST_DAILY
order by SNAPSHOT_TS desc, SYMBOL, MARKET_TYPE
limit 100;

-- 3) Midnight UTC boundary demonstration.
select
    convert_timezone('UTC', to_timestamp_tz('2026-03-01 23:59:59 +00:00'))::date as D_BEFORE_MIDNIGHT_UTC,
    convert_timezone('UTC', to_timestamp_tz('2026-03-02 00:00:01 +00:00'))::date as D_AFTER_MIDNIGHT_UTC;

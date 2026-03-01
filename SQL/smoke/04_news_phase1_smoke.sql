-- 04_news_phase1_smoke.sql
-- Purpose: Phase 1 smoke checks for News Context schema + config.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Table inventory.
select
    TABLE_SCHEMA,
    TABLE_NAME,
    ROW_COUNT
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'NEWS'
  and TABLE_NAME in (
      'NEWS_SOURCE_REGISTRY',
      'NEWS_RAW',
      'NEWS_SYMBOL_MAP',
      'NEWS_DEDUP',
      'NEWS_INFO_STATE_DAILY',
      'SYMBOL_ALIAS_DICT'
  )
order by TABLE_NAME;

-- 2) NEWS config values.
select
    CONFIG_KEY,
    CONFIG_VALUE,
    DESCRIPTION
from MIP.APP.APP_CONFIG
where CONFIG_KEY like 'NEWS_%'
order by CONFIG_KEY;

-- 3) v1 content contract check (informational).
select
    count(*) as full_text_non_null_rows
from MIP.NEWS.NEWS_RAW
where FULL_TEXT_OPTIONAL is not null;

-- 4) Freshness fields presence via simple projection.
select
    AS_OF_DATE,
    SYMBOL,
    MARKET_TYPE,
    LAST_NEWS_PUBLISHED_AT,
    LAST_INGESTED_AT,
    SNAPSHOT_TS
from MIP.NEWS.NEWS_INFO_STATE_DAILY
limit 5;

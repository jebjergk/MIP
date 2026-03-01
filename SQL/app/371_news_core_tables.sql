-- 371_news_core_tables.sql
-- Purpose: Phase 1 core News Context tables (daily context layer only).
-- Architecture contract:
--   - News is decision-time context only.
--   - No signal/training/trust objects may depend on MIP.NEWS.
-- Time contract:
--   - published_at and snapshot_ts are UTC.
--   - as_of_date for daily rows is derived from :as_of_ts in UTC.
-- Content contract (v1):
--   - full article text is not stored, only title/snippet/link.

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.NEWS;

create table if not exists MIP.NEWS.NEWS_RAW (
    NEWS_ID                 string         not null,
    SOURCE_ID               string         not null,
    SOURCE_NAME             string         not null,
    PUBLISHED_AT            timestamp_ntz  not null,
    INGESTED_AT             timestamp_ntz  not null default current_timestamp(),
    TITLE                   string,
    SUMMARY                 string,
    FULL_TEXT_OPTIONAL      string,
    URL                     string         not null,
    LANGUAGE                string,
    RAW_PAYLOAD_VARIANT     variant,
    CONTENT_HASH            string         not null,
    CANONICAL_URL_HASH      string         not null,
    DEDUP_CLUSTER_ID        string,
    PARSE_STATUS            string         not null default 'SUCCESS',
    ERROR_REASON            string,
    SNAPSHOT_TS             timestamp_ntz  not null,
    RUN_ID                  string,
    CREATED_AT              timestamp_ntz  not null default current_timestamp(),
    constraint PK_NEWS_RAW primary key (NEWS_ID)
);

create table if not exists MIP.NEWS.NEWS_SYMBOL_MAP (
    NEWS_ID                 string         not null,
    SYMBOL                  string         not null,
    MARKET_TYPE             string         not null,
    MATCH_METHOD            string         not null,
    MATCH_CONFIDENCE        number(6,5)   not null,
    CREATED_AT              timestamp_ntz  not null default current_timestamp(),
    RUN_ID                  string,
    constraint PK_NEWS_SYMBOL_MAP primary key (NEWS_ID, SYMBOL, MARKET_TYPE, MATCH_METHOD)
);

create table if not exists MIP.NEWS.NEWS_DEDUP (
    DEDUP_CLUSTER_ID        string         not null,
    REPRESENTATIVE_NEWS_ID  string         not null,
    CLUSTER_SIZE            number         not null,
    CLUSTER_FIRST_SEEN_AT   timestamp_ntz  not null,
    CLUSTER_LAST_SEEN_AT    timestamp_ntz  not null,
    UPDATED_AT              timestamp_ntz  not null default current_timestamp(),
    constraint PK_NEWS_DEDUP primary key (DEDUP_CLUSTER_ID)
);

create table if not exists MIP.NEWS.NEWS_INFO_STATE_DAILY (
    AS_OF_DATE               date           not null,
    SYMBOL                   string         not null,
    MARKET_TYPE              string         not null,
    NEWS_COUNT               number         not null default 0,
    UNIQUE_SOURCES           number         not null default 0,
    DEDUP_COUNT              number         not null default 0,
    NOVELTY_SCORE            number(10,6),
    BURST_SCORE              number(10,6),
    UNCERTAINTY_FLAG         boolean        default false,
    TOP_HEADLINES            variant,
    TRAILING_AVG_7D          number(12,6),
    TRAILING_STD_7D          number(12,6),
    Z_SCORE                  number(12,6),
    NEWS_CONTEXT_BADGE       string,
    LAST_NEWS_PUBLISHED_AT   timestamp_ntz,
    LAST_INGESTED_AT         timestamp_ntz,
    SNAPSHOT_TS              timestamp_ntz  not null,
    CREATED_AT               timestamp_ntz  not null default current_timestamp(),
    RUN_ID                   string,
    constraint PK_NEWS_INFO_STATE_DAILY primary key (AS_OF_DATE, SYMBOL, MARKET_TYPE, SNAPSHOT_TS)
);

create table if not exists MIP.NEWS.SYMBOL_ALIAS_DICT (
    SYMBOL                  string         not null,
    MARKET_TYPE             string         not null,
    ALIAS                   string         not null,
    ALIAS_TYPE              string         not null default 'MANUAL',
    IS_ACTIVE               boolean        not null default true,
    CREATED_AT              timestamp_ntz  not null default current_timestamp(),
    UPDATED_AT              timestamp_ntz  not null default current_timestamp(),
    constraint PK_SYMBOL_ALIAS_DICT primary key (SYMBOL, MARKET_TYPE, ALIAS)
);

-- v1 contract enforcement: no full text storage.
update MIP.NEWS.NEWS_RAW
   set FULL_TEXT_OPTIONAL = null
 where FULL_TEXT_OPTIONAL is not null;

-- Useful indexes via clustering hints (non-blocking for Snowflake):
alter table if exists MIP.NEWS.NEWS_RAW
    cluster by (SOURCE_ID, PUBLISHED_AT::date, CANONICAL_URL_HASH);

alter table if exists MIP.NEWS.NEWS_SYMBOL_MAP
    cluster by (SYMBOL, MARKET_TYPE, CREATED_AT::date);

alter table if exists MIP.NEWS.NEWS_INFO_STATE_DAILY
    cluster by (AS_OF_DATE, SYMBOL, MARKET_TYPE);

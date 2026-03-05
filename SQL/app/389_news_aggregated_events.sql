-- 389_news_aggregated_events.sql
-- Purpose: Symbol/bucket aggregated news tape for proposal-time joins.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.NEWS.NEWS_AGGREGATED_EVENTS (
    AS_OF_TS_BUCKET           timestamp_ntz  not null,
    SYMBOL                    string         not null,
    MARKET_TYPE               string         not null,
    ITEMS_TOTAL               number         not null default 0,
    SOURCES_TOTAL             number         not null default 0,
    DEDUP_CLUSTERS_TOTAL      number         not null default 0,
    TOP_CLUSTERS              variant,
    EVENT_TYPE_MIX            variant,
    INFO_PRESSURE             number(12,6),
    NOVELTY                   number(12,6),
    CONFLICT                  number(12,6),
    BADGE                     string,
    LAST_PUBLISHED_AT         timestamp_ntz,
    LAST_INGESTED_AT          timestamp_ntz,
    SNAPSHOT_TS               timestamp_ntz  not null default current_timestamp(),
    RUN_ID                    string,
    CREATED_AT                timestamp_ntz  not null default current_timestamp(),
    UPDATED_AT                timestamp_ntz  not null default current_timestamp(),
    constraint PK_NEWS_AGGREGATED_EVENTS primary key (AS_OF_TS_BUCKET, SYMBOL, MARKET_TYPE)
);

alter table if exists MIP.NEWS.NEWS_AGGREGATED_EVENTS
    cluster by (AS_OF_TS_BUCKET::date, SYMBOL, MARKET_TYPE);

-- 383_news_features_snapshot.sql
-- Purpose: Phase B symbol-level news feature snapshots at decision time.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.NEWS.NEWS_FEATURES_SNAPSHOT (
    AS_OF_TS                     timestamp_ntz  not null,
    SYMBOL                       string         not null,
    MARKET_TYPE                  string         not null,
    EVENT_COUNT                  number         not null default 0,
    POSITIVE_EVENT_COUNT         number         not null default 0,
    NEGATIVE_EVENT_COUNT         number         not null default 0,
    NEUTRAL_EVENT_COUNT          number         not null default 0,
    NEWS_PRESSURE                number(12,6),
    NEWS_SENTIMENT               number(12,6),
    UNCERTAINTY_SCORE            number(12,6),
    EVENT_RISK_SCORE             number(12,6),
    MACRO_HEAT                   number(12,6),
    TOP_EVENTS                   variant,
    LAST_EVENT_TS                timestamp_ntz,
    LAST_NEWS_PUBLISHED_AT       timestamp_ntz,
    LAST_INGESTED_AT             timestamp_ntz,
    SNAPSHOT_TS                  timestamp_ntz  not null,
    NEWS_SNAPSHOT_AGE_MINUTES    number,
    NEWS_IS_STALE                boolean,
    DECAY_TAU_HOURS              number(12,6),
    LOOKBACK_HOURS               number(12,6),
    CREATED_AT                   timestamp_ntz  not null default current_timestamp(),
    RUN_ID                       string,
    constraint PK_NEWS_FEATURES_SNAPSHOT primary key (AS_OF_TS, SYMBOL, MARKET_TYPE, SNAPSHOT_TS)
);

alter table if exists MIP.NEWS.NEWS_FEATURES_SNAPSHOT
    cluster by (AS_OF_TS::date, SYMBOL, MARKET_TYPE);

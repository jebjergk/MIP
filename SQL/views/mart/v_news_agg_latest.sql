-- v_news_agg_latest.sql
-- Purpose: Latest aggregated news bucket per symbol/market_type.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_NEWS_AGG_LATEST as
with universe as (
    select distinct
        upper(SYMBOL) as SYMBOL,
        upper(MARKET_TYPE) as MARKET_TYPE
    from MIP.APP.INGEST_UNIVERSE
    where coalesce(IS_ENABLED, true)
),
latest as (
    select
        n.AS_OF_TS_BUCKET,
        n.SYMBOL,
        n.MARKET_TYPE,
        n.ITEMS_TOTAL,
        n.SOURCES_TOTAL,
        n.DEDUP_CLUSTERS_TOTAL,
        n.TOP_CLUSTERS,
        n.EVENT_TYPE_MIX,
        n.INFO_PRESSURE,
        n.NOVELTY,
        n.CONFLICT,
        n.BADGE,
        n.LAST_PUBLISHED_AT,
        n.LAST_INGESTED_AT,
        n.SNAPSHOT_TS
    from MIP.NEWS.NEWS_AGGREGATED_EVENTS n
    qualify row_number() over (
        partition by n.SYMBOL, n.MARKET_TYPE
        order by n.AS_OF_TS_BUCKET desc, n.SNAPSHOT_TS desc
    ) = 1
)
select
    coalesce(l.AS_OF_TS_BUCKET, to_timestamp_ntz('1970-01-01 00:00:00')) as AS_OF_TS_BUCKET,
    u.SYMBOL,
    u.MARKET_TYPE,
    coalesce(l.ITEMS_TOTAL, 0) as ITEMS_TOTAL,
    coalesce(l.SOURCES_TOTAL, 0) as SOURCES_TOTAL,
    coalesce(l.DEDUP_CLUSTERS_TOTAL, 0) as DEDUP_CLUSTERS_TOTAL,
    coalesce(l.TOP_CLUSTERS, parse_json('[]')) as TOP_CLUSTERS,
    coalesce(l.EVENT_TYPE_MIX, parse_json('{}')) as EVENT_TYPE_MIX,
    coalesce(l.INFO_PRESSURE, 0.0) as INFO_PRESSURE,
    coalesce(l.NOVELTY, 0.0) as NOVELTY,
    coalesce(l.CONFLICT, 0.0) as CONFLICT,
    coalesce(l.BADGE, 'NO_NEWS') as BADGE,
    l.LAST_PUBLISHED_AT,
    l.LAST_INGESTED_AT,
    coalesce(l.SNAPSHOT_TS, to_timestamp_ntz('1970-01-01 00:00:00')) as SNAPSHOT_TS
from universe u
left join latest l
  on l.SYMBOL = u.SYMBOL
 and l.MARKET_TYPE = u.MARKET_TYPE;

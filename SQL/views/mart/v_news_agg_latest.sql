-- v_news_agg_latest.sql
-- Purpose: Latest aggregated news bucket per symbol/market_type.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_NEWS_AGG_LATEST as
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
) = 1;

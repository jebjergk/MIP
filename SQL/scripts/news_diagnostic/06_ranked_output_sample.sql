select
    SYMBOL,
    MARKET_TYPE,
    AS_OF_TS_BUCKET,
    BADGE,
    ITEMS_TOTAL,
    DEDUP_CLUSTERS_TOTAL,
    INFO_PRESSURE,
    NOVELTY,
    CONFLICT,
    LAST_PUBLISHED_AT,
    SNAPSHOT_TS,
    TOP_CLUSTERS
from MIP.NEWS.NEWS_AGGREGATED_EVENTS
where SYMBOL in ('AAPL', 'MSFT', 'XOM', 'APA', 'SBUX')
qualify row_number() over (
    partition by SYMBOL, MARKET_TYPE
    order by AS_OF_TS_BUCKET desc, SNAPSHOT_TS desc
) = 1
order by SYMBOL;

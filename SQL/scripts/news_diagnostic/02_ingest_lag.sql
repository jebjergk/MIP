-- Ingest lag minutes: INGESTED_AT - PUBLISHED_AT (SEC XBRL dominates lag when included)
select
    'ALL_30D' as slice,
    percentile_cont(0.5) within group (order by datediff('minute', PUBLISHED_AT, INGESTED_AT)) as p50_min,
    percentile_cont(0.9) within group (order by datediff('minute', PUBLISHED_AT, INGESTED_AT)) as p90_min,
    avg(datediff('minute', PUBLISHED_AT, INGESTED_AT)) as avg_min
from MIP.NEWS.NEWS_RAW
where INGESTED_AT >= dateadd(day, -30, current_timestamp())
union all
select
    'EXCL_SEC_XBRL_30D',
    percentile_cont(0.5) within group (order by datediff('minute', PUBLISHED_AT, INGESTED_AT)),
    percentile_cont(0.9) within group (order by datediff('minute', PUBLISHED_AT, INGESTED_AT)),
    avg(datediff('minute', PUBLISHED_AT, INGESTED_AT))
from MIP.NEWS.NEWS_RAW
where INGESTED_AT >= dateadd(day, -30, current_timestamp())
  and SOURCE_ID <> 'SEC_XBRL_ALL';

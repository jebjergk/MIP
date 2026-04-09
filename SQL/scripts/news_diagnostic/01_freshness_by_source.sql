-- Per-source volume and last-seen timestamps
select
    SOURCE_ID,
    SOURCE_NAME,
    max(PUBLISHED_AT) as last_published_at,
    max(INGESTED_AT) as last_ingested_at,
    count_if(INGESTED_AT >= dateadd(day, -7, current_timestamp())) as rows_7d,
    count_if(INGESTED_AT >= dateadd(day, -30, current_timestamp())) as rows_30d,
    count(*) as rows_all_time
from MIP.NEWS.NEWS_RAW
group by 1, 2
order by rows_30d desc nulls last;

select MATCH_METHOD, count(*) as c
from MIP.NEWS.NEWS_SYMBOL_MAP
group by 1
order by c desc;

select
    count(*) as map_rows_30d,
    count(distinct NEWS_ID) as distinct_news_30d,
    avg(MATCH_CONFIDENCE) as avg_conf
from MIP.NEWS.NEWS_SYMBOL_MAP
where CREATED_AT >= dateadd(day, -30, current_timestamp());

select count(*) as unmapped_rows_7d
from MIP.NEWS.NEWS_RAW r
where r.INGESTED_AT >= dateadd(day, -7, current_timestamp())
  and not exists (
        select 1 from MIP.NEWS.NEWS_SYMBOL_MAP m where m.NEWS_ID = r.NEWS_ID
    );

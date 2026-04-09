-- Raw ingest share by source (30d)
select
    SOURCE_ID,
    count(*) as n,
    count(*) / sum(count(*)) over () as share
from MIP.NEWS.NEWS_RAW
where INGESTED_AT >= dateadd(day, -30, current_timestamp())
group by 1
order by n desc;

-- Distinct sources per ingest day
select
    INGESTED_AT::date as d,
    count(distinct SOURCE_ID) as sources
from MIP.NEWS.NEWS_RAW
where INGESTED_AT >= dateadd(day, -30, current_timestamp())
group by 1
order by 1 desc;

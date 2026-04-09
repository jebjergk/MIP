-- URL cardinality vs rows (30d)
select
    count(*) as raw_rows,
    count(distinct URL) as distinct_url,
    count(distinct CANONICAL_URL_HASH) as distinct_canon_hash,
    count(distinct NEWS_ID) as distinct_news_id
from MIP.NEWS.NEWS_RAW
where INGESTED_AT >= dateadd(day, -30, current_timestamp());

-- Normalized title cardinality (30d)
with t as (
    select upper(trim(regexp_replace(TITLE, '\\s+', ' '))) as nt
    from MIP.NEWS.NEWS_RAW
    where INGESTED_AT >= dateadd(day, -30, current_timestamp())
      and TITLE is not null
)
select count(*) as total_rows, count(distinct nt) as distinct_norm_titles from t;

-- Repeated normalized titles (7d)
with r as (
    select upper(trim(regexp_replace(TITLE, '\\s+', ' '))) as nt
    from MIP.NEWS.NEWS_RAW
    where INGESTED_AT >= dateadd(day, -7, current_timestamp())
),
d as (
    select nt, count(*) as c from r group by 1 having c > 1
)
select nt, c from d order by c desc limit 25;

-- Cluster size distribution (URL-canonical dedupe)
select CLUSTER_SIZE, count(*) as cluster_count
from MIP.NEWS.NEWS_DEDUP
group by 1
order by 1 desc;

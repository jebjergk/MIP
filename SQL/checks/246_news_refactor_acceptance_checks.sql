-- 246_news_refactor_acceptance_checks.sql
-- Purpose: Validate coverage, density, dedup quality, and proposer impact for news refactor.

use role MIP_ADMIN_ROLE;
use database MIP;

with universe as (
    select distinct upper(SYMBOL) as SYMBOL, upper(MARKET_TYPE) as MARKET_TYPE
    from MIP.APP.INGEST_UNIVERSE
    where coalesce(IS_ENABLED, true)
      and INTERVAL_MINUTES = 1440
),
daily_symbol_counts as (
    select
        date_trunc('day', r.INGESTED_AT) as D,
        m.SYMBOL,
        m.MARKET_TYPE,
        count(*) as ITEMS
    from MIP.NEWS.NEWS_SYMBOL_MAP m
    join MIP.NEWS.NEWS_RAW r
      on r.NEWS_ID = m.NEWS_ID
    where r.INGESTED_AT >= dateadd(day, -7, current_timestamp())
    group by 1, 2, 3
),
coverage as (
    select
        d.D,
        count(distinct concat(d.SYMBOL, '|', d.MARKET_TYPE)) as SYMBOLS_WITH_ITEMS
    from daily_symbol_counts d
    group by d.D
),
universe_size as (
    select count(*) as N from universe
),
density as (
    select
        D,
        approx_percentile(ITEMS, 0.5) as P50_ITEMS_PER_SYMBOL,
        approx_percentile(ITEMS, 0.9) as P90_ITEMS_PER_SYMBOL
    from daily_symbol_counts
    group by D
),
dedup as (
    select
        date_trunc('day', r.INGESTED_AT) as D,
        count(*) as ITEMS_TOTAL,
        count(distinct r.DEDUP_CLUSTER_ID) as CLUSTERS_TOTAL
    from MIP.NEWS.NEWS_RAW r
    where r.INGESTED_AT >= dateadd(day, -7, current_timestamp())
      and r.DEDUP_CLUSTER_ID is not null
    group by 1
),
impact as (
    select
        date_trunc('day', p.PROPOSED_AT) as D,
        count(*) as PROPOSALS_TOTAL,
        count_if(lower(coalesce(to_varchar(p.SOURCE_SIGNALS:news_block_new_entry), 'false')) = 'true') as PROPOSALS_BLOCKED_BY_NEWS,
        count_if(abs(coalesce(try_to_number(to_varchar(p.SOURCE_SIGNALS:news_score_adj)), 0)) > 0.0001) as PROPOSALS_RERANKED_BY_NEWS
    from MIP.AGENT_OUT.ORDER_PROPOSALS p
    where p.PROPOSED_AT >= dateadd(day, -7, current_timestamp())
      and p.STATUS in ('PROPOSED', 'APPROVED', 'EXECUTED')
    group by 1
)
select
    c.D as AS_OF_DAY,
    u.N as UNIVERSE_SIZE,
    c.SYMBOLS_WITH_ITEMS,
    round(c.SYMBOLS_WITH_ITEMS::float / nullif(u.N, 0) * 100, 2) as COVERAGE_PCT,
    d.P50_ITEMS_PER_SYMBOL,
    d.P90_ITEMS_PER_SYMBOL,
    dd.ITEMS_TOTAL as RAW_ITEMS_TOTAL,
    dd.CLUSTERS_TOTAL as DEDUP_CLUSTERS_TOTAL,
    iff(dd.ITEMS_TOTAL > 0, round(1 - (dd.CLUSTERS_TOTAL::float / dd.ITEMS_TOTAL::float), 4), null) as DEDUP_RATIO,
    i.PROPOSALS_TOTAL,
    i.PROPOSALS_RERANKED_BY_NEWS,
    i.PROPOSALS_BLOCKED_BY_NEWS
from coverage c
cross join universe_size u
left join density d
  on d.D = c.D
left join dedup dd
  on dd.D = c.D
left join impact i
  on i.D = c.D
order by AS_OF_DAY desc;

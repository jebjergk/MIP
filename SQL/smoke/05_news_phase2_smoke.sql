-- 05_news_phase2_smoke.sql
-- Purpose: Phase 2 smoke checks for ingestion + dedup.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Execute deterministic test-mode ingest.
call MIP.NEWS.SP_INGEST_RSS_NEWS(true, 3);

-- 2) Latest raw rows.
select
    NEWS_ID,
    SOURCE_ID,
    SOURCE_NAME,
    PUBLISHED_AT,
    INGESTED_AT,
    URL,
    CONTENT_HASH,
    CANONICAL_URL_HASH,
    DEDUP_CLUSTER_ID,
    SNAPSHOT_TS
from MIP.NEWS.NEWS_RAW
order by CREATED_AT desc
limit 20;

-- 3) Dedup aggregate snapshot.
select
    DEDUP_CLUSTER_ID,
    REPRESENTATIVE_NEWS_ID,
    CLUSTER_SIZE,
    CLUSTER_FIRST_SEEN_AT,
    CLUSTER_LAST_SEEN_AT,
    UPDATED_AT
from MIP.NEWS.NEWS_DEDUP
order by UPDATED_AT desc
limit 20;

-- 4) Simple source-level counts.
select
    SOURCE_ID,
    count(*) as raw_rows,
    min(PUBLISHED_AT) as first_published_at,
    max(PUBLISHED_AT) as last_published_at
from MIP.NEWS.NEWS_RAW
group by SOURCE_ID
order by SOURCE_ID;

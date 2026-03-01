-- 06_news_phase3_smoke.sql
-- Purpose: Phase 3 smoke checks for alias seed + deterministic mapping.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Seed alias dictionary.
-- (Idempotent merge script)
-- Run manually before this smoke when executed as separate deployment step:
--   MIP/SQL/app/376_seed_symbol_alias_dict.sql

-- 2) Deterministic test ingestion and mapping.
call MIP.NEWS.SP_INGEST_RSS_NEWS(true, 3);
call MIP.NEWS.SP_MAP_NEWS_SYMBOLS(null);

-- 3) Alias inventory snapshot.
select
    SYMBOL,
    MARKET_TYPE,
    ALIAS,
    ALIAS_TYPE,
    IS_ACTIVE
from MIP.NEWS.SYMBOL_ALIAS_DICT
order by MARKET_TYPE, SYMBOL, ALIAS
limit 120;

-- 4) Mapping output snapshot.
select
    m.NEWS_ID,
    m.SYMBOL,
    m.MARKET_TYPE,
    m.MATCH_METHOD,
    m.MATCH_CONFIDENCE,
    m.CREATED_AT,
    r.SOURCE_ID,
    r.TITLE,
    r.URL
from MIP.NEWS.NEWS_SYMBOL_MAP m
join MIP.NEWS.NEWS_RAW r
  on r.NEWS_ID = m.NEWS_ID
order by m.CREATED_AT desc, m.MATCH_CONFIDENCE desc, m.SYMBOL
limit 100;

-- 5) Method and confidence summary.
select
    MATCH_METHOD,
    count(*) as mapped_rows,
    min(MATCH_CONFIDENCE) as min_confidence,
    max(MATCH_CONFIDENCE) as max_confidence
from MIP.NEWS.NEWS_SYMBOL_MAP
group by MATCH_METHOD
order by MATCH_METHOD;

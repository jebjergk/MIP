-- 09_news_phasea_smoke.sql
-- Purpose: Phase A smoke for structured extraction layer.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Ensure we have deterministic test rows available.
call MIP.NEWS.SP_INGEST_RSS_NEWS(true, 3);
call MIP.NEWS.SP_MAP_NEWS_SYMBOLS(null);

set as_of_for_data = (
    select to_timestamp_ntz(max(PUBLISHED_AT))
    from MIP.NEWS.NEWS_RAW
);

-- Cortex path can be enabled ad-hoc; smoke uses deterministic fallback for stable runtime.
call MIP.NEWS.SP_EXTRACT_NEWS_EVENTS($as_of_for_data, 300, 100, true, 'v1_phase_a', 'llama3.1-70b', false);

select
    count(*) as EXTRACTED_ROWS,
    coalesce(count_if(LLM_USED = true), 0) as LLM_USED_ROWS,
    coalesce(count_if(CONFIDENCE between 0 and 1), 0) as CONFIDENCE_IN_RANGE_ROWS
from MIP.NEWS.NEWS_EVENT_EXTRACTED;

select
    NEWS_ID,
    SYMBOL,
    MARKET_TYPE,
    EVENT_TS,
    EVENT_TYPE,
    DIRECTION,
    CONFIDENCE,
    IMPACT_HORIZON,
    RELEVANCE_SCOPE,
    EVENT_RISK_SCORE,
    LLM_USED,
    PROMPT_VERSION
from MIP.NEWS.NEWS_EVENT_EXTRACTED
order by EVENT_TS desc, NEWS_ID, SYMBOL
limit 50;

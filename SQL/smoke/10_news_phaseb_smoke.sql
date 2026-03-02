-- 10_news_phaseb_smoke.sql
-- Purpose: Phase B smoke for feature vector snapshot and resolver view.

use role MIP_ADMIN_ROLE;
use database MIP;

call MIP.NEWS.SP_INGEST_RSS_NEWS(true, 3);
call MIP.NEWS.SP_MAP_NEWS_SYMBOLS(null);
call MIP.NEWS.SP_EXTRACT_NEWS_EVENTS(current_timestamp(), 400, 100, true, 'v1_phase_a', 'llama3.1-70b', false);
call MIP.NEWS.SP_BUILD_NEWS_FEATURES_SNAPSHOT(current_timestamp(), null);

select
    count(*) as FEATURE_ROWS,
    coalesce(count_if(NEWS_PRESSURE between 0 and 1), 0) as PRESSURE_BOUNDED_ROWS,
    coalesce(count_if(NEWS_SENTIMENT between -1 and 1), 0) as SENTIMENT_BOUNDED_ROWS
from MIP.NEWS.NEWS_FEATURES_SNAPSHOT
where AS_OF_TS::date = current_date();

select
    AS_OF_TS,
    SYMBOL,
    MARKET_TYPE,
    EVENT_COUNT,
    NEWS_PRESSURE,
    NEWS_SENTIMENT,
    UNCERTAINTY_SCORE,
    EVENT_RISK_SCORE,
    MACRO_HEAT,
    NEWS_SNAPSHOT_AGE_MINUTES,
    NEWS_IS_STALE
from MIP.MART.V_NEWS_FEATURES_BY_TS
where AS_OF_TS::date = current_date()
order by AS_OF_TS desc, SYMBOL, MARKET_TYPE
limit 50;

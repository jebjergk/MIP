-- v_news_features_by_ts.sql
-- Purpose: Phase B resolver surface for symbol-level news features by timestamp.
-- Usage contract:
--   - Join on SYMBOL + MARKET_TYPE.
--   - Resolve latest snapshot with AS_OF_TS <= :as_of_ts.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_NEWS_FEATURES_BY_TS as
select
    f.AS_OF_TS,
    f.SYMBOL,
    f.MARKET_TYPE,
    f.EVENT_COUNT,
    f.POSITIVE_EVENT_COUNT,
    f.NEGATIVE_EVENT_COUNT,
    f.NEUTRAL_EVENT_COUNT,
    f.NEWS_PRESSURE,
    f.NEWS_SENTIMENT,
    f.UNCERTAINTY_SCORE,
    f.EVENT_RISK_SCORE,
    f.MACRO_HEAT,
    f.TOP_EVENTS,
    f.LAST_EVENT_TS,
    f.LAST_NEWS_PUBLISHED_AT,
    f.LAST_INGESTED_AT,
    f.SNAPSHOT_TS,
    f.NEWS_SNAPSHOT_AGE_MINUTES,
    f.NEWS_IS_STALE,
    f.DECAY_TAU_HOURS,
    f.LOOKBACK_HOURS,
    f.CREATED_AT,
    f.RUN_ID
from MIP.NEWS.NEWS_FEATURES_SNAPSHOT f;

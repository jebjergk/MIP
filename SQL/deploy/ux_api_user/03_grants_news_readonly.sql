-- 03_grants_news_readonly.sql
-- Purpose: Grant read-only access for News Context objects to UX API role.
-- Idempotent and additive.

use role MIP_ADMIN_ROLE;
use database MIP;

grant usage on schema MIP.NEWS to role MIP_UI_API_ROLE;

grant select on table MIP.NEWS.NEWS_SOURCE_REGISTRY  to role MIP_UI_API_ROLE;
grant select on table MIP.NEWS.NEWS_RAW              to role MIP_UI_API_ROLE;
grant select on table MIP.NEWS.NEWS_SYMBOL_MAP       to role MIP_UI_API_ROLE;
grant select on table MIP.NEWS.NEWS_DEDUP            to role MIP_UI_API_ROLE;
grant select on table MIP.NEWS.NEWS_INFO_STATE_DAILY to role MIP_UI_API_ROLE;
grant select on table MIP.NEWS.SYMBOL_ALIAS_DICT     to role MIP_UI_API_ROLE;
grant select on table MIP.NEWS.NEWS_SOURCE_SUBSCRIPTIONS to role MIP_UI_API_ROLE;
grant select on table MIP.NEWS.NEWS_AGGREGATED_EVENTS to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_NEWS_AGG_LATEST to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_NEWS_FEED_HEALTH to role MIP_UI_API_ROLE;

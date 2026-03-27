-- Live Intelligence Cockpit bootstrap dependencies (objects referenced by mip_ui_api bootstrap + tiles assembly).
use role MIP_ADMIN_ROLE;
use database MIP;

select 'LIVE_PORTFOLIO_CONFIG' as object_name, count(*) as row_cnt from MIP.LIVE.LIVE_PORTFOLIO_CONFIG limit 1;
select 'BROKER_SNAPSHOTS' as object_name, count(*) as row_cnt from MIP.LIVE.BROKER_SNAPSHOTS limit 1;
select 'LIVE_ORDERS' as object_name, count(*) as row_cnt from MIP.LIVE.LIVE_ORDERS limit 1;
select 'LIVE_ACTIONS' as object_name, count(*) as row_cnt from MIP.LIVE.LIVE_ACTIONS limit 1;
select 'MARKET_BARS' as object_name, count(*) as row_cnt from MIP.MART.MARKET_BARS limit 1;
select 'RECOMMENDATION_OUTCOMES' as object_name, count(*) as row_cnt from MIP.APP.RECOMMENDATION_OUTCOMES limit 1;
select 'RECOMMENDATION_LOG' as object_name, count(*) as row_cnt from MIP.APP.RECOMMENDATION_LOG limit 1;
select 'V_NEWS_AGG_LATEST' as object_name, count(*) as row_cnt from MIP.MART.V_NEWS_AGG_LATEST limit 1;
select 'APP_CONFIG' as object_name, count(*) as row_cnt from MIP.APP.APP_CONFIG where CONFIG_KEY in ('FEE_BPS', 'MIN_FEE') limit 1;

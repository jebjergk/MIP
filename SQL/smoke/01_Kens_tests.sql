use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


select * from mip.app.pattern_definition;

UPDATE MIP.APP.PATTERN_DEFINITION
SET 
    PARAMS_JSON = '{
      "fast_window": 5,
      "interval_minutes": 1440,
      "lookback_days": 90,
      "market_type": "ETF",
      "min_return": 0,
      "min_zscore": 1,
      "slow_window": 3
    }',
    UPDATED_AT = CURRENT_TIMESTAMP(),
    UPDATED_BY = 'KJEBERG'
WHERE PATTERN_ID = 201;
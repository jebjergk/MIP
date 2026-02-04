use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Find procedures with :P_PORTFOLIO_ID in exception blocks
-- Run this FIRST to find the culprit procedure
-- =============================================================================

update MIP.APP.PORTFOLIO_PROFILE
set
  CRYSTALLIZE_ENABLED = true,
  PROFIT_TARGET_PCT = 0.05,
  CRYSTALLIZE_MODE = 'WITHDRAW_PROFITS',
  COOLDOWN_DAYS = 2,
  MAX_EPISODE_DAYS = 30,
  TAKE_PROFIT_ON = 'EOD'
where PROFILE_ID = 2;


SELECT 
    QUERY_ID, 
    QUERY_TEXT, 
    ERROR_MESSAGE, 
    START_TIME
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 500))
WHERE ERROR_MESSAGE IS NOT NULL
  AND START_TIME > DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC
LIMIT 20;

SELECT 
    PROCEDURE_NAME,
    ARGUMENT_SIGNATURE,
    CREATED,
    LAST_ALTERED
FROM MIP.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'APP'
  AND PROCEDURE_DEFINITION LIKE '%:P_PORTFOLIO_ID%'
  AND PROCEDURE_DEFINITION LIKE '%exception%'
ORDER BY PROCEDURE_NAME;

-- Find the exact failing query from history
SELECT 
    QUERY_ID, 
    QUERY_TEXT, 
    ERROR_MESSAGE, 
    START_TIME,
    USER_NAME,
    QUERY_TYPE
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 100))
WHERE ERROR_MESSAGE LIKE '%P_PORTFOLIO_ID%'
   OR ERROR_MESSAGE LIKE '%Bind variable%'
ORDER BY START_TIME DESC
LIMIT 10;

-- Check when procedures were last modified (compare to your last deployment)
SELECT 
    PROCEDURE_NAME,
    ARGUMENT_SIGNATURE,
    LAST_ALTERED,
    CREATED
FROM MIP.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'APP'
  AND PROCEDURE_NAME IN (
      'SP_RUN_PORTFOLIO_SIMULATION',
      'SP_PIPELINE_RUN_PORTFOLIO',
      'SP_PIPELINE_RUN_PORTFOLIOS',
      'SP_PIPELINE_WRITE_MORNING_BRIEF',
      'SP_PIPELINE_WRITE_MORNING_BRIEFS',
      'SP_WRITE_MORNING_BRIEF',
      'SP_MONITOR_AUTONOMY_SAFETY',
      'SP_RUN_INTEGRITY_CHECKS'
  )
ORDER BY PROCEDURE_NAME, ARGUMENT_SIGNATURE;

-- Verify the fix is deployed: These should all have "v_portfolio_id number" in declare
-- If you see ":P_PORTFOLIO_ID" AFTER the initial declaration, the fix is not deployed
SELECT 
    PROCEDURE_NAME,
    CASE 
        WHEN PROCEDURE_DEFINITION LIKE '%v_portfolio_id number := :P_PORTFOLIO_ID%' 
        THEN 'FIXED (has local variable copy)'
        ELSE 'NOT FIXED (still using :P_PORTFOLIO_ID directly)'
    END as FIX_STATUS
FROM MIP.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'APP'
  AND PROCEDURE_DEFINITION LIKE '%:P_PORTFOLIO_ID%'
  AND PROCEDURE_DEFINITION LIKE '%exception%'
ORDER BY PROCEDURE_NAME;

-- =============================================================================

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();

update MIP.APP.PORTFOLIO set starting_cash ='2000' where portfolio_id = 2 and name = 'PORTFOLIO_2_LOW_RISK';

delete from MIP.APP.PORTFOLIO_EPISODE where portfolio_id = 101;

select * from MIP.MART.V_BAR_INDEX where bar_index = 57 order by ts desc;

update mip.app.portfolio_profile set crystalize_enabled = true, 

select * from mip.app.portfolio_trades;

select * from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL;

SELECT p.PORTFOLIO_ID, p.NAME, p.PROFILE_ID, pp.NAME as PROFILE_NAME, pp.DRAWDOWN_STOP_PCT
FROM MIP.APP.PORTFOLIO p
LEFT JOIN MIP.APP.PORTFOLIO_PROFILE pp ON pp.PROFILE_ID = p.PROFILE_ID
WHERE p.PORTFOLIO_ID = 2;

select * from mip.app.portfolio_profile;
  select * from MIP.APP.PORTFOLIO_POSITIONS order by hold_until_index;

  select
  portfolio_id,
  as_of_ts,
  pipeline_run_id,
  created_at
from MIP.AGENT_OUT.MORNING_BRIEF
where portfolio_id = 2
order by created_at desc
limit 20;


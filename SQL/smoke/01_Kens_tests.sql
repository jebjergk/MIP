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

-- =============================================================================
-- DIAGNOSTIC: Morning Brief staleness debugging
-- =============================================================================

-- 1. Check what LAST_SIMULATION_RUN_ID is for each portfolio
select 
    PORTFOLIO_ID,
    NAME,
    LAST_SIMULATION_RUN_ID,
    LAST_SIMULATED_AT
from MIP.APP.PORTFOLIO
where STATUS = 'ACTIVE'
order by PORTFOLIO_ID;

-- 2. Check the latest briefs and compare to LAST_SIMULATION_RUN_ID
select 
    mb.PORTFOLIO_ID,
    p.NAME,
    mb.AS_OF_TS,
    coalesce(mb.CREATED_AT, mb.AS_OF_TS) as CREATED_AT,
    mb.PIPELINE_RUN_ID as BRIEF_RUN_ID,
    p.LAST_SIMULATION_RUN_ID as PORTFOLIO_LATEST_RUN_ID,
    case 
        when mb.PIPELINE_RUN_ID = p.LAST_SIMULATION_RUN_ID then 'CURRENT'
        else 'STALE'
    end as STALENESS,
    case 
        when mb.PIPELINE_RUN_ID = p.LAST_SIMULATION_RUN_ID then null
        else 'Brief run ' || left(mb.PIPELINE_RUN_ID, 8) || ' != portfolio run ' || left(p.LAST_SIMULATION_RUN_ID, 8)
    end as STALE_REASON
from MIP.AGENT_OUT.MORNING_BRIEF mb
join MIP.APP.PORTFOLIO p on p.PORTFOLIO_ID = mb.PORTFOLIO_ID
where coalesce(mb.AGENT_NAME, '') = 'MORNING_BRIEF'
qualify row_number() over (partition by mb.PORTFOLIO_ID order by coalesce(mb.CREATED_AT, mb.AS_OF_TS) desc) = 1
order by mb.PORTFOLIO_ID;

-- 3. Check if a brief exists for the latest run (9b46c00d...)
select 
    PORTFOLIO_ID,
    AS_OF_TS,
    PIPELINE_RUN_ID,
    CREATED_AT
from MIP.AGENT_OUT.MORNING_BRIEF
where PIPELINE_RUN_ID like '9b46c00d%'
   or PIPELINE_RUN_ID like '9b%'
order by CREATED_AT desc;

-- 4. Check recent briefs for all portfolios
select
    PORTFOLIO_ID,
    AS_OF_TS,
    PIPELINE_RUN_ID,
    CREATED_AT,
    AGENT_NAME
from MIP.AGENT_OUT.MORNING_BRIEF
order by coalesce(CREATED_AT, AS_OF_TS) desc
limit 20;

-- 5. Check recent audit log for MORNING_BRIEF step
select 
    RUN_ID,
    EVENT_NAME,
    STATUS,
    EVENT_TS,
    DETAILS
from MIP.APP.MIP_AUDIT_LOG
where EVENT_NAME = 'MORNING_BRIEF'
order by EVENT_TS desc
limit 10;

-- 6. Check the FULL audit log for a specific run (replace with actual run_id)
-- Use this to see what happened with run 9b46c00d...
select 
    RUN_ID,
    EVENT_TYPE,
    EVENT_NAME,
    STATUS,
    EVENT_TS,
    left(DETAILS::string, 300) as DETAILS_PREVIEW
from MIP.APP.MIP_AUDIT_LOG
where RUN_ID like '9b46c00d%'
order by EVENT_TS;

-- 7. Check the most recent pipeline runs and their root status
select 
    RUN_ID,
    EVENT_NAME,
    STATUS,
    EVENT_TS,
    DETAILS:pipeline_status_reason::string as STATUS_REASON,
    DETAILS:has_new_bars::boolean as HAS_NEW_BARS
from MIP.APP.MIP_AUDIT_LOG
where EVENT_TYPE = 'PIPELINE'
  and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
order by EVENT_TS desc
limit 10;

-- 8. Check if SP_RUN_DAILY_PIPELINE was redeployed with the new brief-always-write logic
-- (should show LAST_ALTERED after your deployment)
SELECT 
    PROCEDURE_NAME,
    LAST_ALTERED,
    CREATED
FROM MIP.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'APP'
  AND PROCEDURE_NAME = 'SP_RUN_DAILY_PIPELINE';

-- 9. Check active episode start dates (for reset warning logic)
select 
    PORTFOLIO_ID,
    EPISODE_ID,
    STATUS,
    START_TS,
    START_EQUITY
from MIP.APP.PORTFOLIO_EPISODE
where STATUS = 'ACTIVE'
order by PORTFOLIO_ID;

select 
    PORTFOLIO_ID,
    NAME,
    LAST_SIMULATION_RUN_ID,
    LAST_SIMULATED_AT
from MIP.APP.PORTFOLIO
where STATUS = 'ACTIVE'
order by PORTFOLIO_ID;
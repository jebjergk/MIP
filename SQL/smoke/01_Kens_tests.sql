use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


select * from mip.app.pattern_definition;



use role MIP_ADMIN_ROLE;
use database MIP;

-- Check which objects exist
SELECT 'PORTFOLIO_LIFECYCLE_EVENT' as obj, count(*) as exists_flag FROM information_schema.tables WHERE table_schema = 'APP' AND table_name = 'PORTFOLIO_LIFECYCLE_EVENT'
UNION ALL
SELECT 'PORTFOLIO_LIFECYCLE_NARRATIVE', count(*) FROM information_schema.tables WHERE table_schema = 'AGENT_OUT' AND table_name = 'PORTFOLIO_LIFECYCLE_NARRATIVE'
UNION ALL
SELECT 'V_PORTFOLIO_LIFECYCLE_TIMELINE', count(*) FROM information_schema.views WHERE table_schema = 'MART' AND table_name = 'V_PORTFOLIO_LIFECYCLE_TIMELINE'
UNION ALL
SELECT 'V_PORTFOLIO_LIFECYCLE_SNAPSHOT', count(*) FROM information_schema.views WHERE table_schema = 'MART' AND table_name = 'V_PORTFOLIO_LIFECYCLE_SNAPSHOT'
UNION ALL
SELECT 'SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE', count(*) FROM information_schema.procedures WHERE procedure_schema = 'APP' AND procedure_name = 'SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE';

-- 1. Check if the grant exists
SHOW GRANTS ON PROCEDURE MIP.APP.SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE(NUMBER, VARCHAR, TIMESTAMP_NTZ);

-- 2. Test the call AS the API role to reproduce the exact error
USE ROLE MIP_UI_API_ROLE;
CALL MIP.APP.SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE(1);

USE ROLE ACCOUNTADMIN;

-- Allow MIP_ADMIN_ROLE to assume MIP_UI_API_ROLE (for testing)
GRANT ROLE MIP_UI_API_ROLE TO ROLE MIP_ADMIN_ROLE;

-- Now apply the procedure grant
USE ROLE MIP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE MIP.APP.SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE(NUMBER, VARCHAR, TIMESTAMP_NTZ)
    TO ROLE MIP_UI_API_ROLE;

-- Test as the API role
USE ROLE MIP_UI_API_ROLE;
CALL MIP.APP.SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE(1);
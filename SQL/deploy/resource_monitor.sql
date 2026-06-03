-- =============================================================================
-- resource_monitor.sql
-- Snowflake warehouse Resource Monitor for MIP_WH_XS.
--
-- Classic RESOURCE_MONITOR governs warehouse credits only.
-- It does NOT cap CORTEX_AGENTS spend (separate meter).
-- For Cortex Agent spend control, use the app-level daily_call_budget guard
-- in orchestrate_phase4_board (--daily-call-budget flag).
--
-- If your account supports SNOWFLAKE.CORE.BUDGET (check first):
--   SELECT SYSTEM$TYPEOF(SNOWFLAKE.CORE.BUDGET);
-- Then a Cortex-specific budget can be added later.  See Snowflake docs.
--
-- Requires: USE ROLE ACCOUNTADMIN
-- =============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE RESOURCE MONITOR MIP_WH_XS_MONTHLY_CAP
    WITH CREDIT_QUOTA = 25          -- adjust to your chosen monthly ceiling
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 70  PERCENT DO NOTIFY
        ON 90  PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE MIP_WH_XS SET RESOURCE_MONITOR = MIP_WH_XS_MONTHLY_CAP;

-- Verify
SHOW RESOURCE MONITORS LIKE 'MIP_WH_XS_MONTHLY_CAP';

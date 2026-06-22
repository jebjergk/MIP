-- 20260622_cockpit_run_log.sql
-- Purpose: Audit table recording each Cockpit operator action as one of two
--   distinct run types:
--     DAILY_MARKET_UPDATE       — deterministic market-data maintenance
--     AGENTIC_OPPORTUNITY_SEARCH — Phase 4 Cortex agentic proposal board
--
--   Written by management.py in-process (MIP_UI_API_ROLE) at action start
--   and updated at completion. Allows the UI and ops team to clearly
--   distinguish routine maintenance runs from agentic spend runs.
--
--   MIP_UI_API_ROLE needs SELECT, INSERT, UPDATE because management.py
--   uses get_connection() (which connects as MIP_UI_API_ROLE) to write rows.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE TABLE IF NOT EXISTS MIP.APP.COCKPIT_RUN_LOG (
    RUN_ID         VARCHAR(36)    NOT NULL,
    RUN_TYPE       VARCHAR(40)    NOT NULL,   -- DAILY_MARKET_UPDATE | AGENTIC_OPPORTUNITY_SEARCH
    TARGET_DATE    DATE,
    STARTED_AT     TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT   TIMESTAMP_NTZ,
    STATUS         VARCHAR(20)    NOT NULL DEFAULT 'RUNNING', -- RUNNING | SUCCESS | FAILED
    OPERATOR       VARCHAR(100),
    SUMMARY_JSON   VARIANT,
    ERROR_DETAIL   VARCHAR(4000),
    CONSTRAINT PK_COCKPIT_RUN_LOG PRIMARY KEY (RUN_ID)
);

COMMENT ON TABLE MIP.APP.COCKPIT_RUN_LOG IS
    'Audit log for Cockpit operator actions. '
    'RUN_TYPE distinguishes DAILY_MARKET_UPDATE (deterministic, no Cortex) '
    'from AGENTIC_OPPORTUNITY_SEARCH (Phase 4 Cortex board, costs AI credits). '
    'Written by management.py API process as MIP_UI_API_ROLE.';

-- MIP_ADMIN_ROLE: full access (standard for APP schema objects)
GRANT SELECT, INSERT, UPDATE ON TABLE MIP.APP.COCKPIT_RUN_LOG TO ROLE MIP_ADMIN_ROLE;

-- MIP_UI_API_ROLE: management.py writes this table in-process via get_connection()
GRANT SELECT, INSERT, UPDATE ON TABLE MIP.APP.COCKPIT_RUN_LOG TO ROLE MIP_UI_API_ROLE;

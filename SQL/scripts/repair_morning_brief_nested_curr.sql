-- repair_morning_brief_nested_curr.sql
-- Remove recursively-nested signals.changes.curr blobs that caused JS OOM in SP_WRITE_MORNING_BRIEF.
-- Safe: Cockpit re-reads latest brief; pipeline will write a fresh slim brief on next run.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

-- Preview (run manually if desired):
-- SELECT PORTFOLIO_ID, AS_OF_TS, LENGTH(BRIEF::STRING) AS BRIEF_CHARS
--   FROM MIP.AGENT_OUT.MORNING_BRIEF
--  WHERE BRIEF:signals:changes:curr IS NOT NULL
--     OR LENGTH(BRIEF::STRING) > 100000;

DELETE FROM MIP.AGENT_OUT.MORNING_BRIEF
 WHERE BRIEF:signals:changes:curr IS NOT NULL
    OR LENGTH(BRIEF::STRING) > 100000;

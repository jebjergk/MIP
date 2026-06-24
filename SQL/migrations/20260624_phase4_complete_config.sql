-- 20260624_phase4_complete_config.sql
-- Phase 4 redesign: bounded AI_COMPLETE inference (no Cortex Agent tool loops).

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

MERGE INTO MIP.APP.APP_CONFIG t
USING (
    SELECT 'PHASE4_ENABLED' AS CONFIG_KEY,
           'true' AS CONFIG_VALUE,
           'Kill switch for Phase 4 board runs (AI_COMPLETE mode)' AS DESCRIPTION
    UNION ALL
    SELECT 'PHASE4_SPECIALIST_MODEL', 'llama3.1-8b',
           'Snowflake AI_COMPLETE model for Phase 4 specialist calls'
    UNION ALL
    SELECT 'PHASE4_CHAIR_MODEL', 'llama3.1-8b',
           'Snowflake AI_COMPLETE model for Phase 4 chair call'
    UNION ALL
    SELECT 'MAX_LLM_CALLS_PER_RUN', '36',
           'Hard cap on AI_COMPLETE calls per board run (6 per candidate)'
    UNION ALL
    SELECT 'PHASE4_MAX_DAILY_RUNS_PER_PORTFOLIO', '1',
           'Max Phase 4 board runs per portfolio per calendar day'
    UNION ALL
    SELECT 'PHASE4_SPECIALIST_MAX_TOKENS', '1500',
           'max_tokens cap for each specialist AI_COMPLETE call'
    UNION ALL
    SELECT 'PHASE4_CHAIR_MAX_TOKENS', '4000',
           'max_tokens cap for chair AI_COMPLETE call'
    UNION ALL
    SELECT 'PHASE4_MAX_EVIDENCE_CHARS', '80000',
           'Max injected EVIDENCE_JSON characters per role/chair call'
    UNION ALL
    SELECT 'PHASE4_COMPLETE_STATEMENT_TIMEOUT_SEC', '120',
           'Snowflake statement timeout for each AI_COMPLETE call'
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN MATCHED THEN UPDATE SET
    CONFIG_VALUE = s.CONFIG_VALUE,
    DESCRIPTION = s.DESCRIPTION,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
VALUES (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION);

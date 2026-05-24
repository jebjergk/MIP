/* ================================================================
   stage4a_agentic_authority_smoke.sql
   Stage 4a smoke checks for AGENTIC_REVALIDATION_AUTHORITY DDL.

   Run after deploying MIP/SQL/app/545_agentic_authority_tables.sql.
   Each statement should succeed. Stage 4a is observational only — no
   gating effect on LPA/Submit.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

-- 1. Table exists and is reachable.
SELECT 'check_1_table_exists' AS check_name,
       COUNT(*)               AS row_count
FROM   MIP.APP.AGENTIC_REVALIDATION_AUTHORITY;

-- 2. Latest-authority view compiles and returns rows only where IS_LATEST=TRUE.
SELECT 'check_2_view_latest_compiles' AS check_name,
       COUNT(*)                       AS row_count
FROM   MIP.APP.V_AGENTIC_AUTHORITY_LATEST;

-- 3. Latest-operator view compiles.
SELECT 'check_3_view_latest_operator_compiles' AS check_name,
       COUNT(*)                                AS row_count
FROM   MIP.APP.V_AGENTIC_AUTHORITY_LATEST_OPERATOR;

-- 4. Full-history view compiles.
SELECT 'check_4_view_history_compiles' AS check_name,
       COUNT(*)                        AS row_count
FROM   MIP.APP.V_AGENTIC_AUTHORITY_HISTORY;

-- 5. APP_CONFIG keys deployed.
SELECT 'check_5_app_config_keys' AS check_name,
       CONFIG_KEY,
       CONFIG_VALUE
FROM   MIP.APP.APP_CONFIG
WHERE  CONFIG_KEY IN (
        'AGENTIC_AUTHORITY_ENABLED',
        'AGENTIC_AUTO_AUDIT_ENABLED',
        'AGENTIC_MIN_CONFIDENCE_THRESHOLD',
        'AGENTIC_MAX_SESSION_AGE_MINUTES'
       )
ORDER BY CONFIG_KEY;

-- 6. Both Stage 4d / 4b feature flags default to 'false'.
SELECT 'check_6_circuit_breakers_off' AS check_name,
       SUM(CASE WHEN CONFIG_KEY = 'AGENTIC_AUTHORITY_ENABLED' AND CONFIG_VALUE = 'false' THEN 1 ELSE 0 END) AS authority_gating_disabled,
       SUM(CASE WHEN CONFIG_KEY = 'AGENTIC_AUTO_AUDIT_ENABLED' AND CONFIG_VALUE = 'false' THEN 1 ELSE 0 END) AS auto_audit_disabled
FROM   MIP.APP.APP_CONFIG
WHERE  CONFIG_KEY IN ('AGENTIC_AUTHORITY_ENABLED', 'AGENTIC_AUTO_AUDIT_ENABLED');

-- 7. Column inventory — confirm append-only columns landed.
SELECT 'check_7_append_only_columns_present' AS check_name,
       SUM(CASE WHEN COLUMN_NAME = 'AUTHORITY_MODE' THEN 1 ELSE 0 END) AS has_authority_mode,
       SUM(CASE WHEN COLUMN_NAME = 'IS_LATEST'      THEN 1 ELSE 0 END) AS has_is_latest,
       SUM(CASE WHEN COLUMN_NAME = 'SUPERSEDED_AT'  THEN 1 ELSE 0 END) AS has_superseded_at,
       SUM(CASE WHEN COLUMN_NAME = 'SUPERSEDED_BY'  THEN 1 ELSE 0 END) AS has_superseded_by,
       SUM(CASE WHEN COLUMN_NAME = 'COMMITTED_BY'   THEN 1 ELSE 0 END) AS has_committed_by,
       SUM(CASE WHEN COLUMN_NAME = 'PACK_VERSION_OK' THEN 1 ELSE 0 END) AS has_pack_version_ok
FROM   MIP.INFORMATION_SCHEMA.COLUMNS
WHERE  TABLE_SCHEMA = 'APP'
  AND  TABLE_NAME   = 'AGENTIC_REVALIDATION_AUTHORITY';

-- 8. Confirm no UNIQUE constraint on the table (append-only invariant).
--    Only the PK on AUTHORITY_ID should exist.
SELECT 'check_8_no_unique_constraints' AS check_name,
       COUNT(*)                        AS unique_constraint_count
FROM   MIP.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE  TABLE_SCHEMA    = 'APP'
  AND  TABLE_NAME      = 'AGENTIC_REVALIDATION_AUTHORITY'
  AND  CONSTRAINT_TYPE = 'UNIQUE';

-- 9. Sample latest-row payload (empty until diagnostic script is run).
SELECT 'check_9_latest_sample' AS check_name,
       ACTION_ID,
       AUTHORITY_MODE,
       AUTHORITY_STATUS,
       AUTHORITY_CONFIDENCE,
       PACK_VERSION_OK,
       IS_STALE,
       DISAGREES_WITH_BASELINE,
       CREATED_AT
FROM   MIP.APP.V_AGENTIC_AUTHORITY_LATEST
ORDER  BY CREATED_AT DESC
LIMIT  5;

-- 10. Grants spot-check via SHOW GRANTS (Snowflake-native).
SHOW GRANTS ON TABLE MIP.APP.AGENTIC_REVALIDATION_AUTHORITY;

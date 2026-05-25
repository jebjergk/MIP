/* ================================================================
   546_agentic_authority_stage4c_config.sql
   Stage 4c — APP_CONFIG seed for the operator-commit endpoint.

   Adds AGENTIC_OPERATOR_COMMIT_ENABLED feature flag. Defaults to
   'false' on initial deploy so we can land the code path off and
   flip it on after backend smoke. Stage 4d Submit gating is a
   separate flag (AGENTIC_AUTHORITY_ENABLED).

   Idempotent — re-running does not flip the value back to false
   for an environment that has already enabled it.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

MERGE INTO MIP.APP.APP_CONFIG t
USING (
    SELECT 'AGENTIC_OPERATOR_COMMIT_ENABLED' AS CONFIG_KEY,
           'false' AS CONFIG_VALUE,
           'Stage 4c circuit breaker for POST /live/trades/actions/{id}/agentic-authority/commit. '
           || 'When false the endpoint returns 400 DISABLED and no OPERATOR_COMMITTED rows can be written. '
           || 'When true the LPA "Apply Agentic Review" button writes AUTHORITY_MODE=OPERATOR_COMMITTED rows. '
           || 'Submit gating is NOT controlled by this flag — Stage 4d uses AGENTIC_AUTHORITY_ENABLED for that.'
           AS DESCRIPTION
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN MATCHED THEN UPDATE SET
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
VALUES (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, CURRENT_TIMESTAMP());

-- Verify
SELECT 'agentic_operator_commit_enabled' AS feature,
       CONFIG_KEY,
       CONFIG_VALUE,
       UPDATED_AT
FROM   MIP.APP.APP_CONFIG
WHERE  CONFIG_KEY = 'AGENTIC_OPERATOR_COMMIT_ENABLED';

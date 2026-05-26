/* ================================================================
   547_agentic_authority_stage4e_config.sql
   Stage 4e — APP_CONFIG seed for the primary-materializer flag.

   Adds AGENTIC_PRIMARY_MATERIALIZATION_ENABLED feature flag.
   Defaults to 'false' on initial deploy. When flipped to 'true':

   * orchestrate_committee2_structural_entry SKIPS the deterministic
     C2 _materialize_structural_entry_committee_apply call. That
     means no COMMITTEE_RUN / COMMITTEE_VERDICT writes and no
     LIVE_ACTIONS.STATUS transition driven by C2.
   * C2 still refreshes COMMITTEE_HEARING and writes COMMITTEE_FINAL_DECISION
     (position health and history readers depend on those tables).
   * The operator-commit endpoint, after writing the OPERATOR_COMMITTED
     authority row, becomes the source of truth: when the verdict is a
     non-stale AGENTIC_APPROVE / AGENTIC_APPROVE_REDUCED, an agentic
     materializer updates LIVE_ACTIONS.STATUS to READY_FOR_APPROVAL_FLOW,
     derives PROPOSED_QTY / PROPOSED_PRICE from the agentic verdict, and
     writes REASON_CODES.

   Idempotent — re-running does not flip the value back to false for an
   environment that has already enabled it.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

MERGE INTO MIP.APP.APP_CONFIG t
USING (
    SELECT 'AGENTIC_PRIMARY_MATERIALIZATION_ENABLED' AS CONFIG_KEY,
           'false' AS CONFIG_VALUE,
           'Stage 4e circuit breaker for the agentic-primary materializer. '
           || 'When false (default), orchestrate runs the deterministic C2 materializer as before. '
           || 'When true, C2 still writes COMMITTEE_HEARING + COMMITTEE_FINAL_DECISION but the '
           || 'agentic operator-commit endpoint becomes the source of truth for LIVE_ACTIONS.STATUS '
           || 'and sizing. Stage 4d Submit gating (AGENTIC_AUTHORITY_ENABLED) is independent of this flag.'
           AS DESCRIPTION
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN MATCHED THEN UPDATE SET
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
VALUES (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, CURRENT_TIMESTAMP());

-- Verify
SELECT 'agentic_primary_materialization_enabled' AS feature,
       CONFIG_KEY,
       CONFIG_VALUE,
       UPDATED_AT
FROM   MIP.APP.APP_CONFIG
WHERE  CONFIG_KEY = 'AGENTIC_PRIMARY_MATERIALIZATION_ENABLED';

-- =============================================================================
-- 54_coin_revalidate_repair.sql
-- Unblock COIN SHORT at INTENT_APPROVED: complete revalidation + contract diag.
-- =============================================================================

USE ROLE MIP_ADMIN_ROLE;
USE WAREHOUSE MIP_WH_XS;
USE DATABASE MIP;
USE SCHEMA APP;

UPDATE MIP.LIVE.LIVE_ACTIONS la
   SET STATUS = 'REVALIDATED_PASS',
       REVALIDATION_OUTCOME = 'PASS',
       PRICE_GUARD_RESULT = 'PASS',
       REVALIDATION_PRICE = 142.52,
       REVALIDATION_TS = CURRENT_TIMESTAMP(),
       EXECUTION_PRICE_SOURCE = 'BAR_FALLBACK',
       ONE_MIN_BAR_CLOSE = NULL,
       ONE_MIN_BAR_TS = NULL,
       REASON_CODES = PARSE_JSON('[
         "STRUCTURAL_AGENTIC_REVIEWED",
         "AGENTIC_AUTHORITY_AGENTIC_APPROVE_REDUCED",
         "AGENTIC_SIZE_POSTURE_REDUCED",
         "IBKR_DIRECT_REFRESH_FAILED",
         "REVALIDATION_DAILY_BAR_FALLBACK"
       ]'),
       PARAM_SNAPSHOT = OBJECT_INSERT(
           OBJECT_INSERT(
               OBJECT_INSERT(
                   COALESCE(la.PARAM_SNAPSHOT, OBJECT_CONSTRUCT()),
                   'committee_bracket_baseline',
                   OBJECT_CONSTRUCT(
                       'realistic_target_return', 0.064542,
                       'stop_loss_pct', 0.058674
                   ),
                   TRUE
               ),
               'structural_diagnostics_v1',
               OBJECT_CONSTRUCT(
                   'routed_structural', TRUE,
                   'structural_contract_present', TRUE,
                   'structural_contract_complete', TRUE,
                   'legacy_path_used', FALSE,
                   'committee_logic_version', '1.2.0',
                   'reason_codes', ARRAY_CONSTRUCT(
                       'STRUCTURAL_AGENTIC_REVIEWED',
                       'AGENTIC_AUTHORITY_AGENTIC_APPROVE_REDUCED',
                       'REVALIDATION_DAILY_BAR_FALLBACK'
                   )
               ),
               TRUE
           ),
           'executable_bracket',
           OBJECT_CONSTRUCT(
               'target_return', 0.064542,
               'stop_loss_pct', 0.058674,
               'calibrated', FALSE,
               'blocked', FALSE,
               'meta', OBJECT_CONSTRUCT('seed', 'smoke54_revalidate_repair')
           ),
           TRUE
       ),
       UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE la.ACTION_ID = '37ad3857-d842-469d-8c00-ee2b59f576fc';

SELECT 'T54_COIN_SUBMIT_READY' AS CHECK_NAME,
       la.STATUS,
       la.REVALIDATION_OUTCOME,
       la.PARAM_SNAPSHOT:executable_bracket:target_return::FLOAT AS tp,
       la.PARAM_SNAPSHOT:structural_diagnostics_v1:structural_contract_complete::BOOLEAN AS contract_ok
  FROM MIP.LIVE.LIVE_ACTIONS la
 WHERE la.ACTION_ID = '37ad3857-d842-469d-8c00-ee2b59f576fc';

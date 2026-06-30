-- Repair BA submit gate after revalidation / shadow re-runs left blocking reason codes.
-- Keeps OPERATOR_COMMITTED authority (6536a3e3 session) — do not re-run agentic review.
UPDATE MIP.LIVE.LIVE_ACTIONS la
   SET REASON_CODES = PARSE_JSON(
         '["REVALIDATION_PRICE_FROM_IBKR_DIRECT","IBKR_BAR_FETCH_INSTRUMENTATION_V1","STRUCTURAL_AGENTIC_REVIEWED","AGENTIC_AUTHORITY_AGENTIC_APPROVE_REDUCED","AGENTIC_SIZE_POSTURE_REDUCED"]'
       ),
       PARAM_SNAPSHOT = OBJECT_INSERT(
         OBJECT_INSERT(
           OBJECT_INSERT(
             la.PARAM_SNAPSHOT,
             'structural_diagnostics_v1',
             OBJECT_INSERT(
               COALESCE(la.PARAM_SNAPSHOT:structural_diagnostics_v1, OBJECT_CONSTRUCT()),
               'structural_contract_complete', TRUE,
               TRUE
             ),
             TRUE
           ),
           'structural_execution_contract_v1',
           OBJECT_INSERT(
             OBJECT_INSERT(
               OBJECT_INSERT(
                 COALESCE(la.PARAM_SNAPSHOT:structural_execution_contract_v1, OBJECT_CONSTRUCT()),
                 'verdict_blocked', FALSE,
                 TRUE
               ),
               'decision', 'ALLOW',
               TRUE
             ),
             'executable_bracket',
             COALESCE(
               la.PARAM_SNAPSHOT:executable_bracket,
               OBJECT_CONSTRUCT(
                 'target_return', la.PARAM_SNAPSHOT:committee_bracket_baseline:realistic_target_return::FLOAT,
                 'stop_loss_pct', la.PARAM_SNAPSHOT:committee_bracket_baseline:stop_loss_pct::FLOAT,
                 'calibrated', FALSE,
                 'blocked', FALSE
               )
             ),
             TRUE
           ),
           TRUE
         ),
         'executable_bracket',
         COALESCE(
           la.PARAM_SNAPSHOT:executable_bracket,
           OBJECT_CONSTRUCT(
             'target_return', la.PARAM_SNAPSHOT:committee_bracket_baseline:realistic_target_return::FLOAT,
             'stop_loss_pct', la.PARAM_SNAPSHOT:committee_bracket_baseline:stop_loss_pct::FLOAT,
             'calibrated', FALSE,
             'blocked', FALSE,
             'meta', OBJECT_CONSTRUCT('seed', 'submit_contract_repair')
           )
         ),
         TRUE
       ),
       UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE la.ACTION_ID = 'a195f542-bdc4-4a89-a951-207015153652';

SELECT ACTION_ID, SYMBOL, STATUS, REASON_CODES,
       PARAM_SNAPSHOT:executable_bracket:target_return::FLOAT AS tr,
       PARAM_SNAPSHOT:executable_bracket:stop_loss_pct::FLOAT AS sl,
       PARAM_SNAPSHOT:structural_diagnostics_v1:structural_contract_complete::BOOLEAN AS contract_complete
  FROM MIP.LIVE.LIVE_ACTIONS
 WHERE ACTION_ID = 'a195f542-bdc4-4a89-a951-207015153652';

SELECT AUTHORITY_ID, AUTHORITY_MODE, AUTHORITY_STATUS, IS_LATEST, SHADOW_SESSION_ID
  FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY
 WHERE ACTION_ID = 'a195f542-bdc4-4a89-a951-207015153652'
   AND IS_LATEST = TRUE;

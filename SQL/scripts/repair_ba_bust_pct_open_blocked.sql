-- Repair BA after BUST_PCT=0 blocked bracket seeding post-agentic APPROVE.
UPDATE MIP.LIVE.LIVE_ACTIONS la
   SET STATUS = 'READY_FOR_APPROVAL_FLOW',
       REASON_CODES = PARSE_JSON(
         '["STRUCTURAL_AGENTIC_REVIEWED","AGENTIC_AUTHORITY_AGENTIC_APPROVE_REDUCED","AGENTIC_SIZE_POSTURE_REDUCED"]'
       ),
       PARAM_SNAPSHOT = OBJECT_INSERT(
         la.PARAM_SNAPSHOT,
         'executable_bracket',
         OBJECT_CONSTRUCT(
           'target_return', la.PARAM_SNAPSHOT:committee_bracket_baseline:realistic_target_return::FLOAT,
           'stop_loss_pct', la.PARAM_SNAPSHOT:committee_bracket_baseline:stop_loss_pct::FLOAT,
           'calibrated', FALSE,
           'blocked', FALSE,
           'meta', OBJECT_CONSTRUCT('seed', 'bust_pct_repair')
         ),
         TRUE
       ),
       UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE la.ACTION_ID = 'a195f542-bdc4-4a89-a951-207015153652'
   AND la.STATUS = 'OPEN_BLOCKED';

SELECT ACTION_ID, SYMBOL, STATUS, REASON_CODES,
       PARAM_SNAPSHOT:executable_bracket:target_return::FLOAT AS tr,
       PARAM_SNAPSHOT:executable_bracket:stop_loss_pct::FLOAT AS sl
  FROM MIP.LIVE.LIVE_ACTIONS
 WHERE ACTION_ID = 'a195f542-bdc4-4a89-a951-207015153652';

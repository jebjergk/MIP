-- =============================================================================
-- 53_lpa_bracket_seed_repair.sql
-- Repair structural LPA rows blocked by missing executable_bracket despite
-- valid invalidation geometry (COIN SHORT on portfolio 2).
-- =============================================================================

USE ROLE MIP_ADMIN_ROLE;
USE WAREHOUSE MIP_WH_XS;
USE DATABASE MIP;
USE SCHEMA APP;

-- T53-A: Seed executable bracket for COIN SHORT (agentic-approved, bracket missing)
UPDATE MIP.LIVE.LIVE_ACTIONS la
   SET PROPOSED_PRICE = COALESCE(la.PROPOSED_PRICE, (la.ENTRY_ZONE_LOW + la.ENTRY_ZONE_HIGH) / 2),
       PARAM_SNAPSHOT = OBJECT_INSERT(
           OBJECT_INSERT(
               COALESCE(la.PARAM_SNAPSHOT, OBJECT_CONSTRUCT()),
               'executable_bracket',
               OBJECT_CONSTRUCT(
                   'target_return', 0.064542,
                   'stop_loss_pct', 0.058674,
                   'calibrated', FALSE,
                   'blocked', FALSE,
                   'meta', OBJECT_CONSTRUCT('seed', 'smoke53_bracket_repair')
               ),
               TRUE
           ),
           'committee_bracket_baseline',
           OBJECT_CONSTRUCT(
               'realistic_target_return', 0.064542,
               'stop_loss_pct', 0.058674
           ),
           TRUE
       ),
       REASON_CODES = ARRAY_CONSTRUCT(
           'STRUCTURAL_AGENTIC_REVIEWED',
           'AGENTIC_AUTHORITY_AGENTIC_APPROVE_REDUCED',
           'AGENTIC_SIZE_POSTURE_REDUCED'
       ),
       STATUS = 'READY_FOR_APPROVAL_FLOW',
       COMMITTEE_VERDICT = 'PROCEED_REDUCED',
       UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE la.ACTION_ID = '37ad3857-d842-469d-8c00-ee2b59f576fc'
   AND la.SYMBOL = 'COIN'
   AND la.DIRECTION = 'SHORT';

-- T53-B: Bracket present on COIN row
SELECT 'T53B_COIN_HAS_EXECUTABLE_BRACKET' AS CHECK_NAME,
       la.PARAM_SNAPSHOT:executable_bracket:target_return::FLOAT AS tp,
       la.PARAM_SNAPSHOT:executable_bracket:stop_loss_pct::FLOAT AS sl,
       la.STATUS,
       la.COMMITTEE_VERDICT
  FROM MIP.LIVE.LIVE_ACTIONS la
 WHERE la.ACTION_ID = '37ad3857-d842-469d-8c00-ee2b59f576fc';

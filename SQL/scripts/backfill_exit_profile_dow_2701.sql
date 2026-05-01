/*  ================================================================
    backfill_exit_profile_dow_2701.sql

    One-off: patch the DOW agentic proposal (PROPOSAL_ID=2701) from an
    earlier run (2c63810e-0291-4c90-b842-3b3a7eea628f) that was inserted
    before the orchestrator wrote EXIT_PROFILE. DOW is a large-cap
    chemical (around $30/share, normal volatility) so TRAIL_STANDARD
    (2.5% PCT) is the appropriate default. Without this patch, the
    proposal-to-action bridge resolves EXIT_POLICY=FIXED_BRACKET and
    IBKR receives a fixed STP instead of a trail.
    ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;


UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
   SET EXIT_PROFILE = 'TRAIL_STANDARD',
       TRAIL_STYLE  = 'PCT',
       TRAIL_PARAMS = PARSE_JSON(
           '{"policy_version":"v1","profile":"TRAIL_STANDARD",'
        || '"reference":"ENTRY_FILL","tp_mode":"LIMIT",'
        || '"trail_mode":"PCT","trail_value":2.5}')
 WHERE PROPOSAL_ID = 2701
   AND STATUS = 'PROPOSED';


SELECT 'POST_BACKFILL_DOW_STATE' AS CHECK_NAME,
       PROPOSAL_ID, SYMBOL, BOARD_RUN_ID, EXIT_PROFILE, TRAIL_STYLE,
       TRAIL_PARAMS:policy_version::STRING AS POLICY_VERSION,
       TRAIL_PARAMS:trail_value::FLOAT     AS TRAIL_VALUE
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
 WHERE PROPOSAL_ID = 2701;

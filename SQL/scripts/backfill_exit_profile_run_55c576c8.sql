/*  ================================================================
    backfill_exit_profile_run_55c576c8.sql

    Patch the 4 published agentic proposals from PROPOSAL_BOARD run
        55c576c8-ab83-45a7-9884-b00a84016c7c (AS_OF 2026-05-01)
    that were inserted before the orchestrator was taught to write
    EXIT_PROFILE + canonical broker-executable TRAIL_PARAMS.

    Without this fix, the live action bridge in mip_ui_api/routers/live.py
    line 15283 falls through COALESCE(stp.EXIT_PROFILE, rp.EXIT_PROFILE,
    'FIXED_STANDARD') and resolves EXIT_POLICY=FIXED_BRACKET, sending a
    plain STP leg to IBKR instead of a TRAIL leg. The operator does not
    watch screens intraday, so a sell-stop here is unacceptable risk
    posture.

    Backfill targets:
      DAL  PROPOSAL_ID=2801
      BA   PROPOSAL_ID=2802
      TGT  PROPOSAL_ID=2803
      RCAT PROPOSAL_ID=2804  (low-priced micro-cap so use TRAIL_WIDE 4.0%)

    Profile selection rationale (one-time, applied here because the
    chair output for this run did not include exit_profile):
      DAL, BA, TGT  -> TRAIL_STANDARD (2.5% PCT, the default)
      RCAT          -> TRAIL_WIDE     (4.0% PCT) since price is around
                                       $11 and a 2.5% trail equals the
                                       normal noise band.

    TRAIL_PARAMS is rewritten to policy_version='v1' so
    validate_trail_params() in services/live_intelligence/exit_policy.py
    accepts it. The previous policy_version='phase4_agentic_v1' would
    have raised UNSUPPORTED_POLICY_VERSION even if EXIT_PROFILE were
    set, blocking execution with TRAIL_PARAMS_INVALID.
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
 WHERE BOARD_RUN_ID = '55c576c8-ab83-45a7-9884-b00a84016c7c'
   AND SYMBOL IN ('DAL','BA','TGT')
   AND STATUS = 'PROPOSED';


UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
   SET EXIT_PROFILE = 'TRAIL_WIDE',
       TRAIL_STYLE  = 'PCT',
       TRAIL_PARAMS = PARSE_JSON(
           '{"policy_version":"v1","profile":"TRAIL_WIDE",'
        || '"reference":"ENTRY_FILL","tp_mode":"LIMIT",'
        || '"trail_mode":"PCT","trail_value":4.0}')
 WHERE BOARD_RUN_ID = '55c576c8-ab83-45a7-9884-b00a84016c7c'
   AND SYMBOL = 'RCAT'
   AND STATUS = 'PROPOSED';


INSERT INTO MIP.APP.MIP_AUDIT_LOG (
    EVENT_TS, RUN_ID, EVENT_TYPE, EVENT_NAME, STATUS, ROWS_AFFECTED,
    DETAILS, INVOKED_BY_USER, INVOKED_BY_ROLE
) SELECT
    CURRENT_TIMESTAMP(),
    '55c576c8-ab83-45a7-9884-b00a84016c7c',
    'PROPOSAL_BOARD',
    'BACKFILL_EXIT_PROFILE_AND_TRAIL_PARAMS',
    'SUCCESS',
    4,
    OBJECT_CONSTRUCT(
        'reason', 'orchestrator_did_not_write_exit_profile_before_fix',
        'symbols', ARRAY_CONSTRUCT('DAL','BA','TGT','RCAT'),
        'profile_assignments', OBJECT_CONSTRUCT(
            'DAL',  'TRAIL_STANDARD',
            'BA',   'TRAIL_STANDARD',
            'TGT',  'TRAIL_STANDARD',
            'RCAT', 'TRAIL_WIDE'
        ),
        'policy_version_before', 'phase4_agentic_v1',
        'policy_version_after',  'v1',
        'note', 'Without this backfill, the proposal-to-action bridge '
             || 'would have resolved EXIT_POLICY=FIXED_BRACKET and IBKR '
             || 'would have received a sell-stop instead of a trail.'
    ),
    CURRENT_USER(),
    CURRENT_ROLE();


SELECT 'POST_BACKFILL_PROPOSAL_STATE' AS CHECK_NAME,
       PROPOSAL_ID, SYMBOL, EXIT_PROFILE, TRAIL_STYLE,
       TRAIL_PARAMS:policy_version::STRING AS POLICY_VERSION,
       TRAIL_PARAMS:profile::STRING        AS PROFILE,
       TRAIL_PARAMS:trail_mode::STRING     AS TRAIL_MODE,
       TRAIL_PARAMS:trail_value::FLOAT     AS TRAIL_VALUE
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
 WHERE BOARD_RUN_ID = '55c576c8-ab83-45a7-9884-b00a84016c7c'
 ORDER BY PROPOSAL_ID;

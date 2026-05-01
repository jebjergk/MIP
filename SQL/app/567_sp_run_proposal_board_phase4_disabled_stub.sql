/* ================================================================
   567_sp_run_proposal_board_phase4_disabled_stub.sql
   Phase 4 cutover: disable SP_RUN_PROPOSAL_BOARD until the true
   Cortex Agentic board (PHASE4_*_AGENT objects + Python orchestrator)
   is deployed.

   Replaces the rejected single-completion roleplay procedure with a
   fail-closed stub that:
     * Inserts a PROPOSAL_BOARD_RUN row with RUN_STATUS='FAILED' and
       a reason_code identifying the rebuild gate.
     * Returns a structured FAILED status so any scheduled caller
       fails visibly.
     * Never publishes proposals.
     * Has no deterministic fallback, no roleplay completion call,
       no candidate selector.

   Once the Cortex Agentic implementation is deployed, the daily
   pipeline must call the new Python entrypoint
   (MIP/scripts/proposal_board_phase4/run_board.py) instead of this
   procedure.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE OR REPLACE PROCEDURE MIP.APP.SP_RUN_PROPOSAL_BOARD(
    P_PORTFOLIO_ID         NUMBER  DEFAULT NULL,
    P_MAX_PROPOSALS        INTEGER DEFAULT 8,
    P_AS_OF_DATE           DATE    DEFAULT NULL,
    P_SYMBOL_COOLDOWN_DAYS INTEGER DEFAULT 7
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id     VARCHAR(36) := UUID_STRING();
    v_as_of      DATE        := COALESCE(P_AS_OF_DATE, CURRENT_DATE());
BEGIN
    INSERT INTO MIP.APP.PROPOSAL_BOARD_RUN (
        RUN_ID, AS_OF_DATE, PORTFOLIO_ID, RUN_STATUS,
        MODEL_CONFIG_JSON, PROMPT_VERSION, POLICY_VERSION,
        FINISHED_AT, ERROR_JSON
    )
    SELECT
        :v_run_id,
        :v_as_of,
        :P_PORTFOLIO_ID,
        'FAILED',
        OBJECT_CONSTRUCT(
            'mode', 'phase4_agentic_rebuild_in_progress',
            'rejected_predecessor', 'symbol_dossier_agentic_thesis_board_single_completion_roleplay',
            'replacement_target', 'symbol_dossier_cortex_agentic_board_multi_round',
            'cortex_call_strategy', 'disabled_until_real_agents_deployed',
            'fallback_policy', 'fail_closed_no_deterministic_fallback',
            'reason', 'SP_RUN_PROPOSAL_BOARD has been disabled. The Phase 4 board is being rebuilt as a true Cortex Agentic board (CREATE AGENT objects with real challenge/revision rounds). Until that is deployed and the daily scheduler points to the new Python entrypoint, no proposals may be published.'
        ),
        'phase4_agentic_rebuild_disabled_stub',
        'phase4_agentic_rebuild_disabled_stub',
        CURRENT_TIMESTAMP(),
        OBJECT_CONSTRUCT(
            'reason_code', 'PHASE4_AGENTIC_REBUILD_IN_PROGRESS',
            'message', 'SP_RUN_PROPOSAL_BOARD is disabled until the true Cortex Agentic board is deployed. No deterministic fallback. No single-completion roleplay.'
        );

    RETURN OBJECT_CONSTRUCT(
        'status', 'FAILED',
        'reason_code', 'PHASE4_AGENTIC_REBUILD_IN_PROGRESS',
        'run_id', :v_run_id,
        'as_of_date', :v_as_of,
        'message', 'SP_RUN_PROPOSAL_BOARD is disabled until the true Cortex Agentic board is deployed. No deterministic fallback. No single-completion roleplay.'
    );
END;
$$;

GRANT USAGE ON PROCEDURE MIP.APP.SP_RUN_PROPOSAL_BOARD(NUMBER, INTEGER, DATE, INTEGER) TO ROLE MIP_ADMIN_ROLE;

/*  ================================================================
    520_sp_propose_structural_trades.sql
    Compatibility entrypoint for structural trade publication.

    The deterministic weighted proposal selector has been retired.
    This procedure delegates to SP_RUN_PROPOSAL_BOARD, which is the sole
    production proposal-time selector and publishes board-approved rows
    into STRUCTURAL_TRADE_PROPOSALS for downstream compatibility.
    ================================================================ */

CREATE OR REPLACE PROCEDURE MIP.APP.SP_PROPOSE_STRUCTURAL_TRADES(
    P_PORTFOLIO_ID         NUMBER  DEFAULT NULL,
    P_MAX_PROPOSALS        INTEGER DEFAULT 8,   -- Phase 2: raised from 5 to 8
    P_AS_OF_DATE           DATE    DEFAULT NULL,
    P_SYMBOL_COOLDOWN_DAYS INTEGER DEFAULT 7    -- Phase 7 PQI Fix 3
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_result VARIANT;
BEGIN
    v_result := (CALL MIP.APP.SP_RUN_PROPOSAL_BOARD(
        :P_PORTFOLIO_ID,
        :P_MAX_PROPOSALS,
        :P_AS_OF_DATE,
        :P_SYMBOL_COOLDOWN_DAYS
    ));
    RETURN :v_result;
END;
$$;

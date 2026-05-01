/* ================================================================
   571_phase4_review_eligibility.sql
   Phase 4 direction-neutral AGENT_REVIEW_ELIGIBILITY filter — audit table
   and reason codes.

   This is a *budget* / *coverage* filter that decides whether a symbol
   deserves expensive Cortex Agent review on a given day. It MUST NOT
   decide trade direction, long/short, trade/no-trade, or any policy
   verdict. Those are still the agents' responsibility.

   Eligibility logic lives in MIP/scripts/proposal_board_phase4/orchestrator.py
   and reads only from structural data + recent proposal/live-action memory
   that is already part of the dossier payload (no horizontal-signal paths).
   This SQL only owns the audit table + reason-code allowlist.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

-- ----------------------------------------------------------------
-- Audit table — one row per (run_id, symbol) considered, regardless
-- of whether the orchestrator chose to call agents.
-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY (
    ELIGIBILITY_ID         NUMBER       AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    RUN_ID                 VARCHAR(36)  NOT NULL,
    AS_OF_DATE             DATE         NOT NULL,
    PORTFOLIO_ID           NUMBER,
    DOSSIER_ID             NUMBER,                  -- NULL when symbol was filtered before snapshot
    SYMBOL                 VARCHAR(20)  NOT NULL,
    MARKET_TYPE            VARCHAR(20),
    ELIGIBLE               BOOLEAN      NOT NULL,
    PRIMARY_REASON_CODE    VARCHAR(80)  NOT NULL,   -- include reason or skip reason (allowlist)
    SECONDARY_REASON_CODE  VARCHAR(80),
    SIGNAL_FLAGS_JSON      VARIANT,                 -- structural flags considered
    EVIDENCE_SUMMARY_JSON  VARIANT,                 -- compact summary for audit
    NOTES                  VARCHAR(2000),
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (RUN_ID, SYMBOL)
);

GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLE MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY
    TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT
    ON TABLE MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY
    TO ROLE MIP_UI_API_ROLE;


-- ----------------------------------------------------------------
-- Reason-code allowlist for the eligibility filter.
-- Include reasons (ELIGIBLE=TRUE):
--   ELIG_OPEN_POSITION
--   ELIG_ACTIVE_LIVE_ACTION
--   ELIG_RECENT_SETUP_EVENT
--   ELIG_STATE_OR_REGIME_CHANGE
--   ELIG_NEAR_KEY_LEVEL
--   ELIG_ABNORMAL_CANDLE
--   ELIG_RECENT_PROPOSAL_OR_TRADE_MEMORY
--   ELIG_STRONG_STRUCTURAL_TRUST
-- Skip reasons (ELIGIBLE=FALSE):
--   NO_MATERIAL_NEW_EVIDENCE
--   INSUFFICIENT_STRUCTURAL_HISTORY
--   FAR_FROM_ACTIONABLE_LEVELS
--   NO_RECENT_STATE_CHANGE
-- System reason (always ELIGIBLE=TRUE):
--   ELIG_OVERRIDE_SYMBOL_FILTER  — operator passed --symbols, never skip
-- ----------------------------------------------------------------

MERGE INTO MIP.APP.PROPOSAL_BOARD_REASON_CODE tgt
USING (
    SELECT * FROM VALUES
        ('ELIG_OPEN_POSITION', 'ELIGIBILITY', 'INFO',
         'Open position exists for this symbol so agent review always runs.'),
        ('ELIG_ACTIVE_LIVE_ACTION', 'ELIGIBILITY', 'INFO',
         'Active live action exists for this symbol so agent review always runs.'),
        ('ELIG_RECENT_SETUP_EVENT', 'ELIGIBILITY', 'INFO',
         'New or active structural setup event within the eligibility window.'),
        ('ELIG_STATE_OR_REGIME_CHANGE', 'ELIGIBILITY', 'INFO',
         'Structural state or regime changed within the eligibility window.'),
        ('ELIG_NEAR_KEY_LEVEL', 'ELIGIBILITY', 'INFO',
         'Price is near an important support / resistance level.'),
        ('ELIG_ABNORMAL_CANDLE', 'ELIGIBILITY', 'INFO',
         'Recent candle is abnormal (battle candle, wide range, large wick, or large move).'),
        ('ELIG_RECENT_PROPOSAL_OR_TRADE_MEMORY', 'ELIGIBILITY', 'INFO',
         'Recent proposal, live action, or trade memory references this symbol.'),
        ('ELIG_STRONG_STRUCTURAL_TRUST', 'ELIGIBILITY', 'INFO',
         'Strong structural setup trust / path-stat evidence.'),
        ('ELIG_OVERRIDE_SYMBOL_FILTER', 'ELIGIBILITY', 'INFO',
         'Operator-supplied symbols override so eligibility filter is bypassed.'),
        ('NO_MATERIAL_NEW_EVIDENCE', 'ELIGIBILITY', 'WARN',
         'No new structural evidence so nothing material to review today.'),
        ('INSUFFICIENT_STRUCTURAL_HISTORY', 'ELIGIBILITY', 'WARN',
         'Insufficient structural history to support meaningful agent review.'),
        ('FAR_FROM_ACTIONABLE_LEVELS', 'ELIGIBILITY', 'WARN',
         'Price is far from any actionable support / resistance level.'),
        ('NO_RECENT_STATE_CHANGE', 'ELIGIBILITY', 'WARN',
         'No recent structural state or regime change to motivate review.'),
        ('ACTIONABILITY_ESCALATION', 'CHAIR', 'WARN',
         'Chair escalated to PROPOSE while at least one specialist was in WATCH or WAIT_FOR_CONFIRMATION and the case was recorded for audit.'),
        ('IBKR_ACCOUNT_MODE_NOT_PAPER', 'CHAIR', 'WARN',
         'Short publication blocked because IBKR_ACCOUNT_MODE is not PAPER for the configured portfolio.')
    AS v(REASON_CODE, REASON_CATEGORY, SEVERITY, DESCRIPTION)
) src
ON tgt.REASON_CODE = src.REASON_CODE
WHEN MATCHED THEN UPDATE SET
    tgt.REASON_CATEGORY = src.REASON_CATEGORY,
    tgt.SEVERITY = src.SEVERITY,
    tgt.DESCRIPTION = src.DESCRIPTION,
    tgt.IS_ACTIVE = TRUE
WHEN NOT MATCHED THEN INSERT (
    REASON_CODE, REASON_CATEGORY, SEVERITY, DESCRIPTION, IS_ACTIVE
) VALUES (
    src.REASON_CODE, src.REASON_CATEGORY, src.SEVERITY, src.DESCRIPTION, TRUE
);

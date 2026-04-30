/* ================================================================
   560_proposal_board_tables.sql
   Agentic Proposal Board — persistence, reason taxonomy, publication
   lineage, and validation error tables.

   The board is the sole proposal-time selector. STRUCTURAL_TRADE_PROPOSALS
   remains the downstream compatibility publication table.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_RUN (
    RUN_ID                 VARCHAR(36)   NOT NULL PRIMARY KEY,
    AS_OF_DATE             DATE          NOT NULL,
    PORTFOLIO_ID           NUMBER,
    STARTED_AT             TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    FINISHED_AT            TIMESTAMP_NTZ,
    RUN_STATUS             VARCHAR(20)   NOT NULL DEFAULT 'RUNNING',
    CANDIDATE_COUNT        NUMBER        DEFAULT 0,
    FINAL_PROPOSAL_COUNT   NUMBER        DEFAULT 0,
    MODEL_CONFIG_JSON      VARIANT,
    PROMPT_VERSION         VARCHAR(64),
    POLICY_VERSION         VARCHAR(64),
    ERROR_JSON             VARIANT,
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT (
    CANDIDATE_ID           NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    RUN_ID                 VARCHAR(36)   NOT NULL,
    SETUP_EVENT_ID          NUMBER        NOT NULL,
    SOURCE_CANDIDATE_KEY   VARCHAR(128),
    AS_OF_DATE             DATE          NOT NULL,
    PORTFOLIO_ID           NUMBER,
    SYMBOL                 VARCHAR(20)   NOT NULL,
    MARKET_TYPE            VARCHAR(20),
    FAMILY                 VARCHAR(40),
    DIRECTION              VARCHAR(5),
    SETUP_DATE             DATE,
    SETUP_STATUS           VARCHAR(20),
    STRUCTURAL_STATE       VARCHAR(40),
    REGIME_COMPAT          VARCHAR(20),
    TRUST_LABEL            VARCHAR(20),
    ENTRY_ZONE_LOW         FLOAT,
    ENTRY_ZONE_HIGH        FLOAT,
    PRICE_INVALIDATION_LEVEL FLOAT,
    STRUCTURE_CONFIDENCE   FLOAT,
    LEVEL_SIGNIFICANCE     FLOAT,
    MEANINGFUL_HIT_RATE    FLOAT,
    PATH_SURVIVAL_HIT_RATE FLOAT,
    MFE_MAE_RATIO          FLOAT,
    RISK_CLASS             VARCHAR(20),
    ABSOLUTE_EXCLUSION_REASONS VARIANT,
    BOARD_WARNING_FLAGS    VARIANT,
    FRESHNESS_FLAGS_JSON   VARIANT,
    RECENT_TRADE_FLAGS_JSON VARIANT,
    EVIDENCE_PAYLOAD_JSON  VARIANT       NOT NULL,
    PAYLOAD_HASH           VARCHAR(64),
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (RUN_ID, SETUP_EVENT_ID)
);

CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME (
    OUTCOME_ID             NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    RUN_ID                 VARCHAR(36)   NOT NULL,
    CANDIDATE_ID           NUMBER        NOT NULL,
    AGENT_NAME             VARCHAR(80)   NOT NULL,
    VERDICT                VARCHAR(40)   NOT NULL,
    PRIMARY_REASON_CODE    VARCHAR(80)   NOT NULL,
    SECONDARY_REASON_CODE  VARCHAR(80),
    CONFIDENCE             FLOAT,
    RATIONALE_TEXT         VARCHAR(4000),
    STRUCTURED_OUTPUT_JSON VARIANT,
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (RUN_ID, CANDIDATE_ID, AGENT_NAME)
);

CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_INTERACTION (
    INTERACTION_ID         NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    RUN_ID                 VARCHAR(36)   NOT NULL,
    CANDIDATE_ID           NUMBER        NOT NULL,
    SOURCE_AGENT           VARCHAR(80),
    TARGET_AGENT           VARCHAR(80),
    TOPIC                  VARCHAR(120),
    DISAGREEMENT_TYPE      VARCHAR(80),
    DISAGREEMENT_TEXT      VARCHAR(4000),
    RESPONSE_TEXT          VARCHAR(4000),
    RESOLVED_FLAG          BOOLEAN       DEFAULT FALSE,
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT (
    VERDICT_ID             NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    RUN_ID                 VARCHAR(36)   NOT NULL,
    CANDIDATE_ID           NUMBER        NOT NULL,
    FINAL_RANK             NUMBER,
    FINAL_VERDICT          VARCHAR(30)   NOT NULL,
    PRIMARY_REASON_CODE    VARCHAR(80)   NOT NULL,
    SECONDARY_REASON_CODE  VARCHAR(80),
    FINAL_RATIONALE        VARCHAR(4000),
    WHY_SELECTED_OR_REJECTED VARCHAR(4000),
    COMPARATIVE_REASONING_JSON VARIANT,
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (RUN_ID, CANDIDATE_ID)
);

CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_FINAL_SLATE (
    SLATE_ID               NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    RUN_ID                 VARCHAR(36)   NOT NULL,
    CANDIDATE_ID           NUMBER        NOT NULL,
    SETUP_EVENT_ID          NUMBER        NOT NULL,
    SYMBOL                 VARCHAR(20)   NOT NULL,
    RANK                   NUMBER        NOT NULL,
    VERDICT                VARCHAR(30)   NOT NULL,
    SIZING_TREATMENT       VARCHAR(80),
    RISK_CLASS             VARCHAR(20),
    FINAL_RATIONALE_SUMMARY VARCHAR(2000),
    DOWNSTREAM_PAYLOAD_POINTER VARCHAR(200),
    PUBLICATION_STATUS     VARCHAR(30)   DEFAULT 'PENDING',
    PUBLISHED_PROPOSAL_ID  NUMBER,
    PUBLISHED_AT           TIMESTAMP_NTZ,
    PUBLICATION_ERROR_JSON VARIANT,
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (RUN_ID, CANDIDATE_ID)
);

CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_REASON_CODE (
    REASON_CODE            VARCHAR(80)   NOT NULL PRIMARY KEY,
    REASON_CATEGORY        VARCHAR(30)   NOT NULL,
    SEVERITY               VARCHAR(20)   NOT NULL,
    DESCRIPTION            VARCHAR(1000),
    IS_ACTIVE              BOOLEAN       NOT NULL DEFAULT TRUE,
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR (
    ERROR_ID               NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    RUN_ID                 VARCHAR(36)   NOT NULL,
    CANDIDATE_ID           NUMBER,
    AGENT_NAME             VARCHAR(80),
    ERROR_TYPE             VARCHAR(80)   NOT NULL,
    RAW_OUTPUT_JSON        VARIANT,
    ERROR_MESSAGE          VARCHAR(4000),
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE IF EXISTS MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS BOARD_RUN_ID VARCHAR(36);

ALTER TABLE IF EXISTS MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS BOARD_CANDIDATE_ID NUMBER;

ALTER TABLE IF EXISTS MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS BOARD_FINAL_RANK NUMBER;

ALTER TABLE IF EXISTS MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS BOARD_FINAL_VERDICT VARCHAR(30);

ALTER TABLE IF EXISTS MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS BOARD_PRIMARY_REASON_CODE VARCHAR(80);

ALTER TABLE IF EXISTS MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS BOARD_REASON_CODES VARIANT;

ALTER TABLE IF EXISTS MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS BOARD_RATIONALE VARCHAR(4000);

ALTER TABLE IF EXISTS MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    ADD COLUMN IF NOT EXISTS BOARD_PAYLOAD_JSON VARIANT;

MERGE INTO MIP.APP.PROPOSAL_BOARD_REASON_CODE tgt
USING (
    SELECT * FROM VALUES
        ('STRUCTURE_APPROVED', 'STRUCTURE', 'INFO', 'Structure is coherent enough for board consideration.'),
        ('STRUCTURE_NOT_FRESH', 'STRUCTURE', 'WARN', 'Setup structure is stale or no longer fresh.'),
        ('STRUCTURE_WEAK', 'STRUCTURE', 'WARN', 'Setup structure is weak or low confidence.'),
        ('FAMILY_INTERPRETATION_WEAK', 'STRUCTURE', 'WARN', 'The setup family interpretation is not strongly supported.'),
        ('CONFLICTING_STRUCTURE', 'STRUCTURE', 'WARN', 'An opposing structural setup is visible.'),
        ('OPPORTUNITY_ATTRACTIVE', 'OPPORTUNITY', 'INFO', 'The opportunity appears attractive now.'),
        ('OPPORTUNITY_WATCH', 'OPPORTUNITY', 'WARN', 'The opportunity is better suited for watch than action.'),
        ('PULLBACK_TOO_WEAK', 'OPPORTUNITY', 'WARN', 'Pullback evidence is too weak.'),
        ('REVERSAL_TOO_EARLY', 'OPPORTUNITY', 'WARN', 'Reversal evidence is too early.'),
        ('TREND_STALE', 'OPPORTUNITY', 'WARN', 'Trend/setup is stale but not expired.'),
        ('TOO_EXTENDED', 'OPPORTUNITY', 'WARN', 'Price is too far from the entry zone.'),
        ('NOISY_CHOPPY_PRICE_ACTION', 'OPPORTUNITY', 'WARN', 'Recent bar behavior appears noisy or choppy.'),
        ('EVIDENCE_SUPPORTED', 'HISTORY', 'INFO', 'Historical evidence supports the setup.'),
        ('EVIDENCE_MIXED', 'HISTORY', 'WARN', 'Historical evidence is mixed.'),
        ('EVIDENCE_WEAK', 'HISTORY', 'WARN', 'Historical evidence is weak.'),
        ('RECENT_FAILED_SYMBOL', 'HISTORY', 'WARN', 'The symbol recently failed or closed poorly.'),
        ('REPEATED_REPITCH', 'HISTORY', 'WARN', 'The symbol has been proposed repeatedly.'),
        ('PATH_SURVIVAL_WEAK', 'HISTORY', 'WARN', 'Path survival evidence is weak.'),
        ('SAMPLE_SIZE_LOW', 'HISTORY', 'WARN', 'Historical sample size is sparse.'),
        ('EXECUTABLE', 'RISK_EXECUTION', 'INFO', 'Proposal appears feasible at proposal time.'),
        ('EXECUTION_CONSTRAINED', 'RISK_EXECUTION', 'WARN', 'Proposal is feasible only with constraints.'),
        ('EXECUTION_IMPRACTICAL', 'RISK_EXECUTION', 'ERROR', 'Proposal is not practically executable.'),
        ('INVALIDATION_TOO_NEAR', 'RISK_EXECUTION', 'WARN', 'Invalidation appears too near for clean execution.'),
        ('INVALIDATION_TOO_FAR', 'RISK_EXECUTION', 'WARN', 'Invalidation distance appears too large.'),
        ('GAP_RISK_HIGH', 'RISK_EXECUTION', 'WARN', 'Gap risk is high.'),
        ('SIZE_REDUCE_REQUIRED', 'RISK_EXECUTION', 'WARN', 'Reduced sizing is required.'),
        ('SHORT_HISTORY_NOT_OPERATIONAL', 'RISK_EXECUTION', 'WARN', 'Short setup evidence is research-visible but not operationally enabled.'),
        ('DIRECTION_NOT_EXECUTABLE', 'RISK_EXECUTION', 'ERROR', 'Candidate direction is not executable for live publication.'),
        ('WATCHLIST_ONLY', 'CHAIR', 'WARN', 'Candidate is useful for watchlist only.'),
        ('APPROVED_BY_BOARD', 'CHAIR', 'INFO', 'The board approved this candidate for publication.'),
        ('APPROVED_REDUCED_BY_BOARD', 'CHAIR', 'INFO', 'The board approved this candidate with reduced/constrained treatment.'),
        ('BETTER_ALTERNATIVE_EXISTS', 'CHAIR', 'WARN', 'A stronger candidate displaced this one.'),
        ('NO_GOOD_IDEAS_TODAY', 'CHAIR', 'WARN', 'No candidate was strong enough to publish.'),
        ('SYSTEM_AGENT_OUTPUT_INVALID', 'SYSTEM', 'ERROR', 'Agent output failed schema or reason-code validation.'),
        ('SYSTEM_VALIDATION_FAILED', 'SYSTEM', 'ERROR', 'Systemic board output validation failed.'),
        ('ABSOLUTE_GUARDRAIL_EXCLUDED', 'SYSTEM', 'ERROR', 'Candidate failed an absolute pre-board guardrail.'),

        -- Phase 3 calibration codes (mirrored from 562_phase3_calibration_reason_codes.sql).
        -- The 562_*.sql file is the authoritative migration; this block exists only so
        -- a fresh bootstrap of the catalog seeds the same codes. Keep both in lockstep.
        ('APPROVED_CLEAN_LONG_STRUCTURE',          'CHAIR', 'INFO',  'Approved at full size: long with clean structure, attractive opportunity, executable risk, and supportive history. No material warnings.'),
        ('APPROVED_CLEAN_SHORT_STRUCTURE',         'CHAIR', 'INFO',  'Approved at full size: short with clean structure, attractive opportunity, executable risk. Only fires when SHORT_LIVE_ENABLED is true.'),
        ('APPROVED_REDUCED_RISK_CONSTRAINED',      'CHAIR', 'INFO',  'Approved at reduced size because the risk/execution agent flagged the candidate as constrained (e.g. SIZE_REDUCE_REQUIRED, GAP_RISK_HIGH).'),
        ('APPROVED_REDUCED_HISTORY_MIXED',         'CHAIR', 'INFO',  'Approved at reduced size because historical evidence is mixed but structure, opportunity, and risk are otherwise clean.'),
        ('WATCH_SHORT_RESEARCH_ONLY',              'CHAIR', 'WARN',  'Held as research-only watchlist evidence: short candidate surfaced but SHORT_LIVE_ENABLED is false.'),
        ('WATCH_DIRECTION_NOT_EXECUTABLE',         'CHAIR', 'WARN',  'Held as watchlist evidence: candidate direction is not currently live-enabled (non-short cases such as FX without FX_LIVE_ENABLED).'),
        ('WATCH_OPPOSING_SETUP',                   'CHAIR', 'WARN',  'Held as watchlist evidence: same symbol carries an opposing-direction setup AND specialist signals do not converge on approve.'),
        ('WATCH_WAITING_FOR_ENTRY',                'CHAIR', 'WARN',  'Held as watchlist evidence: structure and opportunity look adequate but price is outside the entry zone right now.'),
        ('WATCH_RISK_REWARD_UNATTRACTIVE',         'CHAIR', 'WARN',  'Held as watchlist evidence: tradeable but risk/reward is below threshold per the risk agent.'),
        ('REJECT_REPEATED_REPITCH',                'CHAIR', 'WARN',  'Rejected: same setup has been repeatedly proposed without improvement and opportunity quality is weak.'),
        ('REJECT_STALE_WEAK_STRUCTURE',            'CHAIR', 'WARN',  'Rejected: stale candidate (STALE_BUT_NOT_EXPIRED) combined with weak structure and prior re-pitches.'),
        ('REJECT_RECENT_FAILURE_NO_IMPROVEMENT',   'CHAIR', 'WARN',  'Rejected: recent terminal trade outcome on this symbol AND no measurable improvement in evidence.'),
        ('REJECT_WEAK_OPPORTUNITY',                'CHAIR', 'WARN',  'Rejected: opportunity quality agent emitted a hard-reject verdict (e.g. TOO_EXTENDED, REVERSAL_TOO_EARLY).'),
        ('REJECT_EXECUTION_HARD_BLOCK',            'CHAIR', 'ERROR', 'Rejected: risk/execution agent emitted hard_block (instrument disabled, market halted, or otherwise impossible to execute).'),
        ('EXECUTION_RESEARCH_ONLY',                'RISK_EXECUTION', 'WARN', 'Risk agent verdict research_only: direction is research-visible but not live-enabled today (the policy case, distinct from execution impossibility).'),
        ('RISK_REWARD_UNATTRACTIVE',               'RISK_EXECUTION', 'WARN', 'Risk agent verdict risk_unattractive: candidate is tradeable but the risk/reward ratio is below threshold (e.g. invalidation too near vs target).'),
        ('STRUCTURE_ACCEPTABLE',                   'STRUCTURE', 'INFO', 'Structure is recognizable and tradeable but not pristine. Confidence and level significance are in the middle band.'),
        ('EVIDENCE_SPARSE_BUT_ACCEPTABLE',         'HISTORY', 'INFO', 'Historical sample size is sparse (SAMPLE_SIZE_LOW) but the available statistics lean positive, so this is not a non-answer.'),
        ('OPPORTUNITY_ACCEPTABLE',                 'OPPORTUNITY', 'INFO', 'Opportunity quality is adequate but not standout. Drives APPROVE_REDUCED rather than APPROVE.'),
        ('OPPORTUNITY_WEAK',                       'OPPORTUNITY', 'WARN', 'Opportunity quality is weak (poor risk/reward, confluence light) but not a hard reject.'),
        ('OPPORTUNITY_WAIT_FOR_ENTRY',             'OPPORTUNITY', 'WARN', 'Opportunity is valid but price is outside or far from the entry zone right now.'),

        -- Phase 3 Step 4 chair codes (mirrored from 563_phase3_step4_chair_reason_codes.sql).
        -- The 563_*.sql file is the authoritative migration; this block exists only so a
        -- fresh bootstrap of the catalog seeds the same codes. Keep both in lockstep.
        ('WATCH_OPPORTUNITY_NOT_RIPE',                    'CHAIR', 'WARN', 'Held as watchlist evidence: opportunity agent verdict is watch or weak for reasons other than waiting-for-entry (e.g. TREND_STALE, OPPORTUNITY_WEAK). Distinct from the catch-all WATCHLIST_ONLY.'),
        ('APPROVED_REDUCED_OPPOSING_SETUP',               'CHAIR', 'INFO', 'Approved at reduced size because the same symbol carries an opposing-direction setup, but specialists do NOT converge on weak / ambiguous, so the chair demotes rather than forcing WATCH.'),
        ('APPROVED_REDUCED_REPEATED_REPITCH',             'CHAIR', 'INFO', 'Approved at reduced size because REPEATED_REPITCH fires but the compound REJECT_REPEATED_REPITCH rule (history not OK and opportunity not actionable and structure weak/reject) does not. Demotion gives the warning real policy effect.'),
        ('APPROVED_REDUCED_NOISY_CHOPPY_PRICE_ACTION',    'CHAIR', 'INFO', 'Approved at reduced size because NOISY_CHOPPY_PRICE_ACTION fires and the opportunity agent did NOT override it with an attractive verdict. Caps sizing to acknowledge the noisy regime per amendment 3.')
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

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.PROPOSAL_BOARD_RUN TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.PROPOSAL_BOARD_INTERACTION TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.PROPOSAL_BOARD_FINAL_SLATE TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.PROPOSAL_BOARD_REASON_CODE TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR TO ROLE MIP_ADMIN_ROLE;

GRANT SELECT ON TABLE MIP.APP.PROPOSAL_BOARD_RUN TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON TABLE MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON TABLE MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON TABLE MIP.APP.PROPOSAL_BOARD_INTERACTION TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON TABLE MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON TABLE MIP.APP.PROPOSAL_BOARD_FINAL_SLATE TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON TABLE MIP.APP.PROPOSAL_BOARD_REASON_CODE TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON TABLE MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR TO ROLE MIP_UI_API_ROLE;

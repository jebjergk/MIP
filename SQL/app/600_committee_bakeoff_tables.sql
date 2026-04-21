/* ================================================================
   600_committee_bakeoff_tables.sql
   Committee Bake-off Sidecar (read-only evaluation layer).

   Side-by-side scoring of the REAL Committee 2.0 board vs the
   SHADOW board for shared opportunities. Pure sidecar:
     - reads existing committee/shadow/proposal/market-bar data
     - never writes to live execution / proposal / IBKR / committee
       orchestration paths
     - one row per (PROPOSAL_ID, BOARD_KIND) latched to that board's
       FIRST valid ENTER decision (or first terminal non-enter)
     - one outcome row per latched board decision

   Population: MIP.APP.SP_REFRESH_COMMITTEE_BAKEOFF
   Views:      MIP.MART.V_COMMITTEE_BAKEOFF_*
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

-- ----------------------------------------------------------------
-- COMMITTEE_BAKEOFF_LATCH
-- One row per (PROPOSAL_ID, BOARD_KIND).
-- Captures the first valid ENTER (or first terminal non-enter)
-- decision per board with frozen, normalized evaluation config.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.COMMITTEE_BAKEOFF_LATCH (
    LATCH_ID              NUMBER        AUTOINCREMENT START 1 INCREMENT 1
                                          ORDER PRIMARY KEY,

    PROPOSAL_ID           NUMBER        NOT NULL,
    BOARD_KIND            VARCHAR(10)   NOT NULL,        -- REAL | SHADOW
    SYMBOL                VARCHAR(20)   NOT NULL,
    SIDE                  VARCHAR(5)    NOT NULL,        -- BUY/SELL
    DIRECTION             VARCHAR(5)    NOT NULL,        -- LONG/SHORT

    -- Source identity (provenance)
    DECISION_TS           TIMESTAMP_NTZ NOT NULL,
    HEARING_ID            VARCHAR(36),
    SNAPSHOT_ID           NUMBER,
    ACTION_ID             VARCHAR(64),                   -- real path
    SHADOW_SESSION_ID     VARCHAR(36),                   -- shadow path
    LATCH_SOURCE_TABLE    VARCHAR(120)  NOT NULL,
    LATCH_SOURCE_ID       VARCHAR(64),
    LATCH_REASON          VARCHAR(40)   NOT NULL,        -- FIRST_ENTER | FIRST_TERMINAL_NON_ENTER

    -- Decision fields (raw + normalized kept side-by-side)
    RAW_STANCE            VARCHAR(20)   NOT NULL,        -- APPROVE/APPROVE_REDUCED/DEFER/DENY/WAIT_RECLAIM/...
    NORMALIZED_ACTION     VARCHAR(10)   NOT NULL,        -- ENTER | CASH
    CONFIDENCE            FLOAT,

    -- Frozen evaluation config (per-field provenance lives in EVAL_CONFIG_JSON.provenance)
    EVAL_CONFIG_JSON      VARIANT,

    -- Eligibility for the path evaluator
    CONFIG_STATUS         VARCHAR(20)   NOT NULL,        -- SCORABLE | PARTIAL | UNSCORABLE | NOT_APPLICABLE
    CONFIG_STATUS_REASON  VARCHAR(500),

    CREATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    REFRESHED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    UNIQUE (PROPOSAL_ID, BOARD_KIND)
);

COMMENT ON TABLE MIP.APP.COMMITTEE_BAKEOFF_LATCH IS 'Sidecar per-board first-ENTER (or first terminal non-enter) latch for the committee bake-off. RAW_STANCE preserves source verdict (DEFER vs DENY etc) and NORMALIZED_ACTION collapses to ENTER/CASH for scoring. EVAL_CONFIG_JSON freezes the evaluation config with per-field provenance. CONFIG_STATUS controls scoring eligibility (UNSCORABLE rows are still kept for audit but excluded from comparative aggregates).';

-- ----------------------------------------------------------------
-- COMMITTEE_BAKEOFF_OUTCOME
-- One row per (PROPOSAL_ID, BOARD_KIND) with the path-evaluation
-- outcome and the comparative label vs the sibling board.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.COMMITTEE_BAKEOFF_OUTCOME (
    OUTCOME_ID            NUMBER        AUTOINCREMENT START 1 INCREMENT 1
                                          ORDER PRIMARY KEY,

    LATCH_ID              NUMBER        NOT NULL,
    PROPOSAL_ID           NUMBER        NOT NULL,
    BOARD_KIND            VARCHAR(10)   NOT NULL,
    SYMBOL                VARCHAR(20)   NOT NULL,
    DIRECTION             VARCHAR(5)    NOT NULL,
    DECISION_TS           TIMESTAMP_NTZ NOT NULL,
    NORMALIZED_ACTION     VARCHAR(10)   NOT NULL,
    LATCH_CONFIG_STATUS   VARCHAR(20)   NOT NULL,

    -- Fill resolution
    ENTRY_BAR_TS          TIMESTAMP_NTZ,
    ENTRY_PRICE           FLOAT,
    EXIT_BAR_TS           TIMESTAMP_NTZ,
    EXIT_PRICE            FLOAT,
    BARS_HELD             INTEGER,

    -- Raw outcome classification (one of the user-defined labels)
    RAW_OUTCOME           VARCHAR(30)   NOT NULL,
        -- CASH | NO_FILL | STOPPED | TP_HIT | TRAIL_EXIT |
        -- OPEN_AT_HORIZON | TIE_BOTH_TOUCHED

    REALIZED_RETURN_PCT   FLOAT,                          -- direction-aware, sizing-agnostic
    MAX_FAVORABLE_PCT     FLOAT,
    MAX_ADVERSE_PCT       FLOAT,

    -- Comparative label vs sibling board on same proposal
    COMPARISON_LABEL      VARCHAR(40),
        -- REAL_WIN__SHADOW_CASH | REAL_LOSS__SHADOW_CASH |
        -- REAL_CASH__SHADOW_WIN | REAL_CASH__SHADOW_LOSS |
        -- REAL_WIN__SHADOW_WIN  | REAL_LOSS__SHADOW_LOSS |
        -- REAL_WIN__SHADOW_LOSS | REAL_LOSS__SHADOW_WIN  |
        -- BOTH_CASH | TIE_CASE | UNCOMPARABLE

    -- Exclusion semantics — never silently coerced
    SCORING_EXCLUDED_FLAG  BOOLEAN      NOT NULL DEFAULT FALSE,
    SCORING_EXCLUDED_REASON VARCHAR(500),

    EVAL_NOTES_JSON       VARIANT,

    CREATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    REFRESHED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    UNIQUE (PROPOSAL_ID, BOARD_KIND)
);

COMMENT ON TABLE MIP.APP.COMMITTEE_BAKEOFF_OUTCOME IS 'Sidecar path-evaluation outcome per latched board decision. RAW_OUTCOME preserves the unbiased classification and COMPARISON_LABEL is the per-opportunity comparison vs the sibling board. SCORING_EXCLUDED_FLAG is set when either board is UNSCORABLE, when forward market data is missing, or when RAW_OUTCOME equals TIE_BOTH_TOUCHED.';

-- Convenience: a tiny config-style row for the bake-off start cutoff so
-- the procedure has a stable, queryable default. Optional override via
-- procedure parameter remains the source of truth.
CREATE TABLE IF NOT EXISTS MIP.APP.COMMITTEE_BAKEOFF_CONFIG (
    CONFIG_KEY     VARCHAR(80) PRIMARY KEY,
    CONFIG_VALUE   VARCHAR(200),
    UPDATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO MIP.APP.COMMITTEE_BAKEOFF_CONFIG t
USING (
    SELECT 'BAKEOFF_START_DATE' AS CONFIG_KEY, '2026-04-19' AS CONFIG_VALUE
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE) VALUES (s.CONFIG_KEY, s.CONFIG_VALUE);

/* ================================================================
   Grants
   - MIP_ADMIN_ROLE writes (procedure runs as caller).
   - MIP_UI_API_ROLE only reads via SELECT (covered by future-table
     grants in deploy/ux_api_user/02_grants_readonly.sql). We add
     explicit grants here for safety in case future grants weren't
     run on this account.
   ================================================================ */
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.COMMITTEE_BAKEOFF_LATCH    TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.COMMITTEE_BAKEOFF_OUTCOME  TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.COMMITTEE_BAKEOFF_CONFIG   TO ROLE MIP_ADMIN_ROLE;

GRANT SELECT ON TABLE MIP.APP.COMMITTEE_BAKEOFF_LATCH    TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON TABLE MIP.APP.COMMITTEE_BAKEOFF_OUTCOME  TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON TABLE MIP.APP.COMMITTEE_BAKEOFF_CONFIG   TO ROLE MIP_UI_API_ROLE;

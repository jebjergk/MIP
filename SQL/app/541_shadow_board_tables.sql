/* ================================================================
   541_shadow_board_tables.sql
   Shadow Board Phase 1 — persistence tables (APP schema).
   Zero live authority. All shadow session artifacts stored here.
   Completely independent of COMMITTEE_HEARING / COMMITTEE_FINAL_DECISION.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

-- ----------------------------------------------------------------
-- SHADOW_EVIDENCE_PACK_CACHE
-- Staging table: holds the de-verdicted evidence pack for agent
-- tool access during an active shadow session. TTL-limited (24 h).
-- Agent tool GET_SHADOW_EVIDENCE_SLICE reads ONLY from this table.
-- Real board stance / confidence / chair narrative are excluded.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.SHADOW_EVIDENCE_PACK_CACHE (
    HEARING_ID   VARCHAR(36)   NOT NULL PRIMARY KEY,
    PACK_JSON    VARIANT       NOT NULL,
    SESSION_ID   VARCHAR(36)   NOT NULL,
    CREATED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EXPIRES_AT   TIMESTAMP_NTZ NOT NULL
);

COMMENT ON TABLE MIP.APP.SHADOW_EVIDENCE_PACK_CACHE IS
    'Shadow Board Phase 1: staging area for de-verdicted evidence packs. '
    'Agents call GET_SHADOW_EVIDENCE_SLICE which reads only this table. '
    'Expires 24 h after creation. Real board verdict fields excluded from PACK_JSON.';

-- ----------------------------------------------------------------
-- SHADOW_BOARD_SESSION
-- One row per shadow run for a given hearing.
-- Captures the full lifecycle: stage reached, final verdict, errors.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.SHADOW_BOARD_SESSION (
    SESSION_ID        VARCHAR(36)   NOT NULL PRIMARY KEY,
    HEARING_ID        VARCHAR(36)   NOT NULL,
    PROPOSAL_ID       NUMBER        NOT NULL,
    SHADOW_STANCE     VARCHAR(20),
    SHADOW_CONFIDENCE FLOAT,
    STAGE_REACHED     NUMBER        DEFAULT 0,
    STATUS            VARCHAR(20)   DEFAULT 'RUNNING',
    DEGRADED          BOOLEAN       DEFAULT FALSE,
    DEGRADED_REASON   VARCHAR(500),
    AGENT_MODEL       VARCHAR(80)   DEFAULT 'claude-4-sonnet',
    PACK_VERSION      VARCHAR(32)   DEFAULT '1.0.0',
    RUN_MS            NUMBER,
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT      TIMESTAMP_NTZ
);

COMMENT ON TABLE MIP.APP.SHADOW_BOARD_SESSION IS
    'Shadow Board Phase 1: top-level session record per shadow hearing run. '
    'STATUS: RUNNING | COMPLETE | DEGRADED | FAILED. '
    'Never touches COMMITTEE_FINAL_DECISION or executes real trades.';

-- ----------------------------------------------------------------
-- SHADOW_SPECIALIST_POSITION
-- One row per specialist per session (Stage 1 output).
-- Agents produce independent positions; no real-board stance visible.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.SHADOW_SPECIALIST_POSITION (
    POSITION_ID    NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    SESSION_ID     VARCHAR(36)   NOT NULL,
    HEARING_ID     VARCHAR(36)   NOT NULL,
    ROLE_NAME      VARCHAR(64)   NOT NULL,
    STANCE         VARCHAR(20),
    CONFIDENCE     FLOAT,
    RATIONALE      VARCHAR(2000),
    EVIDENCE_USED  VARIANT,
    RAW_RESPONSE   VARIANT,
    PARSE_OK       BOOLEAN       DEFAULT TRUE,
    DEGRADED       BOOLEAN       DEFAULT FALSE,
    DEGRADED_REASON VARCHAR(500),
    AGENT_ELAPSED_MS NUMBER,
    CREATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (SESSION_ID, ROLE_NAME)
);

COMMENT ON TABLE MIP.APP.SHADOW_SPECIALIST_POSITION IS
    'Shadow Board Phase 1: Stage 1 specialist positions. '
    'One row per (SESSION_ID, ROLE_NAME). PARSE_OK=FALSE means raw JSON was invalid.';

-- ----------------------------------------------------------------
-- SHADOW_CONFLICT_MAP
-- Records which specialist pairs disagree, severity, and chosen
-- challenger role for Stage 3 (Python-side detection, Stage 2).
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.SHADOW_CONFLICT_MAP (
    CONFLICT_ID     NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    SESSION_ID      VARCHAR(36)   NOT NULL,
    HEARING_ID      VARCHAR(36)   NOT NULL,
    ROLE_A          VARCHAR(64)   NOT NULL,
    ROLE_B          VARCHAR(64)   NOT NULL,
    STANCE_A        VARCHAR(20),
    STANCE_B        VARCHAR(20),
    SEVERITY        VARCHAR(20)   NOT NULL,
    CHALLENGER_ROLE VARCHAR(64),
    TARGET_ROLE     VARCHAR(64),
    CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MIP.APP.SHADOW_CONFLICT_MAP IS
    'Shadow Board Phase 1: Stage 2 Python-side conflict detection. '
    'SEVERITY: MINOR | MAJOR | CRITICAL. '
    'CHALLENGER_ROLE drives Stage 3 challenge turn.';

-- ----------------------------------------------------------------
-- SHADOW_CHALLENGE_TURN
-- One row per session: the Stage 3 objectless AGENT_RUN challenge.
-- Capped to exactly one round.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.SHADOW_CHALLENGE_TURN (
    CHALLENGE_ID       NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    SESSION_ID         VARCHAR(36)   NOT NULL UNIQUE,
    HEARING_ID         VARCHAR(36)   NOT NULL,
    CHALLENGER_ROLE    VARCHAR(64)   NOT NULL,
    TARGET_ROLE        VARCHAR(64)   NOT NULL,
    CHALLENGE_TEXT     VARCHAR(4000),
    RAW_RESPONSE       VARIANT,
    PARSE_OK           BOOLEAN       DEFAULT TRUE,
    DEGRADED           BOOLEAN       DEFAULT FALSE,
    DEGRADED_REASON    VARCHAR(500),
    AGENT_ELAPSED_MS   NUMBER,
    CREATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MIP.APP.SHADOW_CHALLENGE_TURN IS
    'Shadow Board Phase 1: Stage 3 challenge turn. Exactly one per session. '
    'Objectless AGENT_RUN with dynamic instructions. No recursive expansion.';

-- ----------------------------------------------------------------
-- SHADOW_REVISION_TURN
-- One row per specialist that was challenged (Stage 4).
-- At most one revision per specialist; no recursion.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.SHADOW_REVISION_TURN (
    REVISION_ID        NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    SESSION_ID         VARCHAR(36)   NOT NULL,
    HEARING_ID         VARCHAR(36)   NOT NULL,
    ROLE_NAME          VARCHAR(64)   NOT NULL,
    REVISED_STANCE     VARCHAR(20),
    ORIGINAL_STANCE    VARCHAR(20),
    STANCE_CHANGED     BOOLEAN       DEFAULT FALSE,
    REVISION_NOTE      VARCHAR(2000),
    RAW_RESPONSE       VARIANT,
    PARSE_OK           BOOLEAN       DEFAULT TRUE,
    DEGRADED           BOOLEAN       DEFAULT FALSE,
    DEGRADED_REASON    VARCHAR(500),
    AGENT_ELAPSED_MS   NUMBER,
    CREATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (SESSION_ID, ROLE_NAME)
);

COMMENT ON TABLE MIP.APP.SHADOW_REVISION_TURN IS
    'Shadow Board Phase 1: Stage 4 revision turn. At most one per challenged specialist. '
    'Objectless AGENT_RUN. STANCE_CHANGED tracks if position moved from original.';

-- ----------------------------------------------------------------
-- SHADOW_CHAIR_RULING
-- Final shadow chair output (Stage 5).
-- One row per session. Does NOT interact with COMMITTEE_FINAL_DECISION.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.SHADOW_CHAIR_RULING (
    RULING_ID          NUMBER        AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
    SESSION_ID         VARCHAR(36)   NOT NULL UNIQUE,
    HEARING_ID         VARCHAR(36)   NOT NULL,
    SHADOW_STANCE      VARCHAR(20),
    SHADOW_CONFIDENCE  FLOAT,
    PLURALITY_BASIS    VARCHAR(500),
    CONFLICT_RESOLUTION VARCHAR(2000),
    SHADOW_TRADE_JSON  VARIANT,
    TOP_SUPPORTS       VARIANT,
    TOP_TENSIONS       VARIANT,
    RAW_RESPONSE       VARIANT,
    PARSE_OK           BOOLEAN       DEFAULT TRUE,
    DEGRADED           BOOLEAN       DEFAULT FALSE,
    DEGRADED_REASON    VARCHAR(500),
    AGENT_ELAPSED_MS   NUMBER,
    CREATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MIP.APP.SHADOW_CHAIR_RULING IS
    'Shadow Board Phase 1: Stage 5 shadow chair ruling. '
    'SHADOW_TRADE_JSON is symbolic only — no execution. '
    'Completely separate from COMMITTEE_FINAL_DECISION.';

/* ================================================================
   Grants — both MIP_ADMIN_ROLE and MIP_UI_API_ROLE need full CRUD
   on shadow tables (app uses MIP_UI_API_ROLE; agents use MIP_ADMIN_ROLE)
   ================================================================ */
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_EVIDENCE_PACK_CACHE  TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_BOARD_SESSION         TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_SPECIALIST_POSITION   TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_CONFLICT_MAP          TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_CHALLENGE_TURN        TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_REVISION_TURN         TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_CHAIR_RULING          TO ROLE MIP_ADMIN_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_EVIDENCE_PACK_CACHE  TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_BOARD_SESSION         TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_SPECIALIST_POSITION   TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_CONFLICT_MAP          TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_CHALLENGE_TURN        TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_REVISION_TURN         TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_CHAIR_RULING          TO ROLE MIP_UI_API_ROLE;

/* ================================================================
   540_committee2_tables.sql
   Committee 2.0 — structural hearing room (APP schema).
   Immutable proposal snapshots + current hearing state + final decision.
   Separate from MIP.LIVE.COMMITTEE_*.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

-- ----------------------------------------------------------------
-- STRUCTURAL_PROPOSAL_SNAPSHOT (immutable, one row per proposal)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT (
    SNAPSHOT_ID           NUMBER        AUTOINCREMENT START 1 INCREMENT 1
                                         ORDER PRIMARY KEY,
    PROPOSAL_ID           NUMBER        NOT NULL UNIQUE,
    PROPOSAL_TS           TIMESTAMP_NTZ NOT NULL,
    SYMBOL                VARCHAR(20)   NOT NULL,
    SIDE                  VARCHAR(5)    NOT NULL,
    SETUP_FAMILY          VARCHAR(40)   NOT NULL,
    STRUCTURAL_STATE      VARCHAR(100),
    REGIME_STATE          VARCHAR(20),
    TRUST_LABEL           VARCHAR(40),
    ENTRY_ZONE_JSON       VARIANT,
    INVALIDATION_JSON     VARIANT,
    PATH_METRICS_JSON     VARIANT,
    MFE_MAE_JSON          VARIANT,
    TRAILING_STYLE        VARCHAR(20),
    PROPOSAL_SUMMARY_JSON VARIANT,
    CREATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT IS
    'Committee 2.0 frozen proposal-time truth. One row per STRUCTURAL_TRADE_PROPOSALS row.';

-- ----------------------------------------------------------------
-- COMMITTEE_HEARING (current state only; one row per PROPOSAL_ID)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.COMMITTEE_HEARING (
    HEARING_ID            VARCHAR(36)   NOT NULL PRIMARY KEY,
    PROPOSAL_ID           NUMBER        NOT NULL UNIQUE,
    SNAPSHOT_ID           NUMBER        NOT NULL,
    HEARING_TS            TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EVIDENCE_JSON         VARIANT,
    DELTAS_JSON           VARIANT,
    CHAIR_OUTPUT_JSON     VARIANT,
    OPERATIONAL_JSON      VARIANT,
    STANCE                VARCHAR(20),
    CONFIDENCE            FLOAT,
    STATUS                VARCHAR(20)   DEFAULT 'OPEN',
    EVIDENCE_PACK_VERSION VARCHAR(32)   DEFAULT '1.0.0',
    UPDATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MIP.APP.COMMITTEE_HEARING IS
    'Committee 2.0 latest hearing only. Refresh overwrites. UNIQUE PROPOSAL_ID.';

-- ----------------------------------------------------------------
-- COMMITTEE_ROLE_OUTPUT (current roles; overwritten on refresh)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.COMMITTEE_ROLE_OUTPUT (
    HEARING_ID      VARCHAR(36)  NOT NULL,
    ROLE_NAME       VARCHAR(64)  NOT NULL,
    OUTPUT_JSON     VARIANT,
    EVIDENCE_REFS   VARIANT,
    UPDATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (HEARING_ID, ROLE_NAME)
);

COMMENT ON TABLE MIP.APP.COMMITTEE_ROLE_OUTPUT IS
    'Committee 2.0 specialist outputs for current hearing. Replaced atomically on refresh.';

-- ----------------------------------------------------------------
-- COMMITTEE_EVIDENCE_ARTIFACT (shared visual payloads)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT (
    HEARING_ID      VARCHAR(36)  NOT NULL,
    ARTIFACT_KIND   VARCHAR(64)  NOT NULL,
    PAYLOAD_JSON    VARIANT,
    SCHEMA_VERSION  VARCHAR(16)   DEFAULT '1',
    EVIDENCE_REFS   VARIANT,
    UPDATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (HEARING_ID, ARTIFACT_KIND)
);

COMMENT ON TABLE MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT IS
    'Committee 2.0 structure map geometry meter etc. Replaced on refresh.';

-- ----------------------------------------------------------------
-- COMMITTEE_FINAL_DECISION (durable; one row per HEARING_ID)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.COMMITTEE_FINAL_DECISION (
    FINAL_DECISION_ID     NUMBER        AUTOINCREMENT START 1 INCREMENT 1
                                           ORDER PRIMARY KEY,
    HEARING_ID            VARCHAR(36)   NOT NULL UNIQUE,
    PROPOSAL_ID           NUMBER        NOT NULL,
    SNAPSHOT_ID           NUMBER        NOT NULL,
    STANCE                VARCHAR(20)   NOT NULL,
    CONFIDENCE            FLOAT,
    CHAIR_OUTPUT_JSON     VARIANT,
    ROLE_OUTPUTS_JSON     VARIANT,
    DELTA_SUMMARY_JSON    VARIANT,
    POSTURE_JSON          VARIANT,
    EVIDENCE_REFS_JSON    VARIANT,
    ARTIFACTS_JSON        VARIANT,
    ACTION_ID             VARCHAR(64),
    TRADE_ID              VARCHAR(64),
    COMMIT_NOTE           VARCHAR(500),
    DECISION_TS           TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MIP.APP.COMMITTEE_FINAL_DECISION IS
    'Committee 2.0 frozen packet when user commits hearing for trade. UNIQUE HEARING_ID.';

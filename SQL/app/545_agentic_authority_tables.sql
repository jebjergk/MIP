/* ================================================================
   545_agentic_authority_tables.sql
   Stage 4a — Agentic revalidation authority persistence (APP schema).
   Append-only history of mapped agentic authority verdicts derived
   from completed shadow board sessions.

   No runtime gating in Stage 4a — this DDL is observational only.
   Stage 4d will use OPERATOR_COMMITTED rows from this table to gate
   Submit. AUTO_AUDIT rows are populated by Stage 4b background hook.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

-- ----------------------------------------------------------------
-- AGENTIC_REVALIDATION_AUTHORITY (append-only)
-- One row per authority commit (auto-audit or operator-committed).
-- AUTHORITY_ID is unique per row; ACTION_ID is non-unique.
-- IS_LATEST flag identifies the current authority per ACTION_ID.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.AGENTIC_REVALIDATION_AUTHORITY (
    AUTHORITY_ID             VARCHAR(36)     NOT NULL,
    ACTION_ID                VARCHAR(36)     NOT NULL,
    PROPOSAL_ID              NUMBER          NOT NULL,
    HEARING_ID               VARCHAR(36)     NOT NULL,
    SHADOW_SESSION_ID        VARCHAR(36)     NOT NULL,
    PACK_VERSION             VARCHAR(32)     NOT NULL,
    PACK_VERSION_OK          BOOLEAN         NOT NULL,

    -- Authority provenance
    AUTHORITY_MODE           VARCHAR(20)     NOT NULL,
    COMMITTED_BY             VARCHAR(100),
    IS_LATEST                BOOLEAN         NOT NULL DEFAULT TRUE,
    SUPERSEDED_AT            TIMESTAMP_NTZ,
    SUPERSEDED_BY            VARCHAR(36),

    -- Mapped authority verdict
    AUTHORITY_STATUS         VARCHAR(40)     NOT NULL,
    AUTHORITY_REASON_CODE    VARCHAR(100),
    AUTHORITY_CONFIDENCE     FLOAT,

    -- Raw shadow signals
    SHADOW_STANCE_RAW        VARCHAR(20),
    SHADOW_STATUS_RAW        VARCHAR(20),
    SHADOW_STAGE_REACHED     NUMBER,
    SHADOW_DEGRADED          BOOLEAN,
    SHADOW_DEGRADED_REASON   VARCHAR(500),
    SHADOW_PLURALITY_BASIS   VARCHAR(500),
    SHADOW_SIZE_POSTURE      VARCHAR(20),

    -- Comparison vs deterministic baseline
    DETERMINISTIC_BASELINE_STANCE  VARCHAR(20),
    DISAGREES_WITH_BASELINE        BOOLEAN,

    -- Staleness
    IS_STALE                 BOOLEAN         NOT NULL DEFAULT FALSE,
    STALE_REASON             VARCHAR(200),
    SESSION_AGE_MINUTES      NUMBER,

    -- Operator override (Stage 4 initially disabled)
    IS_OPERATOR_OVERRIDDEN   BOOLEAN         NOT NULL DEFAULT FALSE,
    OVERRIDE_BY              VARCHAR(100),
    OVERRIDE_AT              TIMESTAMP_NTZ,
    OVERRIDE_REASON          VARCHAR(500),
    OVERRIDE_ORIGINAL_STATUS VARCHAR(40),

    -- Full snapshot
    AUTHORITY_PAYLOAD_JSON   VARIANT,

    CREATED_AT               TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT               TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_AGENTIC_REVALIDATION_AUTHORITY PRIMARY KEY (AUTHORITY_ID)
);

COMMENT ON TABLE MIP.APP.AGENTIC_REVALIDATION_AUTHORITY IS 'Stage 4 agentic authority. Append-only history of mapped shadow-board verdicts. AUTHORITY_MODE=AUTO_AUDIT is created by the Stage 4b background hook for observation only. AUTHORITY_MODE=OPERATOR_COMMITTED is created by the Stage 4c LPA Apply Agentic Review button. Stage 4d Submit gating trusts only the latest IS_LATEST=TRUE, AUTHORITY_MODE=OPERATOR_COMMITTED, IS_STALE=FALSE row per ACTION_ID with AUTHORITY_STATUS in (AGENTIC_APPROVE, AGENTIC_APPROVE_REDUCED). No UNIQUE constraint on ACTION_ID. Every commit inserts a new row and supersedes previous IS_LATEST rows. Independent of COMMITTEE_FINAL_DECISION. Stage 4a is observational only with no live gating effect.';

COMMENT ON COLUMN MIP.APP.AGENTIC_REVALIDATION_AUTHORITY.AUTHORITY_MODE IS 'AUTO_AUDIT (background hook from shadow board completion) or OPERATOR_COMMITTED (explicit LPA commit). Only OPERATOR_COMMITTED gates Submit in Stage 4d.';

COMMENT ON COLUMN MIP.APP.AGENTIC_REVALIDATION_AUTHORITY.IS_LATEST IS 'TRUE for the most recent authority row per ACTION_ID. Set to FALSE by the same transaction that inserts a newer row for the same ACTION_ID.';

COMMENT ON COLUMN MIP.APP.AGENTIC_REVALIDATION_AUTHORITY.PACK_VERSION_OK IS 'TRUE when PACK_VERSION is in the explicit SUPPORTED_PACK_VERSIONS allow-list (Python module). FALSE values map to AGENTIC_DEGRADED_NO_AUTHORITY.';

COMMENT ON COLUMN MIP.APP.AGENTIC_REVALIDATION_AUTHORITY.AUTHORITY_STATUS IS 'AGENTIC_APPROVE | AGENTIC_APPROVE_REDUCED | AGENTIC_WAIT_RECLAIM | AGENTIC_DEFER | AGENTIC_REJECT | AGENTIC_DEGRADED_NO_AUTHORITY | AGENTIC_FAILED_NO_AUTHORITY.';

-- ----------------------------------------------------------------
-- V_AGENTIC_AUTHORITY_LATEST
-- Latest non-superseded row per action (any AUTHORITY_MODE).
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.APP.V_AGENTIC_AUTHORITY_LATEST AS
SELECT
    ara.AUTHORITY_ID,
    ara.ACTION_ID,
    ara.PROPOSAL_ID,
    ara.HEARING_ID,
    ara.SHADOW_SESSION_ID,
    ara.PACK_VERSION,
    ara.PACK_VERSION_OK,
    ara.AUTHORITY_MODE,
    ara.COMMITTED_BY,
    ara.AUTHORITY_STATUS,
    ara.AUTHORITY_REASON_CODE,
    ara.AUTHORITY_CONFIDENCE,
    ara.SHADOW_STANCE_RAW,
    ara.SHADOW_STATUS_RAW,
    ara.SHADOW_STAGE_REACHED,
    ara.SHADOW_DEGRADED,
    ara.SHADOW_DEGRADED_REASON,
    ara.SHADOW_PLURALITY_BASIS,
    ara.SHADOW_SIZE_POSTURE,
    ara.DETERMINISTIC_BASELINE_STANCE,
    ara.DISAGREES_WITH_BASELINE,
    ara.IS_STALE,
    ara.STALE_REASON,
    ara.SESSION_AGE_MINUTES,
    ara.IS_OPERATOR_OVERRIDDEN,
    ara.OVERRIDE_BY,
    ara.OVERRIDE_AT,
    ara.OVERRIDE_REASON,
    ara.OVERRIDE_ORIGINAL_STATUS,
    ara.AUTHORITY_PAYLOAD_JSON,
    ara.CREATED_AT,
    ara.UPDATED_AT,
    la.STATUS                AS LIVE_ACTION_STATUS,
    la.PROPOSED_QTY          AS ACTION_QTY,
    la.SYMBOL                AS SYMBOL,
    cfd.STANCE               AS C2_STANCE,
    cfd.CONFIDENCE           AS C2_CONFIDENCE
FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY ara
LEFT JOIN MIP.LIVE.LIVE_ACTIONS la             ON la.ACTION_ID = ara.ACTION_ID
LEFT JOIN MIP.APP.COMMITTEE_FINAL_DECISION cfd ON cfd.ACTION_ID = ara.ACTION_ID
WHERE ara.IS_LATEST = TRUE;

COMMENT ON VIEW MIP.APP.V_AGENTIC_AUTHORITY_LATEST IS 'Latest authority row per ACTION_ID (any AUTHORITY_MODE). Used by LPA for display. Stage 4d Submit gating uses V_AGENTIC_AUTHORITY_LATEST_OPERATOR instead.';

-- ----------------------------------------------------------------
-- V_AGENTIC_AUTHORITY_LATEST_OPERATOR
-- Latest OPERATOR_COMMITTED row per action — drives Stage 4d gating.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.APP.V_AGENTIC_AUTHORITY_LATEST_OPERATOR AS
WITH ranked_operator AS (
    SELECT
        ara.*,
        ROW_NUMBER() OVER (PARTITION BY ara.ACTION_ID ORDER BY ara.CREATED_AT DESC) AS rn
    FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY ara
    WHERE ara.AUTHORITY_MODE = 'OPERATOR_COMMITTED'
)
SELECT
    r.AUTHORITY_ID,
    r.ACTION_ID,
    r.PROPOSAL_ID,
    r.HEARING_ID,
    r.SHADOW_SESSION_ID,
    r.PACK_VERSION,
    r.PACK_VERSION_OK,
    r.AUTHORITY_MODE,
    r.COMMITTED_BY,
    r.IS_LATEST,
    r.AUTHORITY_STATUS,
    r.AUTHORITY_REASON_CODE,
    r.AUTHORITY_CONFIDENCE,
    r.SHADOW_STANCE_RAW,
    r.SHADOW_STATUS_RAW,
    r.SHADOW_SIZE_POSTURE,
    r.DETERMINISTIC_BASELINE_STANCE,
    r.DISAGREES_WITH_BASELINE,
    r.IS_STALE,
    r.STALE_REASON,
    r.SESSION_AGE_MINUTES,
    r.IS_OPERATOR_OVERRIDDEN,
    r.OVERRIDE_BY,
    r.OVERRIDE_AT,
    r.OVERRIDE_REASON,
    r.CREATED_AT,
    la.STATUS                AS LIVE_ACTION_STATUS,
    la.SYMBOL                AS SYMBOL
FROM ranked_operator r
LEFT JOIN MIP.LIVE.LIVE_ACTIONS la ON la.ACTION_ID = r.ACTION_ID
WHERE r.rn = 1;

COMMENT ON VIEW MIP.APP.V_AGENTIC_AUTHORITY_LATEST_OPERATOR IS 'Latest OPERATOR_COMMITTED authority per ACTION_ID. This is the view Stage 4d will use for Submit gating. Note this row may itself be superseded by a newer AUTO_AUDIT row, so callers should also check IS_LATEST and IS_STALE before granting authority.';

-- ----------------------------------------------------------------
-- V_AGENTIC_AUTHORITY_HISTORY
-- Full audit trail per action (any mode, including superseded).
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.APP.V_AGENTIC_AUTHORITY_HISTORY AS
SELECT
    ara.ACTION_ID,
    ara.AUTHORITY_ID,
    ara.AUTHORITY_MODE,
    ara.COMMITTED_BY,
    ara.AUTHORITY_STATUS,
    ara.AUTHORITY_REASON_CODE,
    ara.AUTHORITY_CONFIDENCE,
    ara.SHADOW_STANCE_RAW,
    ara.SHADOW_STATUS_RAW,
    ara.SHADOW_SESSION_ID,
    ara.PACK_VERSION,
    ara.PACK_VERSION_OK,
    ara.IS_STALE,
    ara.STALE_REASON,
    ara.IS_LATEST,
    ara.SUPERSEDED_AT,
    ara.SUPERSEDED_BY,
    ara.IS_OPERATOR_OVERRIDDEN,
    ara.OVERRIDE_BY,
    ara.OVERRIDE_REASON,
    ara.CREATED_AT
FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY ara
ORDER BY ara.ACTION_ID, ara.CREATED_AT DESC;

COMMENT ON VIEW MIP.APP.V_AGENTIC_AUTHORITY_HISTORY IS 'Full audit trail of all authority commits per action, including superseded rows. Read-only diagnostic view, never used for live gating.';

/* ================================================================
   APP_CONFIG seed — Stage 4a feature flags and thresholds
   All flags default to OFF / observation-only. Stage 4d will flip
   AGENTIC_AUTHORITY_ENABLED to 'true' explicitly when ready.
   ================================================================ */
MERGE INTO MIP.APP.APP_CONFIG t
USING (
    SELECT 'AGENTIC_AUTHORITY_ENABLED' AS CONFIG_KEY,
           'false' AS CONFIG_VALUE,
           'Stage 4d circuit breaker. When false, Submit gating ignores agentic authority and '
           || 'remains on the deterministic path. Stage 4a deploys with this OFF.' AS DESCRIPTION
    UNION ALL
    SELECT 'AGENTIC_AUTO_AUDIT_ENABLED',
           'false',
           'Stage 4b background hook. When false, shadow board completion does not write '
           || 'AUTO_AUDIT rows into AGENTIC_REVALIDATION_AUTHORITY. Stage 4a deploys with this OFF.'
    UNION ALL
    SELECT 'AGENTIC_MIN_CONFIDENCE_THRESHOLD',
           '0.40',
           'Minimum SHADOW_CONFIDENCE required for a positive AGENTIC_APPROVE/APPROVE_REDUCED '
           || 'authority. Below this threshold authority maps to AGENTIC_DEGRADED_NO_AUTHORITY.'
    UNION ALL
    SELECT 'AGENTIC_MAX_SESSION_AGE_MINUTES',
           '240',
           'Maximum age of a shadow session at commit time. Older sessions are marked IS_STALE=TRUE '
           || 'and forced to AGENTIC_FAILED_NO_AUTHORITY.'
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN MATCHED THEN UPDATE SET
    t.CONFIG_VALUE = s.CONFIG_VALUE,
    t.DESCRIPTION  = s.DESCRIPTION,
    t.UPDATED_AT   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
VALUES (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, CURRENT_TIMESTAMP());

/* ================================================================
   Grants — admin role for service ops; UI API role for endpoint reads/writes
   ================================================================ */
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.AGENTIC_REVALIDATION_AUTHORITY TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.AGENTIC_REVALIDATION_AUTHORITY TO ROLE MIP_UI_API_ROLE;

GRANT SELECT ON VIEW MIP.APP.V_AGENTIC_AUTHORITY_LATEST           TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.APP.V_AGENTIC_AUTHORITY_LATEST_OPERATOR  TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.APP.V_AGENTIC_AUTHORITY_HISTORY          TO ROLE MIP_ADMIN_ROLE;

GRANT SELECT ON VIEW MIP.APP.V_AGENTIC_AUTHORITY_LATEST           TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.APP.V_AGENTIC_AUTHORITY_LATEST_OPERATOR  TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.APP.V_AGENTIC_AUTHORITY_HISTORY          TO ROLE MIP_UI_API_ROLE;

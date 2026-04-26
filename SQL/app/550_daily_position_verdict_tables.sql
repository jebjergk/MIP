/* ================================================================
   550_daily_position_verdict_tables.sql
   Daily Position Health / Shadow Health Review — V1 persistence.

   Three tables form the persistence layer:
     1) DAILY_POSITION_VERDICT          — deterministic real verdict (SQL)
     2) DAILY_POSITION_SHADOW_REVIEW    — Cortex-Agents shadow review (Python)
     3) SHADOW_POSITION_LIFECYCLE       — bake-off simulated lifecycle

   Identity rule (all three tables):
     POSITION_EPISODE_KEY = SHA2(
         PORTFOLIO_ID || '|' || COALESCE(EPISODE_ID, 'NO_EPISODE')
         || '|' || SYMBOL || '|' || ENTRY_DATE,
         256
     )
   Joins between real, shadow, and lifecycle are by POSITION_EPISODE_KEY.

   Real and shadow are advisory-only. No real positions are altered.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

-- ----------------------------------------------------------------
-- DAILY_POSITION_VERDICT
-- One row per real open position per AS_OF_DATE.
-- Deterministic, canonical, advisory-only daily position verdict.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.DAILY_POSITION_VERDICT (
    AS_OF_DATE                    DATE          NOT NULL,
    POSITION_EPISODE_KEY          VARCHAR(64)   NOT NULL,
    PORTFOLIO_ID                  NUMBER(38,0)  NOT NULL,
    EPISODE_ID                    NUMBER(38,0),
    SYMBOL                        VARCHAR(64)   NOT NULL,
    SIDE                          VARCHAR(8)    NOT NULL,
    ENTRY_DATE                    DATE          NOT NULL,
    ENTRY_TS                      TIMESTAMP_NTZ NOT NULL,
    ENTRY_PRICE                   NUMBER(18,8),
    DAYS_HELD                     NUMBER(18,0),

    VERDICT                       VARCHAR(20)   NOT NULL,
    HEALTH_STATE                  VARCHAR(20)   NOT NULL,

    BASELINE_QUALITY              VARCHAR(10),
    THESIS_INTEGRITY              VARCHAR(20),
    PATH_QUALITY                  VARCHAR(20),
    REGIME_ALIGNMENT              VARCHAR(20),
    TIME_EFFICIENCY               VARCHAR(20),
    FRAGILITY                     VARCHAR(20),
    SEVERITY                      VARCHAR(10),

    EXPECTED_HORIZON_DAYS         NUMBER(18,0),
    HORIZON_SOURCE_CODE           VARCHAR(30),
    DISTANCE_TO_INVALIDATION_PCT  NUMBER(18,4),
    UNREALIZED_PNL_PCT            NUMBER(18,4),

    PRIMARY_REASON_CODE           VARCHAR(64),
    PRIMARY_REASON_TEXT           VARCHAR(500),

    OBSERVATION_SUMMARY           VARCHAR(500),
    VERDICT_SUMMARY               VARCHAR(500),
    WHY_SUMMARY                   VARCHAR(500),

    DETAIL_JSON                   VARIANT,

    ENGINE_VERSION                VARCHAR(20)   DEFAULT '1.0.0',
    SOURCE_RUN_TS                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DAILY_POSITION_VERDICT PRIMARY KEY (AS_OF_DATE, POSITION_EPISODE_KEY)
);

COMMENT ON TABLE MIP.APP.DAILY_POSITION_VERDICT IS 'Daily Position Health V1: deterministic real verdict per open position per as-of date. Advisory only. POSITION_EPISODE_KEY joins to shadow review and lifecycle tables.';

-- ----------------------------------------------------------------
-- DAILY_POSITION_SHADOW_REVIEW
-- One row per real open position per AS_OF_DATE.
-- Cortex-Agents-based health review (advisory / experimental).
-- Independent of the deterministic verdict.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.DAILY_POSITION_SHADOW_REVIEW (
    AS_OF_DATE                    DATE          NOT NULL,
    POSITION_EPISODE_KEY          VARCHAR(64)   NOT NULL,
    PORTFOLIO_ID                  NUMBER(38,0)  NOT NULL,
    EPISODE_ID                    NUMBER(38,0),
    SYMBOL                        VARCHAR(64)   NOT NULL,
    SIDE                          VARCHAR(8),
    ENTRY_DATE                    DATE,

    SHADOW_VERDICT                VARCHAR(20),
    SHADOW_THESIS_STATUS          VARCHAR(20),
    SHADOW_ACTION_BIAS            VARCHAR(20),
    SHADOW_SEVERITY               VARCHAR(10),

    PRIMARY_REASON_CODE           VARCHAR(64),
    PRIMARY_REASON_TEXT           VARCHAR(500),

    OBSERVATION_SUMMARY           VARCHAR(500),
    VERDICT_SUMMARY               VARCHAR(500),
    WHY_SUMMARY                   VARCHAR(500),
    RATIONALE_TEXT                VARCHAR(2000),
    RATIONALE_JSON                VARIANT,

    AGREES_WITH_REAL              BOOLEAN,
    DISAGREEMENT_CLASS            VARCHAR(40),

    SHADOW_RUN_STATUS             VARCHAR(20)   NOT NULL DEFAULT 'PENDING',
    SHADOW_RUN_TS                 TIMESTAMP_NTZ,
    SHADOW_RUN_ELAPSED_MS         NUMBER(18,0),
    SHADOW_RUN_ERROR              VARCHAR(2000),

    AGENT_NAME                    VARCHAR(80)   DEFAULT 'POSITION_HEALTH_REVIEW_AGENT',
    MODEL_VERSION                 VARCHAR(40)   DEFAULT 'claude-4-sonnet',
    PROMPT_VERSION                VARCHAR(20)   DEFAULT '1.0.0',

    SOURCE_RUN_TS                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DAILY_POSITION_SHADOW_REVIEW PRIMARY KEY (AS_OF_DATE, POSITION_EPISODE_KEY)
);

COMMENT ON TABLE MIP.APP.DAILY_POSITION_SHADOW_REVIEW IS 'Daily Position Health V1: Cortex-Agents shadow review per tracked open position per as-of date. SHADOW_RUN_STATUS in PENDING SUCCESS PARSE_ERROR API_ERROR SKIPPED. Joined by POSITION_EPISODE_KEY.';

-- ----------------------------------------------------------------
-- SHADOW_POSITION_LIFECYCLE
-- One row per shadow simulation lifecycle, identified by POSITION_EPISODE_KEY.
-- Tracks whether the shadow simulated an exit (bake-off) and outcome.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.SHADOW_POSITION_LIFECYCLE (
    POSITION_EPISODE_KEY          VARCHAR(64)   NOT NULL PRIMARY KEY,
    PORTFOLIO_ID                  NUMBER(38,0)  NOT NULL,
    EPISODE_ID                    NUMBER(38,0),
    SYMBOL                        VARCHAR(64)   NOT NULL,
    SIDE                          VARCHAR(8),

    SHADOW_ENTRY_DATE             DATE,
    SHADOW_ENTRY_PRICE            NUMBER(18,8),
    SHADOW_QUANTITY               NUMBER(18,8),

    SHADOW_STATUS                 VARCHAR(20)   NOT NULL DEFAULT 'OPEN',
    SHADOW_EXIT_DATE              DATE,
    SHADOW_EXIT_PRICE             NUMBER(18,8),
    SHADOW_EXIT_TRIGGER           VARCHAR(60),
    SHADOW_EXIT_REASON_CODE       VARCHAR(64),
    EXIT_FROM_VERDICT_DATE        DATE,
    DAYS_HELD_SHADOW              NUMBER(18,0),

    REAL_POSITION_STILL_OPEN_FLAG BOOLEAN,
    REAL_EXIT_DATE                DATE,
    REAL_EXIT_PRICE               NUMBER(18,8),

    REALIZED_RETURN_SHADOW_PCT             NUMBER(18,4),
    REALIZED_RETURN_REAL_COMPARABLE_PCT    NUMBER(18,4),
    OUTCOME_COMPARISON_JSON                VARIANT,

    FIRST_REVIEW_DATE             DATE,
    LAST_REVIEW_DATE              DATE,

    SOURCE_RUN_TS                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MIP.APP.SHADOW_POSITION_LIFECYCLE IS 'Daily Position Health V1: shadow lifecycle keyed by POSITION_EPISODE_KEY. SHADOW_STATUS in OPEN SIM_EXITED. EXIT_REVIEW with EXIT_NOW triggers SHADOW_COMMITTEE_EXIT_AT_CLOSE at official close on AS_OF_DATE. No re-entry in V1.';

/* ================================================================
   Grants — both MIP_ADMIN_ROLE and MIP_UI_API_ROLE need full CRUD
   (real engine runs as MIP_ADMIN_ROLE inside SP; shadow service runs
   as MIP_UI_API_ROLE from FastAPI; smoke and replay use MIP_ADMIN_ROLE)
   ================================================================ */
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.DAILY_POSITION_VERDICT       TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.DAILY_POSITION_SHADOW_REVIEW TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_POSITION_LIFECYCLE    TO ROLE MIP_ADMIN_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.DAILY_POSITION_VERDICT       TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.DAILY_POSITION_SHADOW_REVIEW TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_POSITION_LIFECYCLE    TO ROLE MIP_UI_API_ROLE;

/* ================================================================
   APP_CONFIG seeds — feature flag and tunables
   ================================================================ */
INSERT INTO MIP.APP.APP_CONFIG (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
SELECT 'POSITION_HEALTH_ENABLED', 'true',
       'Daily Position Health V1 feature flag. true = SP_RUN_DAILY_POSITION_VERDICT runs in daily pipeline.'
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = 'POSITION_HEALTH_ENABLED');

INSERT INTO MIP.APP.APP_CONFIG (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
SELECT 'POSITION_HEALTH_SHADOW_ENABLED', 'true',
       'Daily Position Health V1 shadow review flag. true = position_health orchestrator runs after pipeline.'
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = 'POSITION_HEALTH_SHADOW_ENABLED');

INSERT INTO MIP.APP.APP_CONFIG (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
SELECT 'POSITION_HEALTH_DEFAULT_HORIZON_DAYS', '20',
       'Fallback horizon (trading days) used by TIME_EFFICIENCY when POSITION_HOLD_UNTIL and PORTFOLIO_PROFILE_DEFAULT are unavailable.'
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = 'POSITION_HEALTH_DEFAULT_HORIZON_DAYS');

INSERT INTO MIP.APP.APP_CONFIG (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
SELECT 'POSITION_HEALTH_PATH_LOOKBACK_DAYS', '20',
       'Trading-day window for path summary metrics in shadow payload.'
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = 'POSITION_HEALTH_PATH_LOOKBACK_DAYS');

INSERT INTO MIP.APP.APP_CONFIG (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
SELECT 'POSITION_HEALTH_AGENT_NAME', 'POSITION_HEALTH_REVIEW_AGENT',
       'Cortex Agent object name used by the position-health shadow reviewer.'
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = 'POSITION_HEALTH_AGENT_NAME');

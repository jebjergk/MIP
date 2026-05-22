/*  ================================================================
    500_structural_strategy_foundation_tables.sql
    MIP Structural Strategy Framework — Foundation DDL
    Phase 2a: Core tables for the structural setup detection engine
    ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;
USE WAREHOUSE MIP_WH_XS;

-- ----------------------------------------------------------------
-- 1. STRUCTURAL_LEVEL_CACHE
--    Materialized swing highs/lows and support/resistance zones
--    with 5-component significance scoring.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_LEVEL_CACHE (
    LEVEL_ID            NUMBER AUTOINCREMENT PRIMARY KEY,
    SYMBOL              VARCHAR(20)   NOT NULL,
    MARKET_TYPE         VARCHAR(10)   NOT NULL,
    AS_OF_DATE          DATE          NOT NULL,
    LEVEL_TYPE          VARCHAR(20)   NOT NULL,  -- SWING_HIGH, SWING_LOW, SUPPORT_ZONE, RESISTANCE_ZONE
    LEVEL_PRICE         FLOAT         NOT NULL,
    LEVEL_LOW           FLOAT,                   -- zone lower boundary (NULL for point levels)
    LEVEL_HIGH          FLOAT,                   -- zone upper boundary (NULL for point levels)
    TOUCH_COUNT         INTEGER       DEFAULT 1,
    FIRST_TOUCH_DATE    DATE,
    LAST_TOUCH_DATE     DATE,
    ATR_AT_DETECTION    FLOAT,

    -- Significance sub-scores (each 0.0 – 1.0)
    TOUCH_SCORE         FLOAT,
    RECENCY_SCORE       FLOAT,
    REACTION_SCORE      FLOAT,
    HISTORY_SCORE       FLOAT,
    CLUSTER_SCORE       FLOAT,

    -- Composite significance (weighted combination)
    LEVEL_SIGNIFICANCE  FLOAT,

    -- Detailed touch history for audit
    TOUCH_DETAILS       VARIANT,   -- JSON array: [{date, reaction_magnitude, hold_or_break}, ...]

    DETECTOR_VERSION    VARCHAR(20)  DEFAULT '1.0',
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------
-- 2. STRUCTURAL_STATE_LOG
--    Daily structural state per symbol from the state machine.
--    Exactly one row per (SYMBOL, MARKET_TYPE, AS_OF_DATE).
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_STATE_LOG (
    SYMBOL              VARCHAR(20)   NOT NULL,
    MARKET_TYPE         VARCHAR(10)   NOT NULL,
    AS_OF_DATE          DATE          NOT NULL,

    STRUCTURAL_STATE    VARCHAR(30)   NOT NULL,
        -- TREND_UP, TREND_DOWN, RANGE_BOUND, BREAKOUT_EXPANSION,
        -- BREAKDOWN_EXPANSION, PULLBACK_IN_TREND, REVERSAL_FORMING, FAILED_MOVE

    STATE_ENTERED_DATE  DATE,
    BARS_IN_STATE       INTEGER       DEFAULT 1,
    PRIOR_STATE         VARCHAR(30),
    TRANSITION_TRIGGER  VARCHAR(200),  -- human-readable reason for last transition
    KEY_LEVEL_CONTEXT   VARIANT,       -- structural level(s) relevant to the current state
    STATE_CONFIDENCE    FLOAT,         -- 0–1: how cleanly the state matches criteria

    DETECTOR_VERSION    VARCHAR(20)  DEFAULT '1.0',
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (SYMBOL, MARKET_TYPE, AS_OF_DATE)
);

-- ----------------------------------------------------------------
-- 3. STRUCTURAL_REGIME_TAG
--    Daily regime context per symbol (volatility, trend, range).
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_REGIME_TAG (
    SYMBOL              VARCHAR(20)   NOT NULL,
    MARKET_TYPE         VARCHAR(10)   NOT NULL,
    AS_OF_DATE          DATE          NOT NULL,

    VOL_REGIME          VARCHAR(20)   NOT NULL,
        -- LOW_VOL, NORMAL_VOL, HIGH_VOL, EXPANDING_VOL

    TREND_REGIME        VARCHAR(25)   NOT NULL,
        -- STRONG_TREND_UP, MODERATE_TREND_UP, NEUTRAL,
        -- MODERATE_TREND_DOWN, STRONG_TREND_DOWN

    RANGE_REGIME        VARCHAR(15)   NOT NULL,
        -- TIGHT_RANGE, NORMAL_RANGE, WIDE_RANGE

    -- Pre-computed setup compatibility for the current regime
    REGIME_SETUP_COMPAT VARIANT,       -- JSON: {"BREAKOUT_RETEST_LONG": "GOOD", ...}

    ATR_20              FLOAT,         -- 20-bar ATR used for regime computation
    SMA_10_SLOPE        FLOAT,         -- slope of 10-bar simple moving average
    SMA_20              FLOAT,         -- 20-bar SMA value
    RANGE_10_HIGH       FLOAT,         -- 10-bar high
    RANGE_10_LOW        FLOAT,         -- 10-bar low

    DETECTOR_VERSION    VARCHAR(20)  DEFAULT '1.0',
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (SYMBOL, MARKET_TYPE, AS_OF_DATE)
);

-- ----------------------------------------------------------------
-- 4. STRUCTURAL_SETUP_EVENTS
--    Detected structural setup instances. Each row is a single
--    setup detection with full context, evidence, risk layers,
--    and lifecycle tracking.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_SETUP_EVENTS (
    SETUP_EVENT_ID      NUMBER AUTOINCREMENT PRIMARY KEY,

    -- Core identification
    SETUP_FAMILY        VARCHAR(40)   NOT NULL,
        -- BREAKOUT_RETEST_LONG, SUPPORT_WICK_LONG, THREE_BAR_REVERSAL_LONG,
        -- TREND_PULLBACK_LONG, BREAKDOWN_RETEST_SHORT, RESISTANCE_WICK_SHORT,
        -- THREE_BAR_REVERSAL_SHORT, FAILED_BREAKOUT_SHORT
    DIRECTION           VARCHAR(5)    NOT NULL,  -- LONG / SHORT
    SYMBOL              VARCHAR(20)   NOT NULL,
    MARKET_TYPE         VARCHAR(10)   NOT NULL,
    SETUP_DATE          DATE          NOT NULL,

    -- Structural context (from state machine and levels)
    STRUCTURAL_STATE    VARCHAR(30),
    PRIOR_STATE         VARCHAR(30),
    LEVEL_TYPE          VARCHAR(20),   -- SUPPORT, RESISTANCE, BREAKOUT, BREAKDOWN, RECLAIM, FAILED_RECLAIM
    LEVEL_PRICE         FLOAT,
    LEVEL_SIGNIFICANCE  FLOAT,         -- composite significance from STRUCTURAL_LEVEL_CACHE

    -- Entry zone
    ENTRY_ZONE_LOW      FLOAT,
    ENTRY_ZONE_HIGH     FLOAT,

    -- 4-layer invalidation / risk model
    PRICE_INVALIDATION_LEVEL   FLOAT,                -- Layer 1: hard structural price
    INVALIDATION_RULE          VARCHAR(30),           -- Layer 2: SINGLE_CLOSE_BEYOND / CONFIRMED_CLOSE_BEYOND
    INVALIDATION_BUFFER_ATR    FLOAT,                 -- Layer 2: tolerance for confirmed-close rules
    TRAIL_ACTIVATION_CONDITION VARCHAR(100),           -- Layer 3: description
    TRAIL_ACTIVATION_THRESHOLD FLOAT,                 -- Layer 3: numeric threshold
    TRAIL_STYLE                VARCHAR(20),            -- Layer 4: STRUCTURAL / PROGRESS_BASED / HYBRID
    TRAIL_PARAMS               VARIANT,               -- Layer 4: style-specific params (JSON)

    -- Evidence and confidence scores (each 0–1)
    STRUCTURE_CONFIDENCE          FLOAT,
    WICK_CONFIRMATION_SCORE       FLOAT,
    THREE_BAR_CONFIRMATION_SCORE  FLOAT,
    TREND_CONTEXT_SCORE           FLOAT,
    REGIME_COMPAT                 VARCHAR(10),   -- GOOD / NEUTRAL / POOR

    -- Lifecycle (Addendum C)
    SETUP_STATUS         VARCHAR(15)   DEFAULT 'DETECTED',
        -- DETECTED, ELIGIBLE, WAITING, STALE, EXPIRED, INVALIDATED
    STATUS_UPDATED_AT    TIMESTAMP_NTZ,
    BARS_SINCE_DETECTION INTEGER       DEFAULT 0,
    DISTANCE_FROM_ENTRY_ZONE FLOAT,    -- signed distance in ATR units
    ELIGIBLE_SINCE       DATE,
    EXPIRY_DATE          DATE,

    -- Classification
    CONFLUENCE_FLAGS     VARIANT,       -- JSON: fib proximity, volume spike, etc.
    VOLATILITY_CONTEXT   FLOAT,         -- current ATR / normalized vol
    EXPECTED_MOVE_CLASS  VARCHAR(10),   -- SMALL / MEDIUM / LARGE
    RISK_CLASS           VARCHAR(10),   -- LOW / MEDIUM / HIGH

    -- Full feature payload
    FEATURES_JSON        VARIANT,

    DETECTOR_VERSION     VARCHAR(20)  DEFAULT '1.0',
    CREATED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------
-- 5. STRUCTURAL_CONFLICT_LOG
--    Audit trail for conflict resolution when multiple setups
--    exist on the same symbol/date.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_CONFLICT_LOG (
    CONFLICT_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    SYMBOL              VARCHAR(20)   NOT NULL,
    CONFLICT_DATE       DATE          NOT NULL,
    SETUP_EVENT_ID_A    NUMBER        NOT NULL,
    SETUP_EVENT_ID_B    NUMBER        NOT NULL,
    CONFLICT_TYPE       VARCHAR(40)   NOT NULL,
        -- OPPOSING_DIRECTION, SAME_DIRECTION_COMPETING, TEMPORAL_SUPERSEDE
    RESOLUTION          VARCHAR(30)   NOT NULL,
        -- A_WINS, B_WINS, BOTH_CANCELLED, CONFLUENCE
    RESOLUTION_REASON   VARCHAR(500),
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------
-- 6. STRUCTURAL_SETUP_OUTCOMES
--    Path-aware outcome evaluation per setup event and eval window.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_SETUP_OUTCOMES (
    SETUP_EVENT_ID              NUMBER        NOT NULL,
    EVAL_WINDOW                 INTEGER       NOT NULL,   -- 5, 10, or 20 bars

    DIRECTIONAL_SUCCESS         BOOLEAN,
    MEANINGFUL_MOVE_SUCCESS     BOOLEAN,
    MEANINGFUL_MOVE_THRESHOLD   FLOAT,        -- the threshold used (2% stock, vol-adjusted FX)

    MAX_ADVERSE_EXCURSION       FLOAT,        -- worst adverse move (negative %)
    MAX_FAVORABLE_EXCURSION     FLOAT,        -- best favorable move (positive %)
    MFE_MAE_RATIO               FLOAT,

    INVALIDATION_HIT            BOOLEAN,
    BARS_TO_INVALIDATION        INTEGER,
    BARS_TO_CONFIRMATION        INTEGER,      -- bars until >= 0.5% favorable
    BARS_TO_FAVORABLE_THRESHOLD INTEGER,      -- bars until >= meaningful move threshold

    FAILURE_MODE                VARCHAR(30),
        -- SUCCESS, IMMEDIATE_FAILURE, STRUCTURAL_BREAK, WICK_FAILURE,
        -- REVERSAL_AFTER_CONFIRMATION, LATE_FADE, NO_MOVE

    ADVERSE_BEFORE_FAVORABLE    BOOLEAN,      -- did MAE occur before MFE?

    -- Trailing stop simulation results (realized % under each strategy)
    TRAIL_STRATEGY_1_REALIZED   FLOAT,        -- structural trailing
    TRAIL_STRATEGY_2_REALIZED   FLOAT,        -- progress-based trailing
    TRAIL_STRATEGY_3_REALIZED   FLOAT,        -- hybrid trailing

    EVAL_STATUS                 VARCHAR(20)   DEFAULT 'PENDING',
        -- PENDING, SUCCESS, INSUFFICIENT_DATA
    EVALUATED_AT                TIMESTAMP_NTZ,

    PRIMARY KEY (SETUP_EVENT_ID, EVAL_WINDOW)
);

-- ----------------------------------------------------------------
-- 7. STRUCTURAL_SETUP_TRUST
--    Aggregated trust / summary per (SETUP_FAMILY, MARKET_TYPE).
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_SETUP_TRUST (
    SETUP_FAMILY                VARCHAR(40)   NOT NULL,
    MARKET_TYPE                 VARCHAR(10)   NOT NULL,
    EVAL_WINDOW                 INTEGER       NOT NULL,

    N_SETUPS                    INTEGER,
    MEANINGFUL_HIT_RATE         FLOAT,
    DIRECTIONAL_HIT_RATE        FLOAT,
    PATH_SURVIVAL_HIT_RATE      FLOAT,

    AVG_MFE                     FLOAT,
    AVG_MAE                     FLOAT,
    MFE_MAE_RATIO               FLOAT,
    AVG_BARS_TO_THRESHOLD       FLOAT,

    FAILURE_MODE_DISTRIBUTION   VARIANT,      -- JSON: {SUCCESS: 0.4, IMMEDIATE_FAILURE: 0.1, ...}
    BEST_WINDOW                 INTEGER,
    TRUST_LABEL                 VARCHAR(15),  -- TRUSTED, PROVISIONAL, RESEARCH, REJECTED

    TRAIL_STRATEGY_RECOMMENDATION VARCHAR(20),
    EXIT_STYLE_RECOMMENDATION    VARCHAR(20),

    COMPUTED_AT                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (SETUP_FAMILY, MARKET_TYPE, EVAL_WINDOW)
);

-- ----------------------------------------------------------------
-- 8. STRUCTURAL_PATH_STATS
--    Materialized path statistics per setup family for committee context.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_PATH_STATS (
    SETUP_FAMILY                VARCHAR(40)   NOT NULL,
    MARKET_TYPE                 VARCHAR(10)   NOT NULL,
    EVAL_WINDOW                 INTEGER       NOT NULL,

    PERCENTILE_25_MFE           FLOAT,
    MEDIAN_MFE                  FLOAT,
    PERCENTILE_75_MFE           FLOAT,
    PERCENTILE_25_MAE           FLOAT,
    MEDIAN_MAE                  FLOAT,
    PCT_ADVERSE_BEFORE_FAVORABLE FLOAT,
    AVG_BARS_TO_MFE             FLOAT,
    AVG_BARS_TO_MAE             FLOAT,
    GAP_RISK_CONTRIBUTION       FLOAT,

    COMPUTED_AT                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (SETUP_FAMILY, MARKET_TYPE, EVAL_WINDOW)
);

-- ----------------------------------------------------------------
-- 9. STRUCTURAL_RISK_POLICY
--    Per-setup-family risk management configuration (4-layer model).
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_RISK_POLICY (
    SETUP_FAMILY                VARCHAR(40)   NOT NULL,
    DIRECTION                   VARCHAR(5)    NOT NULL,

    PRICE_INVALIDATION_RULE     VARCHAR(30),   -- SINGLE_CLOSE_BEYOND / CONFIRMED_CLOSE_BEYOND
    INVALIDATION_BUFFER_ATR     FLOAT,
    MAX_INVALIDATION_DISTANCE_PCT FLOAT,

    TRAIL_ACTIVATION_TYPE       VARCHAR(30),   -- MFE_RISK_MULTIPLE / STRUCTURAL_LEVEL_BREAK / IMMEDIATE
    TRAIL_ACTIVATION_PARAM      FLOAT,         -- numeric threshold
    TRAIL_STYLE                 VARCHAR(20),   -- STRUCTURAL / PROGRESS_BASED / HYBRID
    TRAIL_PARAMS                VARIANT,       -- breakeven threshold, lock ratios, etc.

    EXIT_STYLE                  VARCHAR(20),   -- OPEN_RUNNER / STAGED_PARTIAL / STRUCTURAL_TARGET
    MAX_HOLD_BARS               INTEGER,
    IS_ACTIVE                   BOOLEAN        DEFAULT TRUE,
    POLICY_VERSION              VARCHAR(10)    DEFAULT '1.0',

    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (SETUP_FAMILY, DIRECTION, POLICY_VERSION)
);

-- ----------------------------------------------------------------
-- 10. STRUCTURAL_TRADE_PROPOSALS
--     New proposal output for the structural strategy framework.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MIP.APP.STRUCTURAL_TRADE_PROPOSALS (
    PROPOSAL_ID                 NUMBER AUTOINCREMENT PRIMARY KEY,
    -- NULL when primary evidence is cross-direction (direction mismatch);
    -- always preserved in PRIMARY_EVIDENCE_SETUP_EVENT_ID.
    SETUP_EVENT_ID              NUMBER,
    PORTFOLIO_ID                NUMBER,
    SYMBOL                      VARCHAR(20)   NOT NULL,
    DIRECTION                   VARCHAR(5)    NOT NULL,
    SETUP_FAMILY                VARCHAR(40)   NOT NULL,

    ENTRY_ZONE_LOW              FLOAT,
    ENTRY_ZONE_HIGH             FLOAT,
    PRICE_INVALIDATION_LEVEL    FLOAT,
    INVALIDATION_RULE           VARCHAR(30),
    TRAIL_STYLE                 VARCHAR(20),
    TRAIL_PARAMS                VARIANT,
    EXIT_STYLE                  VARCHAR(20),
    EXIT_PROFILE                VARCHAR(20),

    STRUCTURE_CONFIDENCE        FLOAT,
    LEVEL_SIGNIFICANCE          FLOAT,
    REGIME_COMPAT               VARCHAR(10),
    MEANINGFUL_HIT_RATE         FLOAT,
    PATH_SURVIVAL_HIT_RATE      FLOAT,
    MFE_MAE_RATIO               FLOAT,
    RISK_CLASS                  VARCHAR(10),

    CONFLICT_RESOLUTION         VARCHAR(30),   -- NULL / STRONG_WINNER / CONFLUENCE_BOOSTED
    RATIONALE_TEXT              VARCHAR(2000),
    COMMITTEE_PAYLOAD           VARIANT,
    STATUS                      VARCHAR(20)    DEFAULT 'PROPOSED',
    CREATED_AT                  TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

    -- Phase 4 board lineage
    BOARD_RUN_ID                VARCHAR(100),
    BOARD_CANDIDATE_ID          NUMBER,
    BOARD_DOSSIER_ID            NUMBER,
    PRIMARY_EVIDENCE_SETUP_EVENT_ID  NUMBER,   -- always the evidence source; direction may differ
    BOARD_FINAL_RANK            NUMBER,
    BOARD_FINAL_VERDICT         VARCHAR(30),
    BOARD_PRIMARY_REASON_CODE   VARCHAR(80),
    BOARD_REASON_CODES          VARIANT,
    BOARD_RATIONALE             VARCHAR(2000),
    BOARD_PAYLOAD_JSON          VARIANT,

    -- Execution policy gate (hard backend gate — LPA/API must reject non-EXECUTABLE)
    -- Values: EXECUTABLE | RESEARCH_ONLY | POLICY_BLOCKED | BROKER_BLOCKED |
    --         RISK_BLOCKED | GEOMETRY_INVALID
    EXECUTION_POLICY_STATUS     VARCHAR(30)    DEFAULT 'EXECUTABLE',
    -- Values: SHORT_LIVE_DISABLED | FX_LIVE_DISABLED | INVALID_LONG_GEOMETRY |
    --         INVALID_SHORT_GEOMETRY | SETUP_EVENT_DIRECTION_MISMATCH | IBKR_NOT_PAPER
    EXECUTION_POLICY_REASON     VARCHAR(80),
    IS_RESEARCH_ONLY            BOOLEAN        DEFAULT FALSE
);

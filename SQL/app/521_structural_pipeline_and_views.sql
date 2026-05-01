/*  ================================================================
    521_structural_pipeline_and_views.sql
    MIP Structural Strategy Framework — Pipeline + Integration Views
    Phase 4b: Daily pipeline orchestrator and views that bridge
    structural proposals into the existing committee/execution flow.
    ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

-- ================================================================
-- 1. V_STRUCTURAL_PROPOSALS_FOR_COMMITTEE
--    Committee-compatible view of active structural proposals
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_PROPOSALS_FOR_COMMITTEE AS
SELECT
    sp.PROPOSAL_ID,
    sp.SYMBOL,
    sp.DIRECTION,
    sp.SETUP_FAMILY,
    COALESCE(sp.COMMITTEE_PAYLOAD:dossier_payload:structure:structural_state::STRING, se.STRUCTURAL_STATE) AS STRUCTURAL_STATE,
    COALESCE(sp.COMMITTEE_PAYLOAD:dossier_payload:identity:as_of_date::DATE, se.SETUP_DATE) AS SETUP_DATE,
    sp.ENTRY_ZONE_LOW,
    sp.ENTRY_ZONE_HIGH,
    sp.PRICE_INVALIDATION_LEVEL,
    sp.INVALIDATION_RULE,
    sp.TRAIL_STYLE,
    sp.EXIT_STYLE,
    sp.STRUCTURE_CONFIDENCE,
    sp.LEVEL_SIGNIFICANCE,
    sp.REGIME_COMPAT,
    sp.MEANINGFUL_HIT_RATE,
    sp.PATH_SURVIVAL_HIT_RATE,
    sp.MFE_MAE_RATIO,
    sp.RISK_CLASS,
    sp.RATIONALE_TEXT,
    sp.COMMITTEE_PAYLOAD,
    sp.STATUS,
    sp.CREATED_AT,
    -- Additional context for committee
    COALESCE(sp.COMMITTEE_PAYLOAD:dossier_payload:regime:tags:vol_regime::STRING, rg.VOL_REGIME) AS VOL_REGIME,
    COALESCE(sp.COMMITTEE_PAYLOAD:dossier_payload:regime:tags:trend_regime::STRING, rg.TREND_REGIME) AS TREND_REGIME,
    COALESCE(sp.COMMITTEE_PAYLOAD:dossier_payload:regime:tags:range_regime::STRING, rg.RANGE_REGIME) AS RANGE_REGIME,
    ps.MEDIAN_MFE,
    ps.MEDIAN_MAE,
    ps.PCT_ADVERSE_BEFORE_FAVORABLE,
    ps.GAP_RISK_CONTRIBUTION,
    tr.TRUST_LABEL,
    tr.BEST_WINDOW,
    tr.N_SETUPS AS TRUST_SAMPLE_SIZE
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se ON se.SETUP_EVENT_ID = sp.SETUP_EVENT_ID
LEFT JOIN MIP.APP.STRUCTURAL_REGIME_TAG rg
  ON rg.SYMBOL = sp.SYMBOL AND rg.AS_OF_DATE = COALESCE(sp.COMMITTEE_PAYLOAD:dossier_payload:identity:as_of_date::DATE, se.SETUP_DATE)
LEFT JOIN MIP.APP.STRUCTURAL_PATH_STATS ps
  ON ps.SETUP_FAMILY = sp.SETUP_FAMILY AND ps.MARKET_TYPE = COALESCE(sp.COMMITTEE_PAYLOAD:dossier_payload:identity:market_type::STRING, se.MARKET_TYPE) AND ps.EVAL_WINDOW = 20
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_TRUST tr
  ON tr.SETUP_FAMILY = sp.SETUP_FAMILY AND tr.MARKET_TYPE = COALESCE(sp.COMMITTEE_PAYLOAD:dossier_payload:identity:market_type::STRING, se.MARKET_TYPE) AND tr.EVAL_WINDOW = 20
WHERE sp.STATUS = 'PROPOSED';

-- ================================================================
-- 2. V_STRUCTURAL_DASHBOARD
--    Summary view for monitoring the structural strategy engine
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_DASHBOARD AS
WITH latest_date AS (
    SELECT MAX(AS_OF_DATE) AS DT FROM MIP.APP.STRUCTURAL_STATE_LOG
),
state_summary AS (
    SELECT STRUCTURAL_STATE, COUNT(*) AS CNT
    FROM MIP.APP.STRUCTURAL_STATE_LOG, latest_date
    WHERE AS_OF_DATE = latest_date.DT
    GROUP BY STRUCTURAL_STATE
),
setup_summary AS (
    SELECT SETUP_FAMILY, DIRECTION, SETUP_STATUS, COUNT(*) AS CNT,
           AVG(STRUCTURE_CONFIDENCE) AS AVG_CONF
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS
    WHERE SETUP_DATE >= DATEADD('day', -5, CURRENT_DATE())
    GROUP BY SETUP_FAMILY, DIRECTION, SETUP_STATUS
),
trust_summary AS (
    SELECT SETUP_FAMILY, MARKET_TYPE, TRUST_LABEL,
           MEANINGFUL_HIT_RATE, PATH_SURVIVAL_HIT_RATE, N_SETUPS
    FROM MIP.APP.STRUCTURAL_SETUP_TRUST
    WHERE EVAL_WINDOW = 20
),
proposal_summary AS (
    SELECT COUNT(*) AS ACTIVE_PROPOSALS,
           COUNT(DISTINCT SYMBOL) AS UNIQUE_SYMBOLS
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    WHERE STATUS = 'PROPOSED'
)
SELECT
    ld.DT AS LATEST_STATE_DATE,
    (SELECT SUM(CNT) FROM state_summary) AS TOTAL_SYMBOLS_WITH_STATE,
    (SELECT COUNT(*) FROM trust_summary WHERE TRUST_LABEL = 'TRUSTED') AS TRUSTED_COMBOS,
    (SELECT COUNT(*) FROM trust_summary WHERE TRUST_LABEL = 'PROVISIONAL') AS PROVISIONAL_COMBOS,
    (SELECT COUNT(*) FROM trust_summary WHERE TRUST_LABEL = 'RESEARCH') AS RESEARCH_COMBOS,
    ps.ACTIVE_PROPOSALS,
    ps.UNIQUE_SYMBOLS AS PROPOSAL_SYMBOLS,
    (SELECT COUNT(*) FROM MIP.APP.STRUCTURAL_SETUP_EVENTS) AS TOTAL_HISTORICAL_SETUPS,
    (SELECT COUNT(*) FROM MIP.APP.STRUCTURAL_SETUP_OUTCOMES) AS TOTAL_OUTCOMES_EVALUATED
FROM latest_date ld, proposal_summary ps;

-- ================================================================
-- 3. SP_RUN_STRUCTURAL_DAILY_PIPELINE
--    Orchestrates the full daily structural pipeline
-- ================================================================
CREATE OR REPLACE PROCEDURE MIP.APP.SP_RUN_STRUCTURAL_DAILY_PIPELINE(
    P_AS_OF_DATE           DATE    DEFAULT NULL,
    P_PORTFOLIO_ID         NUMBER  DEFAULT NULL,
    P_MAX_PROPOSALS        INTEGER DEFAULT 8,  -- Phase 2: raised from 5 to 8
    P_SYMBOL_COOLDOWN_DAYS INTEGER DEFAULT 7   -- Phase 7 PQI Fix 3
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_as_of       DATE := COALESCE(P_AS_OF_DATE, CURRENT_DATE());
    v_run_start   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_result      VARIANT;
    v_levels      VARIANT;
    v_state       VARIANT;
    v_regime      VARIANT;
    v_setups      VARIANT;
    v_lifecycle   VARIANT;
    v_outcomes    VARIANT;
    v_trust       VARIANT;
    v_proposals   VARIANT;
    v_expiry      VARIANT;
BEGIN

    -- Step 0: Expire prior-day PROPOSED rows BEFORE generating new ones.
    --   Enforces "max one daily-bar cycle" lifetime: a structural proposal
    --   created on day D becomes EXPIRED as soon as the day-(D+1) (or later)
    --   pipeline runs. Cascades to open LIVE_ACTIONS (pre-broker only).
    CALL MIP.APP.SP_EXPIRE_STALE_DAILY_PROPOSALS(:v_as_of);
    v_expiry := (SELECT PARSE_JSON('{"status":"done"}'));

    -- Step 1: Detect structural levels
    CALL MIP.APP.SP_DETECT_STRUCTURAL_LEVELS(:v_as_of, NULL, 10, 1.0);
    v_levels := (SELECT PARSE_JSON('{"status":"done"}'));

    -- Step 2: Compute structural state
    CALL MIP.APP.SP_COMPUTE_STRUCTURAL_STATE(:v_as_of, NULL);
    v_state := (SELECT PARSE_JSON('{"status":"done"}'));

    -- Step 3: Compute regime tags
    CALL MIP.APP.SP_COMPUTE_REGIME_TAGS(:v_as_of, NULL);
    v_regime := (SELECT PARSE_JSON('{"status":"done"}'));

    -- Step 4: Detect setups
    CALL MIP.APP.SP_DETECT_STRUCTURAL_SETUPS(:v_as_of, NULL);
    v_setups := (SELECT PARSE_JSON('{"status":"done"}'));

    -- Step 5: Update lifecycle for all active setups
    CALL MIP.APP.SP_UPDATE_SETUP_LIFECYCLE(:v_as_of, NULL);
    v_lifecycle := (SELECT PARSE_JSON('{"status":"done"}'));

    -- Step 6: Evaluate outcomes for setups old enough to have forward bars
    CALL MIP.APP.SP_EVALUATE_STRUCTURAL_OUTCOMES(DATEADD('day', -90, :v_as_of), NULL, NULL);
    v_outcomes := (SELECT PARSE_JSON('{"status":"done"}'));

    -- Step 7: Refresh trust labels and path stats from evaluated outcomes
    CALL MIP.APP.SP_COMPUTE_STRUCTURAL_TRUST();
    v_trust := (SELECT PARSE_JSON('{"status":"done"}'));

    -- Step 8: Generate proposals (now using up-to-date trust data)
    CALL MIP.APP.SP_PROPOSE_STRUCTURAL_TRADES(:P_PORTFOLIO_ID, :P_MAX_PROPOSALS, :v_as_of, :P_SYMBOL_COOLDOWN_DAYS);
    v_proposals := (SELECT PARSE_JSON('{"status":"done"}'));

    -- Summary stats
    RETURN OBJECT_CONSTRUCT(
        'status',      'SUCCESS',
        'as_of_date',  :v_as_of,
        'pipeline_steps', ARRAY_CONSTRUCT(
            'SP_EXPIRE_STALE_DAILY_PROPOSALS',
            'SP_DETECT_STRUCTURAL_LEVELS',
            'SP_COMPUTE_STRUCTURAL_STATE',
            'SP_COMPUTE_REGIME_TAGS',
            'SP_DETECT_STRUCTURAL_SETUPS',
            'SP_UPDATE_SETUP_LIFECYCLE',
            'SP_EVALUATE_STRUCTURAL_OUTCOMES',
            'SP_COMPUTE_STRUCTURAL_TRUST',
            'SP_PROPOSE_STRUCTURAL_TRADES'
        ),
        'elapsed_sec', DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;

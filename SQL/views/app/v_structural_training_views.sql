/*  ================================================================
    v_structural_training_views.sql
    Snowflake views powering the Structural Training Intelligence UI.
    All views query existing structural tables; no new tables needed.
    DIRECTION is carried explicitly through every view grain.
    ETF market type excluded — not tradeable from this platform.
    Trend/symbol views use INNER JOIN to outcomes so only evaluated
    setups contribute to quality metrics.
    ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE WAREHOUSE MIP_WH_XS;

-- ================================================================
-- 1. V_STRUCTURAL_TRAINING_SUMMARY
--    Single-row top-strip KPIs for the page header.
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TRAINING_SUMMARY AS
WITH trust_base AS (
    SELECT
        t.SETUP_FAMILY,
        t.MARKET_TYPE,
        t.TRUST_LABEL,
        t.MEANINGFUL_HIT_RATE,
        t.N_SETUPS,
        t.EVAL_WINDOW,
        t.BEST_WINDOW
    FROM MIP.APP.STRUCTURAL_SETUP_TRUST t
    WHERE t.EVAL_WINDOW = t.BEST_WINDOW
      AND t.MARKET_TYPE != 'ETF'
),
direction_families AS (
    SELECT DISTINCT SETUP_FAMILY, DIRECTION
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS
    WHERE MARKET_TYPE != 'ETF'
),
active_families AS (
    SELECT COUNT(DISTINCT SETUP_FAMILY) AS CNT
    FROM trust_base
    WHERE TRUST_LABEL != 'REJECTED'
),
trust_counts AS (
    SELECT
        COUNT(CASE WHEN TRUST_LABEL = 'TRUSTED'     THEN 1 END) AS TRUSTED_COMBOS,
        COUNT(CASE WHEN TRUST_LABEL = 'PROVISIONAL'  THEN 1 END) AS PROVISIONAL_COMBOS,
        COUNT(CASE WHEN TRUST_LABEL = 'RESEARCH'     THEN 1 END) AS RESEARCH_COMBOS,
        COUNT(CASE WHEN TRUST_LABEL = 'REJECTED'     THEN 1 END) AS REJECTED_COMBOS
    FROM trust_base
),
proposal_eligible AS (
    SELECT COUNT(DISTINCT t.SETUP_FAMILY || '/' || t.MARKET_TYPE) AS CNT
    FROM trust_base t
    JOIN MIP.APP.STRUCTURAL_RISK_POLICY rp
      ON rp.SETUP_FAMILY = t.SETUP_FAMILY AND rp.IS_ACTIVE = TRUE
    WHERE t.TRUST_LABEL IN ('TRUSTED', 'PROVISIONAL')
),
strongest AS (
    SELECT SETUP_FAMILY
    FROM trust_base
    WHERE TRUST_LABEL = 'TRUSTED'
    ORDER BY MEANINGFUL_HIT_RATE DESC NULLS LAST
    LIMIT 1
),
weakest AS (
    SELECT SETUP_FAMILY
    FROM trust_base
    WHERE TRUST_LABEL != 'REJECTED'
    ORDER BY MEANINGFUL_HIT_RATE ASC NULLS LAST
    LIMIT 1
)
SELECT
    af.CNT                          AS TOTAL_FAMILIES_ACTIVE,
    tc.TRUSTED_COMBOS,
    tc.PROVISIONAL_COMBOS,
    tc.RESEARCH_COMBOS,
    tc.REJECTED_COMBOS,
    (SELECT COUNT(*) FROM MIP.APP.STRUCTURAL_SETUP_EVENTS WHERE MARKET_TYPE != 'ETF') AS TOTAL_HISTORICAL_SETUPS,
    pe.CNT                          AS PROPOSAL_ELIGIBLE_FAMILIES,
    (SELECT SETUP_FAMILY FROM strongest)  AS STRONGEST_FAMILY,
    (SELECT SETUP_FAMILY FROM weakest)    AS WEAKEST_FAMILY,
    (SELECT COUNT(DISTINCT d.SETUP_FAMILY || '/' || d.DIRECTION)
     FROM direction_families d)     AS TOTAL_FAMILY_DIRECTION_COMBOS
FROM active_families af, trust_counts tc, proposal_eligible pe;

-- ================================================================
-- 2. V_STRUCTURAL_TRAINING_LEADERBOARD
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TRAINING_LEADERBOARD AS
WITH setup_agg AS (
    SELECT
        se.SETUP_FAMILY,
        se.MARKET_TYPE,
        se.DIRECTION,
        COUNT(*)                                              AS N_SETUPS_TOTAL,
        COUNT(CASE WHEN se.SETUP_STATUS = 'ELIGIBLE' THEN 1 END) AS N_ELIGIBLE,
        MAX(se.SETUP_DATE)                                    AS LATEST_SETUP_DATE,
        MODE(se.RISK_CLASS)                                   AS DOMINANT_RISK_CLASS,
        MODE(se.STRUCTURAL_STATE)                             AS DOMINANT_STATE,
        AVG(CASE WHEN se.REGIME_COMPAT = 'GOOD' THEN 1.0
                  WHEN se.REGIME_COMPAT = 'NEUTRAL' THEN 0.0
                  ELSE 0.0 END)                               AS REGIME_FIT_GOOD_PCT
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
    WHERE se.MARKET_TYPE != 'ETF'
    GROUP BY se.SETUP_FAMILY, se.MARKET_TYPE, se.DIRECTION
),
proposal_counts AS (
    SELECT
        sp.SETUP_FAMILY,
        se.MARKET_TYPE,
        sp.DIRECTION,
        COUNT(*) AS N_PROPOSALS
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
    JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se ON se.SETUP_EVENT_ID = sp.SETUP_EVENT_ID
    WHERE se.MARKET_TYPE != 'ETF'
    GROUP BY sp.SETUP_FAMILY, se.MARKET_TYPE, sp.DIRECTION
),
trust_at_best AS (
    SELECT t.*
    FROM MIP.APP.STRUCTURAL_SETUP_TRUST t
    WHERE t.EVAL_WINDOW = t.BEST_WINDOW
      AND t.MARKET_TYPE != 'ETF'
),
active_policies AS (
    SELECT DISTINCT SETUP_FAMILY, DIRECTION
    FROM MIP.APP.STRUCTURAL_RISK_POLICY
    WHERE IS_ACTIVE = TRUE
),
dominant_failure_cte AS (
    SELECT
        se.SETUP_FAMILY,
        se.MARKET_TYPE,
        se.DIRECTION,
        so.FAILURE_MODE,
        COUNT(*) AS FM_CNT,
        ROW_NUMBER() OVER (PARTITION BY se.SETUP_FAMILY, se.MARKET_TYPE, se.DIRECTION
                           ORDER BY COUNT(*) DESC) AS RN
    FROM MIP.APP.STRUCTURAL_SETUP_OUTCOMES so
    JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se ON se.SETUP_EVENT_ID = so.SETUP_EVENT_ID
    WHERE so.EVAL_STATUS = 'SUCCESS' AND so.FAILURE_MODE IS NOT NULL
      AND se.MARKET_TYPE != 'ETF'
    GROUP BY se.SETUP_FAMILY, se.MARKET_TYPE, se.DIRECTION, so.FAILURE_MODE
),
top_failure AS (
    SELECT SETUP_FAMILY, MARKET_TYPE, DIRECTION, FAILURE_MODE AS DOMINANT_FAILURE_MODE
    FROM dominant_failure_cte
    WHERE RN = 1
)
SELECT
    sa.SETUP_FAMILY,
    sa.MARKET_TYPE,
    sa.DIRECTION,
    COALESCE(tb.BEST_WINDOW, 20)                AS BEST_WINDOW,
    COALESCE(tb.TRUST_LABEL, 'UNKNOWN')         AS TRUST_LABEL,

    CASE
        WHEN sa.N_SETUPS_TOTAL < 10                      THEN 'INSUFFICIENT'
        WHEN sa.N_SETUPS_TOTAL < 20                      THEN 'EMERGING'
        WHEN sa.N_SETUPS_TOTAL < 30                      THEN 'RESEARCHING'
        WHEN sa.N_SETUPS_TOTAL < 40                      THEN 'PROVISIONAL'
        WHEN COALESCE(tb.TRUST_LABEL, '') = 'REJECTED'
             AND sa.N_SETUPS_TOTAL >= 40                  THEN 'DEGRADING'
        ELSE 'TRUSTED'
    END                                          AS MATURITY_LABEL,
    LEAST(100, ROUND(sa.N_SETUPS_TOTAL * 100.0 / 40, 1)) AS MATURITY_PCT,

    sa.N_SETUPS_TOTAL,
    sa.N_ELIGIBLE,
    COALESCE(pc.N_PROPOSALS, 0)                  AS N_PROPOSALS,
    sa.LATEST_SETUP_DATE,

    tb.MEANINGFUL_HIT_RATE,
    tb.DIRECTIONAL_HIT_RATE,
    tb.PATH_SURVIVAL_HIT_RATE,
    tb.MFE_MAE_RATIO,
    tb.AVG_BARS_TO_THRESHOLD,

    ps.MEDIAN_MFE,
    ps.MEDIAN_MAE,
    ps.PCT_ADVERSE_BEFORE_FAVORABLE,
    ps.GAP_RISK_CONTRIBUTION,

    sa.DOMINANT_STATE,
    sa.DOMINANT_RISK_CLASS,
    sa.REGIME_FIT_GOOD_PCT,
    tb.TRAIL_STRATEGY_RECOMMENDATION,
    tb.EXIT_STYLE_RECOMMENDATION,

    CASE
        WHEN COALESCE(tb.TRUST_LABEL, '') IN ('TRUSTED', 'PROVISIONAL')
             AND ap.SETUP_FAMILY IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END                                          AS IS_PROPOSAL_READY,

    tf.DOMINANT_FAILURE_MODE

FROM setup_agg sa
LEFT JOIN trust_at_best tb
  ON tb.SETUP_FAMILY = sa.SETUP_FAMILY AND tb.MARKET_TYPE = sa.MARKET_TYPE
LEFT JOIN MIP.APP.STRUCTURAL_PATH_STATS ps
  ON ps.SETUP_FAMILY = sa.SETUP_FAMILY AND ps.MARKET_TYPE = sa.MARKET_TYPE
  AND ps.EVAL_WINDOW = COALESCE(tb.BEST_WINDOW, 20)
LEFT JOIN proposal_counts pc
  ON pc.SETUP_FAMILY = sa.SETUP_FAMILY AND pc.MARKET_TYPE = sa.MARKET_TYPE
  AND pc.DIRECTION = sa.DIRECTION
LEFT JOIN active_policies ap
  ON ap.SETUP_FAMILY = sa.SETUP_FAMILY AND ap.DIRECTION = sa.DIRECTION
LEFT JOIN top_failure tf
  ON tf.SETUP_FAMILY = sa.SETUP_FAMILY AND tf.MARKET_TYPE = sa.MARKET_TYPE
  AND tf.DIRECTION = sa.DIRECTION;

-- ================================================================
-- 3. V_STRUCTURAL_TRAINING_DETAIL
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TRAINING_DETAIL AS
SELECT
    t.SETUP_FAMILY,
    t.MARKET_TYPE,
    t.EVAL_WINDOW,
    t.N_SETUPS,
    t.MEANINGFUL_HIT_RATE,
    t.DIRECTIONAL_HIT_RATE,
    t.PATH_SURVIVAL_HIT_RATE,
    t.AVG_MFE,
    t.AVG_MAE,
    t.MFE_MAE_RATIO,
    t.AVG_BARS_TO_THRESHOLD,
    t.BEST_WINDOW,
    t.TRUST_LABEL,
    t.TRAIL_STRATEGY_RECOMMENDATION,
    t.EXIT_STYLE_RECOMMENDATION,
    t.FAILURE_MODE_DISTRIBUTION,
    ps.PERCENTILE_25_MFE,
    ps.MEDIAN_MFE,
    ps.PERCENTILE_75_MFE,
    ps.PERCENTILE_25_MAE,
    ps.MEDIAN_MAE,
    ps.PCT_ADVERSE_BEFORE_FAVORABLE,
    ps.AVG_BARS_TO_MFE,
    ps.AVG_BARS_TO_MAE,
    ps.GAP_RISK_CONTRIBUTION,
    trail.AVG_TRAIL_STRUCTURAL,
    trail.AVG_TRAIL_PROGRESS,
    trail.AVG_TRAIL_HYBRID
FROM MIP.APP.STRUCTURAL_SETUP_TRUST t
LEFT JOIN MIP.APP.STRUCTURAL_PATH_STATS ps
  ON ps.SETUP_FAMILY = t.SETUP_FAMILY
  AND ps.MARKET_TYPE = t.MARKET_TYPE
  AND ps.EVAL_WINDOW = t.EVAL_WINDOW
LEFT JOIN (
    SELECT
        se.SETUP_FAMILY,
        se.MARKET_TYPE,
        so.EVAL_WINDOW,
        AVG(so.TRAIL_STRATEGY_1_REALIZED) AS AVG_TRAIL_STRUCTURAL,
        AVG(so.TRAIL_STRATEGY_2_REALIZED) AS AVG_TRAIL_PROGRESS,
        AVG(so.TRAIL_STRATEGY_3_REALIZED) AS AVG_TRAIL_HYBRID
    FROM MIP.APP.STRUCTURAL_SETUP_OUTCOMES so
    JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se ON se.SETUP_EVENT_ID = so.SETUP_EVENT_ID
    WHERE so.EVAL_STATUS = 'SUCCESS'
    GROUP BY se.SETUP_FAMILY, se.MARKET_TYPE, so.EVAL_WINDOW
) trail
  ON trail.SETUP_FAMILY = t.SETUP_FAMILY
  AND trail.MARKET_TYPE = t.MARKET_TYPE
  AND trail.EVAL_WINDOW = t.EVAL_WINDOW
WHERE t.MARKET_TYPE != 'ETF';

-- ================================================================
-- 4. V_STRUCTURAL_TRAINING_FAILURE_DIST
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TRAINING_FAILURE_DIST AS
SELECT
    se.SETUP_FAMILY,
    se.MARKET_TYPE,
    se.DIRECTION,
    so.EVAL_WINDOW,
    so.FAILURE_MODE,
    COUNT(*)                                                  AS CNT,
    ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (
        PARTITION BY se.SETUP_FAMILY, se.MARKET_TYPE, se.DIRECTION, so.EVAL_WINDOW
    ), 0), 1)                                                 AS PCT
FROM MIP.APP.STRUCTURAL_SETUP_OUTCOMES so
JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se ON se.SETUP_EVENT_ID = so.SETUP_EVENT_ID
WHERE so.EVAL_STATUS = 'SUCCESS'
  AND so.FAILURE_MODE IS NOT NULL
  AND se.MARKET_TYPE != 'ETF'
GROUP BY se.SETUP_FAMILY, se.MARKET_TYPE, se.DIRECTION, so.EVAL_WINDOW, so.FAILURE_MODE;

-- ================================================================
-- 5. V_STRUCTURAL_TRAINING_TREND
--    Rolling 60-day windows. INNER JOIN to outcomes so only
--    evaluated setups contribute to quality metrics.
--    Unevaluated setups (no outcome row) are excluded entirely.
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TRAINING_TREND AS
WITH date_spine AS (
    SELECT DATEADD('month', ROW_NUMBER() OVER (ORDER BY 1) - 1, '2025-02-01')::DATE AS PERIOD_END
    FROM TABLE(GENERATOR(ROWCOUNT => 36))
    QUALIFY PERIOD_END <= CURRENT_DATE()
),
windowed AS (
    SELECT
        ds.PERIOD_END,
        se.SETUP_FAMILY,
        se.MARKET_TYPE,
        se.DIRECTION,
        se.SETUP_EVENT_ID,
        so.MEANINGFUL_MOVE_SUCCESS,
        so.DIRECTIONAL_SUCCESS,
        so.INVALIDATION_HIT,
        so.MAX_FAVORABLE_EXCURSION,
        so.MAX_ADVERSE_EXCURSION,
        so.MFE_MAE_RATIO
    FROM date_spine ds
    JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se
      ON se.SETUP_DATE BETWEEN DATEADD('day', -60, ds.PERIOD_END) AND ds.PERIOD_END
      AND se.MARKET_TYPE != 'ETF'
    INNER JOIN MIP.APP.STRUCTURAL_SETUP_OUTCOMES so
      ON so.SETUP_EVENT_ID = se.SETUP_EVENT_ID
      AND so.EVAL_WINDOW = 20
      AND so.EVAL_STATUS = 'SUCCESS'
)
SELECT
    SETUP_FAMILY,
    MARKET_TYPE,
    DIRECTION,
    PERIOD_END,
    DATEADD('day', -60, PERIOD_END)::DATE              AS PERIOD_START,
    COUNT(DISTINCT SETUP_EVENT_ID)                      AS N_SETUPS,
    AVG(CASE WHEN MEANINGFUL_MOVE_SUCCESS THEN 1.0 ELSE 0.0 END)  AS MEANINGFUL_HIT_RATE,
    AVG(CASE WHEN DIRECTIONAL_SUCCESS THEN 1.0 ELSE 0.0 END)      AS DIRECTIONAL_HIT_RATE,
    AVG(CASE WHEN NOT COALESCE(INVALIDATION_HIT, FALSE) THEN 1.0 ELSE 0.0 END) AS PATH_SURVIVAL_RATE,
    AVG(MAX_FAVORABLE_EXCURSION)                        AS AVG_MFE,
    AVG(MAX_ADVERSE_EXCURSION)                          AS AVG_MAE,
    AVG(MFE_MAE_RATIO)                                  AS AVG_MFE_MAE_RATIO
FROM windowed
GROUP BY SETUP_FAMILY, MARKET_TYPE, DIRECTION, PERIOD_END
HAVING COUNT(DISTINCT SETUP_EVENT_ID) >= 3;

-- ================================================================
-- 6. V_STRUCTURAL_TRAINING_SYMBOL
--    INNER JOIN to outcomes so only evaluated setups are counted.
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TRAINING_SYMBOL AS
WITH symbol_stats AS (
    SELECT
        se.SYMBOL,
        se.SETUP_FAMILY,
        se.MARKET_TYPE,
        se.DIRECTION,
        COUNT(*)                                                   AS N_SETUPS,
        AVG(CASE WHEN so.MEANINGFUL_MOVE_SUCCESS THEN 1.0 ELSE 0.0 END) AS SYMBOL_MHR,
        AVG(so.MFE_MAE_RATIO)                                     AS SYMBOL_MFE_MAE
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
    INNER JOIN MIP.APP.STRUCTURAL_SETUP_OUTCOMES so
      ON so.SETUP_EVENT_ID = se.SETUP_EVENT_ID AND so.EVAL_WINDOW = 20 AND so.EVAL_STATUS = 'SUCCESS'
    WHERE se.MARKET_TYPE != 'ETF'
    GROUP BY se.SYMBOL, se.SETUP_FAMILY, se.MARKET_TYPE, se.DIRECTION
),
family_pooled AS (
    SELECT
        SETUP_FAMILY,
        MARKET_TYPE,
        AVG(CASE WHEN so.MEANINGFUL_MOVE_SUCCESS THEN 1.0 ELSE 0.0 END) AS FAMILY_MHR,
        AVG(so.MFE_MAE_RATIO)                                           AS FAMILY_MFE_MAE
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
    INNER JOIN MIP.APP.STRUCTURAL_SETUP_OUTCOMES so
      ON so.SETUP_EVENT_ID = se.SETUP_EVENT_ID AND so.EVAL_WINDOW = 20 AND so.EVAL_STATUS = 'SUCCESS'
    WHERE se.MARKET_TYPE != 'ETF'
    GROUP BY SETUP_FAMILY, MARKET_TYPE
)
SELECT
    ss.SYMBOL,
    ss.SETUP_FAMILY,
    ss.MARKET_TYPE,
    ss.DIRECTION,
    ss.N_SETUPS,
    ss.SYMBOL_MHR,
    ss.SYMBOL_MFE_MAE,
    fp.FAMILY_MHR,
    fp.FAMILY_MFE_MAE,
    ss.SYMBOL_MHR - fp.FAMILY_MHR                     AS VS_FAMILY_MHR_DIFF,
    CASE
        WHEN ss.N_SETUPS < 5                           THEN 'INSUFFICIENT'
        WHEN ABS(ss.SYMBOL_MHR - fp.FAMILY_MHR) < 0.05 THEN 'ALIGNED'
        WHEN ss.SYMBOL_MHR > fp.FAMILY_MHR             THEN 'STRONGER'
        ELSE 'WEAKER'
    END                                                 AS EVIDENCE_STRENGTH
FROM symbol_stats ss
LEFT JOIN family_pooled fp
  ON fp.SETUP_FAMILY = ss.SETUP_FAMILY AND fp.MARKET_TYPE = ss.MARKET_TYPE;

-- ================================================================
-- 7. V_STRUCTURAL_TRAINING_EXAMPLES
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TRAINING_EXAMPLES AS
SELECT
    se.SETUP_EVENT_ID,
    se.SYMBOL,
    se.SETUP_DATE,
    se.SETUP_FAMILY,
    se.DIRECTION,
    se.MARKET_TYPE,
    se.STRUCTURAL_STATE,
    se.SETUP_STATUS,
    se.ENTRY_ZONE_LOW,
    se.ENTRY_ZONE_HIGH,
    se.PRICE_INVALIDATION_LEVEL,
    se.STRUCTURE_CONFIDENCE,
    se.LEVEL_SIGNIFICANCE,
    se.REGIME_COMPAT,
    se.RISK_CLASS,
    rt.VOL_REGIME,
    rt.TREND_REGIME,
    rt.RANGE_REGIME,
    so.MAX_FAVORABLE_EXCURSION   AS MFE_PCT,
    so.MAX_ADVERSE_EXCURSION     AS MAE_PCT,
    so.FAILURE_MODE,
    so.MEANINGFUL_MOVE_SUCCESS,
    CASE WHEN sp.PROPOSAL_ID IS NOT NULL THEN TRUE ELSE FALSE END AS BECAME_PROPOSAL,
    sp.STATUS                    AS PROPOSAL_STATUS
FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_OUTCOMES so
  ON so.SETUP_EVENT_ID = se.SETUP_EVENT_ID AND so.EVAL_WINDOW = 20
LEFT JOIN MIP.APP.STRUCTURAL_REGIME_TAG rt
  ON rt.SYMBOL = se.SYMBOL AND rt.MARKET_TYPE = se.MARKET_TYPE AND rt.AS_OF_DATE = se.SETUP_DATE
LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
  ON sp.SETUP_EVENT_ID = se.SETUP_EVENT_ID
WHERE se.MARKET_TYPE != 'ETF';

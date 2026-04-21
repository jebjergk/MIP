/* ================================================================
   v_committee_bakeoff.sql
   Read-only views for the Committee Bake-off Sidecar.
   Source tables (sidecar only):
     - MIP.APP.COMMITTEE_BAKEOFF_LATCH
     - MIP.APP.COMMITTEE_BAKEOFF_OUTCOME
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA MART;

-- ----------------------------------------------------------------
-- 1. Per-opportunity wide row (real + shadow side-by-side)
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_COMMITTEE_BAKEOFF_OPPORTUNITIES AS
WITH outcomes AS (
    SELECT * FROM MIP.APP.COMMITTEE_BAKEOFF_OUTCOME
),
latches AS (
    SELECT * FROM MIP.APP.COMMITTEE_BAKEOFF_LATCH
)
SELECT
    p.PROPOSAL_ID,
    COALESCE(l_real.SYMBOL, l_shadow.SYMBOL, o_real.SYMBOL, o_shadow.SYMBOL)        AS SYMBOL,
    COALESCE(l_real.DIRECTION, l_shadow.DIRECTION, o_real.DIRECTION, o_shadow.DIRECTION) AS DIRECTION,
    LEAST(
        COALESCE(l_real.DECISION_TS,   l_shadow.DECISION_TS),
        COALESCE(l_shadow.DECISION_TS, l_real.DECISION_TS)
    )                                               AS FIRST_DECISION_TS,

    -- Real side
    l_real.RAW_STANCE        AS REAL_RAW_STANCE,
    l_real.NORMALIZED_ACTION AS REAL_ACTION,
    l_real.CONFIDENCE        AS REAL_CONFIDENCE,
    l_real.CONFIG_STATUS     AS REAL_CONFIG_STATUS,
    l_real.CONFIG_STATUS_REASON AS REAL_CONFIG_REASON,
    l_real.LATCH_REASON      AS REAL_LATCH_REASON,
    l_real.LATCH_SOURCE_TABLE AS REAL_LATCH_SRC,
    l_real.DECISION_TS       AS REAL_DECISION_TS,
    o_real.RAW_OUTCOME       AS REAL_OUTCOME,
    o_real.REALIZED_RETURN_PCT AS REAL_RETURN,
    o_real.MAX_FAVORABLE_PCT AS REAL_MFE,
    o_real.MAX_ADVERSE_PCT   AS REAL_MAE,
    o_real.SCORING_EXCLUDED_FLAG  AS REAL_EXCLUDED,
    o_real.SCORING_EXCLUDED_REASON AS REAL_EXCLUDED_REASON,

    -- Shadow side
    l_shadow.RAW_STANCE        AS SHADOW_RAW_STANCE,
    l_shadow.NORMALIZED_ACTION AS SHADOW_ACTION,
    l_shadow.CONFIDENCE        AS SHADOW_CONFIDENCE,
    l_shadow.CONFIG_STATUS     AS SHADOW_CONFIG_STATUS,
    l_shadow.CONFIG_STATUS_REASON AS SHADOW_CONFIG_REASON,
    l_shadow.LATCH_REASON      AS SHADOW_LATCH_REASON,
    l_shadow.LATCH_SOURCE_TABLE AS SHADOW_LATCH_SRC,
    l_shadow.DECISION_TS       AS SHADOW_DECISION_TS,
    o_shadow.RAW_OUTCOME       AS SHADOW_OUTCOME,
    o_shadow.REALIZED_RETURN_PCT AS SHADOW_RETURN,
    o_shadow.MAX_FAVORABLE_PCT AS SHADOW_MFE,
    o_shadow.MAX_ADVERSE_PCT   AS SHADOW_MAE,
    o_shadow.SCORING_EXCLUDED_FLAG  AS SHADOW_EXCLUDED,
    o_shadow.SCORING_EXCLUDED_REASON AS SHADOW_EXCLUDED_REASON,

    -- Comparison
    COALESCE(o_real.COMPARISON_LABEL, o_shadow.COMPARISON_LABEL) AS COMPARISON_LABEL,
    CASE
        WHEN COALESCE(l_real.RAW_STANCE,'') <> COALESCE(l_shadow.RAW_STANCE,'') THEN TRUE
        ELSE FALSE
    END                       AS BOARDS_DISAGREE_RAW,
    CASE
        WHEN COALESCE(l_real.NORMALIZED_ACTION,'') <> COALESCE(l_shadow.NORMALIZED_ACTION,'') THEN TRUE
        ELSE FALSE
    END                       AS BOARDS_DISAGREE_NORMALIZED
  FROM (SELECT DISTINCT PROPOSAL_ID FROM latches) p
  LEFT JOIN latches  l_real    ON l_real.PROPOSAL_ID    = p.PROPOSAL_ID AND l_real.BOARD_KIND   = 'REAL'
  LEFT JOIN latches  l_shadow  ON l_shadow.PROPOSAL_ID  = p.PROPOSAL_ID AND l_shadow.BOARD_KIND = 'SHADOW'
  LEFT JOIN outcomes o_real    ON o_real.PROPOSAL_ID    = p.PROPOSAL_ID AND o_real.BOARD_KIND   = 'REAL'
  LEFT JOIN outcomes o_shadow  ON o_shadow.PROPOSAL_ID  = p.PROPOSAL_ID AND o_shadow.BOARD_KIND = 'SHADOW';

COMMENT ON VIEW MIP.MART.V_COMMITTEE_BAKEOFF_OPPORTUNITIES IS
    'Per-opportunity wide row of real + shadow latches and outcomes for the bake-off page.';

-- ----------------------------------------------------------------
-- 2. Overall scorecard (KPIs)
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_COMMITTEE_BAKEOFF_SCORECARD AS
WITH base AS (
    SELECT * FROM MIP.MART.V_COMMITTEE_BAKEOFF_OPPORTUNITIES
),
included AS (
    SELECT * FROM base
     WHERE COALESCE(REAL_EXCLUDED,  FALSE) = FALSE
       AND COALESCE(SHADOW_EXCLUDED, FALSE) = FALSE
)
SELECT
    -- Population
    (SELECT COUNT(*) FROM base)                                                 AS TOTAL_OPPORTUNITIES,
    (SELECT COUNT(*) FROM base WHERE REAL_EXCLUDED OR SHADOW_EXCLUDED)         AS EXCLUDED_OPPORTUNITIES,
    (SELECT COUNT(*) FROM included)                                              AS SCORED_OPPORTUNITIES,
    (SELECT COUNT(*) FROM base WHERE BOARDS_DISAGREE_NORMALIZED)                 AS DISAGREE_NORMALIZED_COUNT,
    (SELECT COUNT(*) FROM base WHERE BOARDS_DISAGREE_RAW)                        AS DISAGREE_RAW_COUNT,

    -- Real summary (entered only)
    (SELECT COUNT(*) FROM included WHERE REAL_ACTION='ENTER')                    AS REAL_ENTER_COUNT,
    (SELECT COUNT(*) FROM included WHERE REAL_ACTION='ENTER' AND REAL_RETURN >= 0) AS REAL_WIN_COUNT,
    (SELECT COUNT(*) FROM included WHERE REAL_ACTION='ENTER' AND REAL_RETURN <  0) AS REAL_LOSS_COUNT,
    (SELECT AVG(REAL_RETURN) FROM included WHERE REAL_ACTION='ENTER')            AS REAL_AVG_RETURN,
    (SELECT SUM(REAL_RETURN) FROM included WHERE REAL_ACTION='ENTER')            AS REAL_SUM_RETURN,

    -- Shadow summary (entered only)
    (SELECT COUNT(*) FROM included WHERE SHADOW_ACTION='ENTER')                  AS SHADOW_ENTER_COUNT,
    (SELECT COUNT(*) FROM included WHERE SHADOW_ACTION='ENTER' AND SHADOW_RETURN >= 0) AS SHADOW_WIN_COUNT,
    (SELECT COUNT(*) FROM included WHERE SHADOW_ACTION='ENTER' AND SHADOW_RETURN <  0) AS SHADOW_LOSS_COUNT,
    (SELECT AVG(SHADOW_RETURN) FROM included WHERE SHADOW_ACTION='ENTER')        AS SHADOW_AVG_RETURN,
    (SELECT SUM(SHADOW_RETURN) FROM included WHERE SHADOW_ACTION='ENTER')        AS SHADOW_SUM_RETURN,

    -- Hit rates
    DIV0(
        (SELECT COUNT(*) FROM included WHERE REAL_ACTION='ENTER' AND REAL_RETURN >= 0),
        (SELECT NULLIF(COUNT(*),0) FROM included WHERE REAL_ACTION='ENTER')
    )                                                                            AS REAL_HIT_RATE,
    DIV0(
        (SELECT COUNT(*) FROM included WHERE SHADOW_ACTION='ENTER' AND SHADOW_RETURN >= 0),
        (SELECT NULLIF(COUNT(*),0) FROM included WHERE SHADOW_ACTION='ENTER')
    )                                                                            AS SHADOW_HIT_RATE,

    CURRENT_TIMESTAMP() AS COMPUTED_AT;

COMMENT ON VIEW MIP.MART.V_COMMITTEE_BAKEOFF_SCORECARD IS
    'Top-strip KPIs for the Committee Bake-off page.';

-- ----------------------------------------------------------------
-- 3. Comparative label distribution
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_COMMITTEE_BAKEOFF_LABEL_DIST AS
SELECT
    COMPARISON_LABEL,
    COUNT(*) AS N,
    AVG(COALESCE(REAL_RETURN, 0))   AS AVG_REAL_RETURN,
    AVG(COALESCE(SHADOW_RETURN, 0)) AS AVG_SHADOW_RETURN
  FROM MIP.MART.V_COMMITTEE_BAKEOFF_OPPORTUNITIES
 GROUP BY COMPARISON_LABEL
 ORDER BY N DESC;

-- ----------------------------------------------------------------
-- 4. Disagreements only
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_COMMITTEE_BAKEOFF_DISAGREEMENTS AS
SELECT *
  FROM MIP.MART.V_COMMITTEE_BAKEOFF_OPPORTUNITIES
 WHERE BOARDS_DISAGREE_NORMALIZED = TRUE
    OR BOARDS_DISAGREE_RAW        = TRUE
 ORDER BY FIRST_DECISION_TS DESC;

-- ----------------------------------------------------------------
-- 5. Month-to-date recommendation roll-up
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_COMMITTEE_BAKEOFF_MTD AS
WITH s AS (SELECT * FROM MIP.MART.V_COMMITTEE_BAKEOFF_SCORECARD)
SELECT
    s.*,
    CASE
        WHEN SCORED_OPPORTUNITIES = 0 THEN 'INSUFFICIENT_DATA'
        WHEN ABS(COALESCE(REAL_AVG_RETURN,0) - COALESCE(SHADOW_AVG_RETURN,0)) < 0.0025
            THEN 'TIE'  -- within 25 bps avg
        WHEN COALESCE(REAL_AVG_RETURN,0)  > COALESCE(SHADOW_AVG_RETURN,0) THEN 'PREFER_REAL'
        WHEN COALESCE(SHADOW_AVG_RETURN,0) > COALESCE(REAL_AVG_RETURN,0) THEN 'PREFER_SHADOW'
        ELSE 'TIE'
    END AS MTD_RECOMMENDATION,
    COALESCE(REAL_AVG_RETURN,0)   - COALESCE(SHADOW_AVG_RETURN,0) AS AVG_RETURN_DELTA_REAL_MINUS_SHADOW,
    COALESCE(REAL_HIT_RATE,0)     - COALESCE(SHADOW_HIT_RATE,0)   AS HIT_RATE_DELTA_REAL_MINUS_SHADOW
  FROM s;

COMMENT ON VIEW MIP.MART.V_COMMITTEE_BAKEOFF_MTD IS
    'Month-to-date roll-up + simple recommendation (PREFER_REAL/PREFER_SHADOW/TIE) for the bake-off page.';

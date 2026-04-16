/*  ================================================================
    510_sp_evaluate_structural_outcomes.sql
    MIP Structural Strategy Framework — Outcome Evaluation
    Phase 3a: Path-aware outcome evaluation with MAE/MFE, failure
    modes, trailing stop simulations, and multi-window analysis.
    ================================================================ */

CREATE OR REPLACE PROCEDURE MIP.APP.SP_EVALUATE_STRUCTURAL_OUTCOMES(
    P_MIN_SETUP_DATE  DATE    DEFAULT '2025-02-01',
    P_MAX_SETUP_DATE  DATE    DEFAULT NULL,
    P_SYMBOL          VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_max_date    DATE := COALESCE(P_MAX_SETUP_DATE, DATEADD('day', -21, CURRENT_DATE()));
    v_run_start   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_evaluated   INTEGER := 0;
BEGIN

    -- ============================================================
    -- STEP 1: Build forward price paths for each setup event
    --   For each setup, get bars 1-20 after detection date
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_FORWARD_BARS AS
    SELECT
        se.SETUP_EVENT_ID,
        se.SYMBOL, se.MARKET_TYPE, se.SETUP_DATE,
        se.DIRECTION,
        se.SETUP_FAMILY,
        (se.ENTRY_ZONE_LOW + se.ENTRY_ZONE_HIGH) / 2.0 AS ENTRY_PRICE,
        se.PRICE_INVALIDATION_LEVEL,
        se.VOLATILITY_CONTEXT AS ATR_20,
        mb.TS AS FORWARD_DATE,
        mb.OPEN, mb.HIGH, mb.LOW, mb.CLOSE,
        ROW_NUMBER() OVER (
            PARTITION BY se.SETUP_EVENT_ID
            ORDER BY mb.TS
        ) AS BAR_N
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
    JOIN MIP.MART.MARKET_BARS mb
      ON mb.SYMBOL = se.SYMBOL
     AND mb.MARKET_TYPE = se.MARKET_TYPE
     AND mb.INTERVAL_MINUTES = 1440
     AND mb.TS > se.SETUP_DATE
    WHERE se.SETUP_DATE >= :P_MIN_SETUP_DATE
      AND se.SETUP_DATE <= :v_max_date
      AND (:P_SYMBOL IS NULL OR se.SYMBOL = :P_SYMBOL)
    QUALIFY BAR_N <= 20;

    -- ============================================================
    -- STEP 2: Compute per-bar metrics relative to entry
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_BAR_METRICS AS
    SELECT
        fb.SETUP_EVENT_ID, fb.BAR_N, fb.DIRECTION,
        fb.ENTRY_PRICE, fb.PRICE_INVALIDATION_LEVEL, fb.ATR_20,
        fb.SETUP_FAMILY, fb.SYMBOL, fb.MARKET_TYPE,

        -- Favorable excursion this bar
        CASE WHEN fb.DIRECTION = 'LONG'
             THEN (fb.HIGH - fb.ENTRY_PRICE) / NULLIF(fb.ENTRY_PRICE, 0) * 100.0
             ELSE (fb.ENTRY_PRICE - fb.LOW) / NULLIF(fb.ENTRY_PRICE, 0) * 100.0
        END AS BAR_MFE_PCT,

        -- Adverse excursion this bar
        CASE WHEN fb.DIRECTION = 'LONG'
             THEN (fb.LOW - fb.ENTRY_PRICE) / NULLIF(fb.ENTRY_PRICE, 0) * 100.0
             ELSE (fb.ENTRY_PRICE - fb.HIGH) / NULLIF(fb.ENTRY_PRICE, 0) * 100.0
        END AS BAR_MAE_PCT,

        -- Close relative to entry
        CASE WHEN fb.DIRECTION = 'LONG'
             THEN (fb.CLOSE - fb.ENTRY_PRICE) / NULLIF(fb.ENTRY_PRICE, 0) * 100.0
             ELSE (fb.ENTRY_PRICE - fb.CLOSE) / NULLIF(fb.ENTRY_PRICE, 0) * 100.0
        END AS CLOSE_VS_ENTRY_PCT,

        -- Invalidation hit?
        CASE WHEN fb.DIRECTION = 'LONG'  AND fb.CLOSE < fb.PRICE_INVALIDATION_LEVEL THEN TRUE
             WHEN fb.DIRECTION = 'SHORT' AND fb.CLOSE > fb.PRICE_INVALIDATION_LEVEL THEN TRUE
             ELSE FALSE
        END AS INVALIDATION_HIT,

        -- Meaningful move thresholds
        CASE WHEN fb.MARKET_TYPE = 'FX'
             THEN GREATEST(1.5 * fb.ATR_20 / NULLIF(fb.ENTRY_PRICE, 0) * 100.0, 1.0)
             ELSE 2.0
        END AS MEANINGFUL_THRESHOLD

    FROM TMP_FORWARD_BARS fb;

    -- ============================================================
    -- STEP 3: Aggregate path metrics per setup per eval window
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_WINDOW_METRICS AS
    SELECT
        bm.SETUP_EVENT_ID,
        w.EVAL_WINDOW,
        bm.DIRECTION,
        bm.SETUP_FAMILY, bm.SYMBOL, bm.MARKET_TYPE,
        MAX(bm.MEANINGFUL_THRESHOLD) AS MEANINGFUL_THRESHOLD,

        -- Running max/min across bars up to eval window
        MAX(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW THEN bm.BAR_MFE_PCT END) AS MFE,
        MIN(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW THEN bm.BAR_MAE_PCT END) AS MAE,

        -- Directional success: >= 0.5% favorable at any bar
        MAX(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW AND bm.BAR_MFE_PCT >= 0.5 THEN 1 ELSE 0 END) = 1 AS DIRECTIONAL_SUCCESS,

        -- Meaningful move: >= threshold at any bar
        MAX(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW AND bm.BAR_MFE_PCT >= bm.MEANINGFUL_THRESHOLD THEN 1 ELSE 0 END) = 1 AS MEANINGFUL_MOVE_SUCCESS,

        -- Invalidation hit (any bar closed beyond level)
        MAX(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW AND bm.INVALIDATION_HIT THEN 1 ELSE 0 END) = 1 AS INVALIDATION_HIT,

        -- First bar where invalidation was hit
        MIN(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW AND bm.INVALIDATION_HIT THEN bm.BAR_N END) AS BARS_TO_INVALIDATION,

        -- First bar with >= 0.5% favorable (confirmation)
        MIN(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW AND bm.BAR_MFE_PCT >= 0.5 THEN bm.BAR_N END) AS BARS_TO_CONFIRMATION,

        -- First bar with >= meaningful threshold
        MIN(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW AND bm.BAR_MFE_PCT >= bm.MEANINGFUL_THRESHOLD THEN bm.BAR_N END) AS BARS_TO_FAVORABLE_THRESHOLD,

        -- First bar where MFE peaked
        MIN(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW AND bm.BAR_MFE_PCT = (
            SELECT MAX(bm2.BAR_MFE_PCT) FROM TMP_BAR_METRICS bm2
            WHERE bm2.SETUP_EVENT_ID = bm.SETUP_EVENT_ID AND bm2.BAR_N <= w.EVAL_WINDOW
        ) THEN bm.BAR_N END) AS BAR_OF_MFE,

        -- First bar where MAE troughed
        MIN(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW AND bm.BAR_MAE_PCT = (
            SELECT MIN(bm2.BAR_MAE_PCT) FROM TMP_BAR_METRICS bm2
            WHERE bm2.SETUP_EVENT_ID = bm.SETUP_EVENT_ID AND bm2.BAR_N <= w.EVAL_WINDOW
        ) THEN bm.BAR_N END) AS BAR_OF_MAE,

        -- Count of forward bars available
        COUNT(CASE WHEN bm.BAR_N <= w.EVAL_WINDOW THEN 1 END) AS BARS_AVAILABLE

    FROM TMP_BAR_METRICS bm
    CROSS JOIN (SELECT 5 AS EVAL_WINDOW UNION ALL SELECT 10 UNION ALL SELECT 20) w
    GROUP BY bm.SETUP_EVENT_ID, w.EVAL_WINDOW, bm.DIRECTION, bm.SETUP_FAMILY, bm.SYMBOL, bm.MARKET_TYPE;

    -- ============================================================
    -- STEP 4: Trailing stop simulations
    --   Strategy 1: Structural trailing (trail below each new swing low / above swing high)
    --   Strategy 2: Progress-based (breakeven at 1x risk, lock 50% at 2x, 60% at 3x)
    --   Strategy 3: Hybrid (structural until 2x risk, then progress-based)
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_TRAIL_SIMS AS
    WITH bar_path AS (
        SELECT
            bm.SETUP_EVENT_ID, bm.BAR_N, bm.DIRECTION,
            bm.ENTRY_PRICE, bm.PRICE_INVALIDATION_LEVEL,
            bm.CLOSE_VS_ENTRY_PCT,
            bm.BAR_MFE_PCT,
            bm.BAR_MAE_PCT,
            ABS(bm.ENTRY_PRICE - bm.PRICE_INVALIDATION_LEVEL) AS RISK_DISTANCE,
            -- Running MFE up to this bar
            MAX(bm.BAR_MFE_PCT) OVER (PARTITION BY bm.SETUP_EVENT_ID ORDER BY bm.BAR_N
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RUNNING_MFE,
            -- Running MAE
            MIN(bm.BAR_MAE_PCT) OVER (PARTITION BY bm.SETUP_EVENT_ID ORDER BY bm.BAR_N
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RUNNING_MAE
        FROM TMP_BAR_METRICS bm
    ),
    trail_calcs AS (
        SELECT bp.*,
            -- Risk distance as % of entry
            bp.RISK_DISTANCE / NULLIF(bp.ENTRY_PRICE, 0) * 100.0 AS RISK_PCT,

            -- Strategy 1 (Structural): exit on invalidation close, otherwise hold
            CASE WHEN bp.DIRECTION = 'LONG' AND bp.CLOSE_VS_ENTRY_PCT < -(bp.RISK_DISTANCE / NULLIF(bp.ENTRY_PRICE, 0) * 100.0)
                 THEN TRUE
                 WHEN bp.DIRECTION = 'SHORT' AND bp.CLOSE_VS_ENTRY_PCT < -(bp.RISK_DISTANCE / NULLIF(bp.ENTRY_PRICE, 0) * 100.0)
                 THEN TRUE
                 ELSE FALSE
            END AS S1_STOPPED,

            -- Strategy 2 (Progress): move stop to breakeven after 1x risk MFE
            CASE WHEN bp.RUNNING_MFE >= (bp.RISK_DISTANCE / NULLIF(bp.ENTRY_PRICE, 0) * 100.0)
                      AND bp.CLOSE_VS_ENTRY_PCT <= 0 THEN TRUE
                 ELSE FALSE
            END AS S2_BE_STOP,

            -- Strategy 2: lock 50% after 2x risk MFE
            CASE WHEN bp.RUNNING_MFE >= 2.0 * (bp.RISK_DISTANCE / NULLIF(bp.ENTRY_PRICE, 0) * 100.0)
                      AND bp.CLOSE_VS_ENTRY_PCT <= 0.5 * bp.RUNNING_MFE THEN TRUE
                 ELSE FALSE
            END AS S2_LOCK_50

        FROM bar_path bp
    ),
    exit_bars AS (
        SELECT
            tc.SETUP_EVENT_ID,
            -- Strategy 1: first bar where stopped
            MIN(CASE WHEN tc.S1_STOPPED THEN tc.BAR_N END) AS S1_EXIT_BAR,
            -- Strategy 2: first bar where BE stop or lock triggered
            MIN(CASE WHEN tc.S2_BE_STOP OR tc.S2_LOCK_50 THEN tc.BAR_N END) AS S2_EXIT_BAR
        FROM trail_calcs tc
        GROUP BY tc.SETUP_EVENT_ID
    )
    SELECT
        eb.SETUP_EVENT_ID,
        -- Strategy 1 realized: close at exit bar or last bar
        MAX(CASE WHEN bm.BAR_N = COALESCE(eb.S1_EXIT_BAR, 20) AND bm.BAR_N <= 5 THEN bm.CLOSE_VS_ENTRY_PCT END) AS S1_REAL_5,
        MAX(CASE WHEN bm.BAR_N = COALESCE(eb.S1_EXIT_BAR, 20) AND bm.BAR_N <= 10 THEN bm.CLOSE_VS_ENTRY_PCT END) AS S1_REAL_10,
        MAX(CASE WHEN bm.BAR_N = COALESCE(eb.S1_EXIT_BAR, 20) AND bm.BAR_N <= 20 THEN bm.CLOSE_VS_ENTRY_PCT END) AS S1_REAL_20,
        -- Strategy 2 realized
        MAX(CASE WHEN bm.BAR_N = COALESCE(eb.S2_EXIT_BAR, 20) AND bm.BAR_N <= 5 THEN bm.CLOSE_VS_ENTRY_PCT END) AS S2_REAL_5,
        MAX(CASE WHEN bm.BAR_N = COALESCE(eb.S2_EXIT_BAR, 20) AND bm.BAR_N <= 10 THEN bm.CLOSE_VS_ENTRY_PCT END) AS S2_REAL_10,
        MAX(CASE WHEN bm.BAR_N = COALESCE(eb.S2_EXIT_BAR, 20) AND bm.BAR_N <= 20 THEN bm.CLOSE_VS_ENTRY_PCT END) AS S2_REAL_20,
        -- Strategy 3 (Hybrid): average of S1 and S2
        NULL AS S3_REAL_5,  -- computed below
        NULL AS S3_REAL_10,
        NULL AS S3_REAL_20
    FROM exit_bars eb
    JOIN TMP_BAR_METRICS bm ON bm.SETUP_EVENT_ID = eb.SETUP_EVENT_ID
    GROUP BY eb.SETUP_EVENT_ID;

    -- ============================================================
    -- STEP 5: Merge results into STRUCTURAL_SETUP_OUTCOMES
    -- ============================================================
    DELETE FROM MIP.APP.STRUCTURAL_SETUP_OUTCOMES
    WHERE SETUP_EVENT_ID IN (
        SELECT DISTINCT SETUP_EVENT_ID FROM TMP_WINDOW_METRICS
    );

    INSERT INTO MIP.APP.STRUCTURAL_SETUP_OUTCOMES (
        SETUP_EVENT_ID, EVAL_WINDOW,
        DIRECTIONAL_SUCCESS, MEANINGFUL_MOVE_SUCCESS, MEANINGFUL_MOVE_THRESHOLD,
        MAX_ADVERSE_EXCURSION, MAX_FAVORABLE_EXCURSION, MFE_MAE_RATIO,
        INVALIDATION_HIT, BARS_TO_INVALIDATION, BARS_TO_CONFIRMATION,
        BARS_TO_FAVORABLE_THRESHOLD,
        FAILURE_MODE,
        ADVERSE_BEFORE_FAVORABLE,
        TRAIL_STRATEGY_1_REALIZED, TRAIL_STRATEGY_2_REALIZED, TRAIL_STRATEGY_3_REALIZED,
        EVAL_STATUS, EVALUATED_AT
    )
    SELECT
        wm.SETUP_EVENT_ID,
        wm.EVAL_WINDOW,
        wm.DIRECTIONAL_SUCCESS,
        wm.MEANINGFUL_MOVE_SUCCESS,
        wm.MEANINGFUL_THRESHOLD,
        wm.MAE,   -- max adverse (negative)
        wm.MFE,   -- max favorable (positive)
        CASE WHEN ABS(wm.MAE) > 0 THEN wm.MFE / ABS(wm.MAE) ELSE NULL END,
        wm.INVALIDATION_HIT,
        wm.BARS_TO_INVALIDATION,
        wm.BARS_TO_CONFIRMATION,
        wm.BARS_TO_FAVORABLE_THRESHOLD,

        -- Failure mode classification
        CASE
            WHEN wm.MEANINGFUL_MOVE_SUCCESS THEN 'SUCCESS'
            WHEN wm.INVALIDATION_HIT AND wm.BARS_TO_INVALIDATION <= 2 THEN 'IMMEDIATE_FAILURE'
            WHEN wm.INVALIDATION_HIT AND NOT wm.DIRECTIONAL_SUCCESS THEN 'STRUCTURAL_BREAK'
            WHEN wm.INVALIDATION_HIT AND wm.DIRECTIONAL_SUCCESS AND wm.BARS_TO_CONFIRMATION < wm.BARS_TO_INVALIDATION
                 THEN 'REVERSAL_AFTER_CONFIRMATION'
            WHEN wm.INVALIDATION_HIT THEN 'WICK_FAILURE'
            WHEN wm.DIRECTIONAL_SUCCESS AND NOT wm.MEANINGFUL_MOVE_SUCCESS THEN 'LATE_FADE'
            ELSE 'NO_MOVE'
        END,

        -- Adverse before favorable
        CASE WHEN wm.BAR_OF_MAE IS NOT NULL AND wm.BAR_OF_MFE IS NOT NULL
                  AND wm.BAR_OF_MAE < wm.BAR_OF_MFE THEN TRUE
             ELSE FALSE
        END,

        -- Trailing stop realized returns
        CASE WHEN wm.EVAL_WINDOW = 5  THEN ts.S1_REAL_5
             WHEN wm.EVAL_WINDOW = 10 THEN ts.S1_REAL_10
             WHEN wm.EVAL_WINDOW = 20 THEN ts.S1_REAL_20 END,
        CASE WHEN wm.EVAL_WINDOW = 5  THEN ts.S2_REAL_5
             WHEN wm.EVAL_WINDOW = 10 THEN ts.S2_REAL_10
             WHEN wm.EVAL_WINDOW = 20 THEN ts.S2_REAL_20 END,
        -- Strategy 3 = average of 1 and 2
        CASE WHEN wm.EVAL_WINDOW = 5  THEN (COALESCE(ts.S1_REAL_5,0) + COALESCE(ts.S2_REAL_5,0)) / 2.0
             WHEN wm.EVAL_WINDOW = 10 THEN (COALESCE(ts.S1_REAL_10,0) + COALESCE(ts.S2_REAL_10,0)) / 2.0
             WHEN wm.EVAL_WINDOW = 20 THEN (COALESCE(ts.S1_REAL_20,0) + COALESCE(ts.S2_REAL_20,0)) / 2.0 END,

        CASE WHEN wm.BARS_AVAILABLE >= wm.EVAL_WINDOW THEN 'SUCCESS' ELSE 'INSUFFICIENT_DATA' END,
        CURRENT_TIMESTAMP()

    FROM TMP_WINDOW_METRICS wm
    LEFT JOIN TMP_TRAIL_SIMS ts ON ts.SETUP_EVENT_ID = wm.SETUP_EVENT_ID;

    SELECT COUNT(*) INTO :v_evaluated FROM MIP.APP.STRUCTURAL_SETUP_OUTCOMES
    WHERE SETUP_EVENT_ID IN (SELECT DISTINCT SETUP_EVENT_ID FROM TMP_WINDOW_METRICS);

    -- Clean up
    DROP TABLE IF EXISTS TMP_FORWARD_BARS;
    DROP TABLE IF EXISTS TMP_BAR_METRICS;
    DROP TABLE IF EXISTS TMP_WINDOW_METRICS;
    DROP TABLE IF EXISTS TMP_TRAIL_SIMS;

    RETURN OBJECT_CONSTRUCT(
        'status',          'SUCCESS',
        'rows_evaluated',  :v_evaluated,
        'elapsed_sec',     DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;

/*  ================================================================
    511_sp_compute_structural_trust.sql
    MIP Structural Strategy Framework — Trust Scoring
    Phase 3b: Aggregate outcomes into trust labels and path stats
    per (SETUP_FAMILY, MARKET_TYPE, EVAL_WINDOW).
    ================================================================ */

CREATE OR REPLACE PROCEDURE MIP.APP.SP_COMPUTE_STRUCTURAL_TRUST()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_start   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_trust_rows  INTEGER := 0;
    v_path_rows   INTEGER := 0;
BEGIN

    -- ============================================================
    -- STEP 1: Compute trust aggregates
    -- ============================================================
    TRUNCATE TABLE MIP.APP.STRUCTURAL_SETUP_TRUST;

    INSERT INTO MIP.APP.STRUCTURAL_SETUP_TRUST (
        SETUP_FAMILY, MARKET_TYPE, EVAL_WINDOW,
        N_SETUPS, MEANINGFUL_HIT_RATE, DIRECTIONAL_HIT_RATE, PATH_SURVIVAL_HIT_RATE,
        AVG_MFE, AVG_MAE, MFE_MAE_RATIO, AVG_BARS_TO_THRESHOLD,
        FAILURE_MODE_DISTRIBUTION, BEST_WINDOW, TRUST_LABEL,
        TRAIL_STRATEGY_RECOMMENDATION, EXIT_STYLE_RECOMMENDATION,
        COMPUTED_AT
    )
    WITH raw_stats AS (
        SELECT
            se.SETUP_FAMILY, se.MARKET_TYPE, so.EVAL_WINDOW,
            COUNT(*) AS N_SETUPS,
            AVG(CASE WHEN so.MEANINGFUL_MOVE_SUCCESS THEN 1.0 ELSE 0.0 END) AS MEAN_HIT_RATE,
            AVG(CASE WHEN so.DIRECTIONAL_SUCCESS THEN 1.0 ELSE 0.0 END) AS DIR_HIT_RATE,
            -- Path survival: meaningful move reached AND invalidation NOT hit before MFE threshold bar
            AVG(CASE WHEN so.MEANINGFUL_MOVE_SUCCESS
                      AND (so.BARS_TO_INVALIDATION IS NULL
                           OR so.BARS_TO_FAVORABLE_THRESHOLD < so.BARS_TO_INVALIDATION)
                      THEN 1.0 ELSE 0.0 END) AS PATH_SURV_RATE,
            AVG(so.MAX_FAVORABLE_EXCURSION) AS AVG_MFE,
            AVG(so.MAX_ADVERSE_EXCURSION) AS AVG_MAE,
            CASE WHEN AVG(ABS(so.MAX_ADVERSE_EXCURSION)) > 0
                 THEN AVG(so.MAX_FAVORABLE_EXCURSION) / AVG(ABS(so.MAX_ADVERSE_EXCURSION))
                 ELSE NULL END AS MFE_MAE_RATIO,
            AVG(so.BARS_TO_FAVORABLE_THRESHOLD) AS AVG_BARS_TO_THRESH,

            -- Failure mode distribution
            OBJECT_CONSTRUCT(
                'SUCCESS', SUM(CASE WHEN so.FAILURE_MODE = 'SUCCESS' THEN 1 ELSE 0 END),
                'IMMEDIATE_FAILURE', SUM(CASE WHEN so.FAILURE_MODE = 'IMMEDIATE_FAILURE' THEN 1 ELSE 0 END),
                'STRUCTURAL_BREAK', SUM(CASE WHEN so.FAILURE_MODE = 'STRUCTURAL_BREAK' THEN 1 ELSE 0 END),
                'WICK_FAILURE', SUM(CASE WHEN so.FAILURE_MODE = 'WICK_FAILURE' THEN 1 ELSE 0 END),
                'REVERSAL_AFTER_CONFIRMATION', SUM(CASE WHEN so.FAILURE_MODE = 'REVERSAL_AFTER_CONFIRMATION' THEN 1 ELSE 0 END),
                'LATE_FADE', SUM(CASE WHEN so.FAILURE_MODE = 'LATE_FADE' THEN 1 ELSE 0 END),
                'NO_MOVE', SUM(CASE WHEN so.FAILURE_MODE = 'NO_MOVE' THEN 1 ELSE 0 END)
            ) AS FAILURE_DIST,

            -- Trail strategy comparison (avg realized for this window)
            AVG(so.TRAIL_STRATEGY_1_REALIZED) AS AVG_S1,
            AVG(so.TRAIL_STRATEGY_2_REALIZED) AS AVG_S2,
            AVG(so.TRAIL_STRATEGY_3_REALIZED) AS AVG_S3

        FROM MIP.APP.STRUCTURAL_SETUP_OUTCOMES so
        JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se ON se.SETUP_EVENT_ID = so.SETUP_EVENT_ID
        WHERE so.EVAL_STATUS = 'SUCCESS'
        GROUP BY se.SETUP_FAMILY, se.MARKET_TYPE, so.EVAL_WINDOW
    ),
    best_windows AS (
        SELECT SETUP_FAMILY, MARKET_TYPE,
            MAX(CASE WHEN RK = 1 THEN EVAL_WINDOW END) AS BEST_WIN
        FROM (
            SELECT SETUP_FAMILY, MARKET_TYPE, EVAL_WINDOW, MEAN_HIT_RATE,
                ROW_NUMBER() OVER (PARTITION BY SETUP_FAMILY, MARKET_TYPE
                                   ORDER BY MEAN_HIT_RATE DESC) AS RK
            FROM raw_stats
        )
        GROUP BY SETUP_FAMILY, MARKET_TYPE
    )
    SELECT
        rs.SETUP_FAMILY, rs.MARKET_TYPE, rs.EVAL_WINDOW,
        rs.N_SETUPS, rs.MEAN_HIT_RATE, rs.DIR_HIT_RATE, rs.PATH_SURV_RATE,
        rs.AVG_MFE, rs.AVG_MAE, rs.MFE_MAE_RATIO, rs.AVG_BARS_TO_THRESH,
        rs.FAILURE_DIST,
        bw.BEST_WIN,

        -- Trust label
        CASE
            WHEN rs.N_SETUPS >= 40 AND rs.MEAN_HIT_RATE >= 0.40
                 AND rs.MFE_MAE_RATIO >= 1.5 AND rs.PATH_SURV_RATE >= 0.35
            THEN 'TRUSTED'
            WHEN rs.N_SETUPS >= 20 AND rs.MEAN_HIT_RATE >= 0.30
                 AND rs.MFE_MAE_RATIO >= 1.2
            THEN 'PROVISIONAL'
            WHEN rs.N_SETUPS >= 10
            THEN 'RESEARCH'
            ELSE 'REJECTED'
        END,

        -- Best trailing strategy
        CASE
            WHEN rs.AVG_S1 >= rs.AVG_S2 AND rs.AVG_S1 >= rs.AVG_S3 THEN 'STRUCTURAL'
            WHEN rs.AVG_S2 >= rs.AVG_S1 AND rs.AVG_S2 >= rs.AVG_S3 THEN 'PROGRESS_BASED'
            ELSE 'HYBRID'
        END,

        -- Exit style
        CASE
            WHEN rs.MEAN_HIT_RATE >= 0.60 AND rs.AVG_BARS_TO_THRESH <= 8 THEN 'STAGED_PARTIAL'
            WHEN rs.AVG_S1 > 0 THEN 'OPEN_RUNNER'
            ELSE 'STRUCTURAL_TARGET'
        END,

        CURRENT_TIMESTAMP()
    FROM raw_stats rs
    LEFT JOIN best_windows bw ON bw.SETUP_FAMILY = rs.SETUP_FAMILY AND bw.MARKET_TYPE = rs.MARKET_TYPE;

    SELECT COUNT(*) INTO :v_trust_rows FROM MIP.APP.STRUCTURAL_SETUP_TRUST;

    -- ============================================================
    -- STEP 2: Compute path statistics
    -- ============================================================
    TRUNCATE TABLE MIP.APP.STRUCTURAL_PATH_STATS;

    INSERT INTO MIP.APP.STRUCTURAL_PATH_STATS (
        SETUP_FAMILY, MARKET_TYPE, EVAL_WINDOW,
        PERCENTILE_25_MFE, MEDIAN_MFE, PERCENTILE_75_MFE,
        PERCENTILE_25_MAE, MEDIAN_MAE,
        PCT_ADVERSE_BEFORE_FAVORABLE,
        AVG_BARS_TO_MFE, AVG_BARS_TO_MAE,
        GAP_RISK_CONTRIBUTION,
        COMPUTED_AT
    )
    SELECT
        se.SETUP_FAMILY, se.MARKET_TYPE, so.EVAL_WINDOW,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY so.MAX_FAVORABLE_EXCURSION),
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY so.MAX_FAVORABLE_EXCURSION),
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY so.MAX_FAVORABLE_EXCURSION),
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY so.MAX_ADVERSE_EXCURSION),
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY so.MAX_ADVERSE_EXCURSION),
        AVG(CASE WHEN so.ADVERSE_BEFORE_FAVORABLE THEN 1.0 ELSE 0.0 END),
        AVG(so.BARS_TO_FAVORABLE_THRESHOLD),
        AVG(so.BARS_TO_INVALIDATION),
        -- Gap risk: % of setups where MAE was hit on bar 1 (opening gap)
        AVG(CASE WHEN so.BARS_TO_INVALIDATION = 1 THEN 1.0 ELSE 0.0 END),
        CURRENT_TIMESTAMP()
    FROM MIP.APP.STRUCTURAL_SETUP_OUTCOMES so
    JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se ON se.SETUP_EVENT_ID = so.SETUP_EVENT_ID
    WHERE so.EVAL_STATUS = 'SUCCESS'
    GROUP BY se.SETUP_FAMILY, se.MARKET_TYPE, so.EVAL_WINDOW;

    SELECT COUNT(*) INTO :v_path_rows FROM MIP.APP.STRUCTURAL_PATH_STATS;

    RETURN OBJECT_CONSTRUCT(
        'status',      'SUCCESS',
        'trust_rows',  :v_trust_rows,
        'path_rows',   :v_path_rows,
        'elapsed_sec', DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;

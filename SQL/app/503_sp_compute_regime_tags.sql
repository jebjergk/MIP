/*  ================================================================
    503_sp_compute_regime_tags.sql
    MIP Structural Strategy Framework — Regime Tagging
    Phase 2d: Compute volatility/trend/range regime context per
    symbol and build setup-family compatibility matrix.
    ================================================================ */

CREATE OR REPLACE PROCEDURE MIP.APP.SP_COMPUTE_REGIME_TAGS(
    P_AS_OF_DATE   DATE     DEFAULT NULL,
    P_SYMBOL       VARCHAR  DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_as_of       DATE     := COALESCE(P_AS_OF_DATE, CURRENT_DATE());
    v_run_start   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_rows_merged INTEGER  := 0;
BEGIN

    -- ============================================================
    -- STEP 1: Compute regime features from daily bars
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_REGIME_FEATURES AS
    WITH bars AS (
        SELECT
            SYMBOL, MARKET_TYPE, TS, OPEN, HIGH, LOW, CLOSE,
            LAG(CLOSE) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS PREV_CLOSE,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS DESC) AS RN_DESC
        FROM MIP.MART.MARKET_BARS
        WHERE INTERVAL_MINUTES = 1440
          AND TS <= :v_as_of
          AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
    ),
    true_range AS (
        SELECT SYMBOL, MARKET_TYPE, TS, RN_DESC,
            GREATEST(
                HIGH - LOW,
                ABS(HIGH - COALESCE(PREV_CLOSE, CLOSE)),
                ABS(LOW  - COALESCE(PREV_CLOSE, CLOSE))
            ) AS TR
        FROM bars
    ),
    atr_calcs AS (
        SELECT SYMBOL, MARKET_TYPE,
            -- 20-bar ATR
            AVG(CASE WHEN RN_DESC <= 20 THEN TR END) AS ATR_20,
            -- 5-bar ATR (recent vol)
            AVG(CASE WHEN RN_DESC <= 5 THEN TR END) AS ATR_5,
            -- ATR rolling history for percentile (60-bar)
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY CASE WHEN RN_DESC <= 60 THEN TR END) AS ATR_P50_60,
            PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY CASE WHEN RN_DESC <= 60 THEN TR END) AS ATR_P80_60
        FROM true_range
        GROUP BY SYMBOL, MARKET_TYPE
    ),
    sma_calcs AS (
        SELECT SYMBOL, MARKET_TYPE,
            -- 10-bar SMA
            AVG(CASE WHEN RN_DESC <= 10 THEN CLOSE END) AS SMA_10,
            -- 20-bar SMA
            AVG(CASE WHEN RN_DESC <= 20 THEN CLOSE END) AS SMA_20,
            -- Current close and price range
            MAX(CASE WHEN RN_DESC = 1 THEN CLOSE END)  AS CURRENT_CLOSE,
            MAX(CASE WHEN RN_DESC <= 10 THEN HIGH END)  AS HIGH_10,
            MIN(CASE WHEN RN_DESC <= 10 THEN LOW END)   AS LOW_10,
            -- Slope of last 10 closes (simple linear regression proxy: close[0] - close[-9])
            MAX(CASE WHEN RN_DESC = 1 THEN CLOSE END) -
            MAX(CASE WHEN RN_DESC = 10 THEN CLOSE END) AS CLOSE_CHANGE_10
        FROM bars
        GROUP BY SYMBOL, MARKET_TYPE
    )
    SELECT
        a.SYMBOL, a.MARKET_TYPE,
        a.ATR_20, a.ATR_5, a.ATR_P50_60, a.ATR_P80_60,
        s.SMA_10, s.SMA_20, s.CURRENT_CLOSE,
        s.HIGH_10, s.LOW_10, s.CLOSE_CHANGE_10,
        s.CLOSE_CHANGE_10 / NULLIF(a.ATR_20, 0) AS SLOPE_ATR_NORM
    FROM atr_calcs a
    JOIN sma_calcs s ON s.SYMBOL = a.SYMBOL AND s.MARKET_TYPE = a.MARKET_TYPE;

    -- ============================================================
    -- STEP 2: Classify regimes
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_REGIME_CLASSIFIED AS
    SELECT
        SYMBOL, MARKET_TYPE, ATR_20, SMA_10, SMA_20,
        HIGH_10, LOW_10,

        -- VOL_REGIME
        CASE
            WHEN ATR_5 > ATR_20 * 1.30 THEN 'EXPANDING_VOL'
            WHEN ATR_20 > ATR_P80_60   THEN 'HIGH_VOL'
            WHEN ATR_20 < ATR_P50_60   THEN 'LOW_VOL'
            ELSE 'NORMAL_VOL'
        END AS VOL_REGIME,

        -- TREND_REGIME
        CASE
            WHEN SLOPE_ATR_NORM > 1.5 AND CURRENT_CLOSE > SMA_20 + 1.5 * ATR_20
                THEN 'STRONG_TREND_UP'
            WHEN SLOPE_ATR_NORM > 0 AND CURRENT_CLOSE > SMA_20
                THEN 'MODERATE_TREND_UP'
            WHEN SLOPE_ATR_NORM < -1.5 AND CURRENT_CLOSE < SMA_20 - 1.5 * ATR_20
                THEN 'STRONG_TREND_DOWN'
            WHEN SLOPE_ATR_NORM < 0 AND CURRENT_CLOSE < SMA_20
                THEN 'MODERATE_TREND_DOWN'
            ELSE 'NEUTRAL'
        END AS TREND_REGIME,

        -- RANGE_REGIME
        CASE
            WHEN (HIGH_10 - LOW_10) < 2.0 * ATR_20 THEN 'TIGHT_RANGE'
            WHEN (HIGH_10 - LOW_10) > 4.0 * ATR_20 THEN 'WIDE_RANGE'
            ELSE 'NORMAL_RANGE'
        END AS RANGE_REGIME,

        CURRENT_CLOSE, SLOPE_ATR_NORM

    FROM TMP_REGIME_FEATURES;

    -- ============================================================
    -- STEP 3: Build setup compatibility JSON per symbol
    -- ============================================================
    MERGE INTO MIP.APP.STRUCTURAL_REGIME_TAG tgt
    USING (
        SELECT
            rc.SYMBOL, rc.MARKET_TYPE, :v_as_of AS AS_OF_DATE,
            rc.VOL_REGIME, rc.TREND_REGIME, rc.RANGE_REGIME,
            rc.ATR_20, rc.SLOPE_ATR_NORM AS SMA_10_SLOPE,
            rc.SMA_20 AS SMA_20_VAL,
            rc.HIGH_10 AS RANGE_10_HIGH,
            rc.LOW_10 AS RANGE_10_LOW,

            -- Compatibility matrix (deterministic per Addendum D)
            OBJECT_CONSTRUCT(
                'BREAKOUT_RETEST_LONG',
                    CASE WHEN rc.TREND_REGIME IN ('STRONG_TREND_UP','MODERATE_TREND_UP') THEN 'GOOD'
                         WHEN rc.TREND_REGIME IN ('STRONG_TREND_DOWN','MODERATE_TREND_DOWN') THEN 'POOR'
                         WHEN rc.VOL_REGIME = 'EXPANDING_VOL' THEN 'GOOD'
                         ELSE 'NEUTRAL' END,
                'SUPPORT_WICK_LONG',
                    CASE WHEN rc.TREND_REGIME IN ('STRONG_TREND_DOWN','MODERATE_TREND_DOWN') THEN 'POOR'
                         WHEN rc.RANGE_REGIME = 'TIGHT_RANGE' THEN 'GOOD'
                         WHEN rc.TREND_REGIME = 'NEUTRAL' THEN 'GOOD'
                         ELSE 'NEUTRAL' END,
                'THREE_BAR_REVERSAL_LONG',
                    CASE WHEN rc.TREND_REGIME IN ('STRONG_TREND_DOWN','MODERATE_TREND_DOWN') THEN 'GOOD'
                         WHEN rc.TREND_REGIME IN ('STRONG_TREND_UP') THEN 'POOR'
                         WHEN rc.VOL_REGIME = 'HIGH_VOL' THEN 'GOOD'
                         ELSE 'NEUTRAL' END,
                'TREND_PULLBACK_LONG',
                    CASE WHEN rc.TREND_REGIME IN ('STRONG_TREND_UP','MODERATE_TREND_UP') THEN 'GOOD'
                         WHEN rc.TREND_REGIME IN ('STRONG_TREND_DOWN','MODERATE_TREND_DOWN') THEN 'POOR'
                         WHEN rc.TREND_REGIME = 'NEUTRAL' THEN 'POOR'
                         ELSE 'NEUTRAL' END,
                'BREAKDOWN_RETEST_SHORT',
                    CASE WHEN rc.TREND_REGIME IN ('STRONG_TREND_DOWN','MODERATE_TREND_DOWN') THEN 'GOOD'
                         WHEN rc.TREND_REGIME IN ('STRONG_TREND_UP','MODERATE_TREND_UP') THEN 'POOR'
                         WHEN rc.VOL_REGIME = 'EXPANDING_VOL' THEN 'GOOD'
                         ELSE 'NEUTRAL' END,
                'RESISTANCE_WICK_SHORT',
                    CASE WHEN rc.TREND_REGIME IN ('STRONG_TREND_UP','MODERATE_TREND_UP') THEN 'POOR'
                         WHEN rc.RANGE_REGIME = 'TIGHT_RANGE' THEN 'GOOD'
                         WHEN rc.TREND_REGIME = 'NEUTRAL' THEN 'GOOD'
                         ELSE 'NEUTRAL' END,
                'THREE_BAR_REVERSAL_SHORT',
                    CASE WHEN rc.TREND_REGIME IN ('STRONG_TREND_UP','MODERATE_TREND_UP') THEN 'GOOD'
                         WHEN rc.TREND_REGIME IN ('STRONG_TREND_DOWN') THEN 'POOR'
                         WHEN rc.VOL_REGIME = 'HIGH_VOL' THEN 'GOOD'
                         ELSE 'NEUTRAL' END,
                'FAILED_BREAKOUT_SHORT',
                    CASE WHEN rc.RANGE_REGIME = 'TIGHT_RANGE' THEN 'GOOD'
                         WHEN rc.VOL_REGIME IN ('HIGH_VOL','EXPANDING_VOL') THEN 'GOOD'
                         ELSE 'NEUTRAL' END
            ) AS REGIME_SETUP_COMPAT

        FROM TMP_REGIME_CLASSIFIED rc
    ) src
    ON  tgt.SYMBOL      = src.SYMBOL
    AND tgt.MARKET_TYPE  = src.MARKET_TYPE
    AND tgt.AS_OF_DATE   = src.AS_OF_DATE
    WHEN MATCHED THEN UPDATE SET
        tgt.VOL_REGIME          = src.VOL_REGIME,
        tgt.TREND_REGIME        = src.TREND_REGIME,
        tgt.RANGE_REGIME        = src.RANGE_REGIME,
        tgt.REGIME_SETUP_COMPAT = src.REGIME_SETUP_COMPAT,
        tgt.ATR_20              = src.ATR_20,
        tgt.SMA_10_SLOPE        = src.SMA_10_SLOPE,
        tgt.SMA_20              = src.SMA_20_VAL,
        tgt.RANGE_10_HIGH       = src.RANGE_10_HIGH,
        tgt.RANGE_10_LOW        = src.RANGE_10_LOW,
        tgt.CREATED_AT          = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        SYMBOL, MARKET_TYPE, AS_OF_DATE, VOL_REGIME, TREND_REGIME, RANGE_REGIME,
        REGIME_SETUP_COMPAT, ATR_20, SMA_10_SLOPE, SMA_20, RANGE_10_HIGH, RANGE_10_LOW,
        DETECTOR_VERSION
    ) VALUES (
        src.SYMBOL, src.MARKET_TYPE, src.AS_OF_DATE, src.VOL_REGIME, src.TREND_REGIME,
        src.RANGE_REGIME, src.REGIME_SETUP_COMPAT, src.ATR_20, src.SMA_10_SLOPE,
        src.SMA_20_VAL, src.RANGE_10_HIGH, src.RANGE_10_LOW, '1.0'
    );

    SELECT COUNT(*) INTO :v_rows_merged
    FROM MIP.APP.STRUCTURAL_REGIME_TAG
    WHERE AS_OF_DATE = :v_as_of
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL);

    DROP TABLE IF EXISTS TMP_REGIME_FEATURES;
    DROP TABLE IF EXISTS TMP_REGIME_CLASSIFIED;

    RETURN OBJECT_CONSTRUCT(
        'status',        'SUCCESS',
        'as_of_date',    :v_as_of,
        'rows_merged',   :v_rows_merged,
        'elapsed_sec',   DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;

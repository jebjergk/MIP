/*  ================================================================
    502_sp_compute_structural_state.sql
    MIP Structural Strategy Framework — State Machine
    Phase 2c: Compute daily structural state per symbol using
    deterministic transition rules based on price action and levels.
    ================================================================ */

CREATE OR REPLACE PROCEDURE MIP.APP.SP_COMPUTE_STRUCTURAL_STATE(
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
    -- STEP 1: Build working set of recent bars with derived features
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_STATE_BARS AS
    WITH bars AS (
        SELECT
            SYMBOL, MARKET_TYPE, TS, OPEN, HIGH, LOW, CLOSE, VOLUME,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS DESC) AS RN_DESC,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS)      AS BAR_IDX,
            LAG(CLOSE, 1)  OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS PREV_CLOSE,
            LAG(HIGH, 1)   OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS PREV_HIGH,
            LAG(LOW, 1)    OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS PREV_LOW,
            LAG(CLOSE, 2)  OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS PREV2_CLOSE,
            LAG(HIGH, 2)   OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS PREV2_HIGH,
            LAG(LOW, 2)    OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS PREV2_LOW
        FROM MIP.MART.MARKET_BARS
        WHERE INTERVAL_MINUTES = 1440
          AND TS <= :v_as_of
          AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
    ),
    with_sma AS (
        SELECT b.*,
            AVG(b.CLOSE) OVER (PARTITION BY b.SYMBOL, b.MARKET_TYPE
                                ORDER BY b.TS ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS SMA_10,
            AVG(b.CLOSE) OVER (PARTITION BY b.SYMBOL, b.MARKET_TYPE
                                ORDER BY b.TS ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS SMA_20
        FROM bars b
    )
    SELECT * FROM with_sma
    WHERE RN_DESC <= 30;

    -- ============================================================
    -- STEP 2: Identify recent swing highs/lows from the level cache
    --   to define structural boundaries for state classification
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_RECENT_STRUCTURE AS
    SELECT
        SYMBOL, MARKET_TYPE,
        -- Most recent swing high
        MAX(CASE WHEN LEVEL_TYPE = 'SWING_HIGH' THEN LEVEL_PRICE END) AS RECENT_SWING_HIGH,
        -- Most recent swing low
        MIN(CASE WHEN LEVEL_TYPE = 'SWING_LOW'  THEN LEVEL_PRICE END) AS RECENT_SWING_LOW,
        -- Nearest support zone
        MAX(CASE WHEN LEVEL_TYPE = 'SUPPORT_ZONE' THEN LEVEL_PRICE END) AS NEAREST_SUPPORT,
        MAX(CASE WHEN LEVEL_TYPE = 'SUPPORT_ZONE' THEN LEVEL_LOW END)   AS NEAREST_SUPPORT_LOW,
        MAX(CASE WHEN LEVEL_TYPE = 'SUPPORT_ZONE' THEN LEVEL_HIGH END)  AS NEAREST_SUPPORT_HIGH,
        -- Nearest resistance zone
        MIN(CASE WHEN LEVEL_TYPE = 'RESISTANCE_ZONE' THEN LEVEL_PRICE END) AS NEAREST_RESISTANCE,
        MIN(CASE WHEN LEVEL_TYPE = 'RESISTANCE_ZONE' THEN LEVEL_LOW END)   AS NEAREST_RESISTANCE_LOW,
        MIN(CASE WHEN LEVEL_TYPE = 'RESISTANCE_ZONE' THEN LEVEL_HIGH END)  AS NEAREST_RESISTANCE_HIGH
    FROM MIP.APP.STRUCTURAL_LEVEL_CACHE
    WHERE AS_OF_DATE = :v_as_of
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
      AND LAST_TOUCH_DATE >= DATEADD('day', -60, :v_as_of)
    GROUP BY SYMBOL, MARKET_TYPE;

    -- ============================================================
    -- STEP 3: Compute ATR for normalization
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_STATE_ATR AS
    SELECT SYMBOL, MARKET_TYPE, ATR_20
    FROM (
        SELECT SYMBOL, MARKET_TYPE, ATR_AT_DETECTION AS ATR_20,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY LEVEL_SIGNIFICANCE DESC) AS RK
        FROM MIP.APP.STRUCTURAL_LEVEL_CACHE
        WHERE AS_OF_DATE = :v_as_of
          AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
    )
    WHERE RK = 1;

    -- ============================================================
    -- STEP 4: Get prior state for each symbol (yesterday's state)
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_PRIOR_STATE AS
    SELECT SYMBOL, MARKET_TYPE, STRUCTURAL_STATE, STATE_ENTERED_DATE, BARS_IN_STATE
    FROM MIP.APP.STRUCTURAL_STATE_LOG
    WHERE AS_OF_DATE = (
        SELECT MAX(AS_OF_DATE) FROM MIP.APP.STRUCTURAL_STATE_LOG
        WHERE AS_OF_DATE < :v_as_of
          AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
    )
    AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL);

    -- ============================================================
    -- STEP 5: Classify structural state for the current bar
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_NEW_STATE AS
    WITH current_bar AS (
        SELECT * FROM TMP_STATE_BARS WHERE RN_DESC = 1
    ),
    prev_bars AS (
        SELECT SYMBOL, MARKET_TYPE,
            -- Higher lows check: count of bars in last 15 where low > prior low
            SUM(CASE WHEN LOW > PREV_LOW AND RN_DESC <= 15 THEN 1 ELSE 0 END) AS HIGHER_LOWS_15,
            -- Lower highs check
            SUM(CASE WHEN HIGH < PREV_HIGH AND RN_DESC <= 15 THEN 1 ELSE 0 END) AS LOWER_HIGHS_15,
            -- Recent counter-move bars (close < prev_close, last 5 bars)
            SUM(CASE WHEN CLOSE < PREV_CLOSE AND RN_DESC <= 5 THEN 1 ELSE 0 END) AS DOWN_BARS_5,
            SUM(CASE WHEN CLOSE > PREV_CLOSE AND RN_DESC <= 5 THEN 1 ELSE 0 END) AS UP_BARS_5,
            -- 3-bar directional sequence detection
            MAX(CASE WHEN RN_DESC = 1 THEN CLOSE END) AS BAR0_CLOSE,
            MAX(CASE WHEN RN_DESC = 1 THEN OPEN END)  AS BAR0_OPEN,
            MAX(CASE WHEN RN_DESC = 1 THEN HIGH END)  AS BAR0_HIGH,
            MAX(CASE WHEN RN_DESC = 1 THEN LOW END)   AS BAR0_LOW,
            MAX(CASE WHEN RN_DESC = 2 THEN CLOSE END) AS BAR1_CLOSE,
            MAX(CASE WHEN RN_DESC = 2 THEN OPEN END)  AS BAR1_OPEN,
            MAX(CASE WHEN RN_DESC = 2 THEN HIGH END)  AS BAR1_HIGH,
            MAX(CASE WHEN RN_DESC = 3 THEN CLOSE END) AS BAR2_CLOSE,
            MAX(CASE WHEN RN_DESC = 3 THEN OPEN END)  AS BAR2_OPEN,
            MAX(CASE WHEN RN_DESC = 3 THEN HIGH END)  AS BAR2_HIGH,
            MAX(CASE WHEN RN_DESC = 3 THEN LOW END)   AS BAR2_LOW
        FROM TMP_STATE_BARS
        GROUP BY SYMBOL, MARKET_TYPE
    )
    SELECT
        cb.SYMBOL, cb.MARKET_TYPE, cb.TS,
        cb.CLOSE, cb.HIGH, cb.LOW, cb.OPEN,
        cb.SMA_10,
        str.RECENT_SWING_HIGH, str.RECENT_SWING_LOW,
        str.NEAREST_SUPPORT, str.NEAREST_SUPPORT_LOW, str.NEAREST_SUPPORT_HIGH,
        str.NEAREST_RESISTANCE, str.NEAREST_RESISTANCE_LOW, str.NEAREST_RESISTANCE_HIGH,
        atr.ATR_20,
        ps.STRUCTURAL_STATE AS PRIOR_STATE_VAL,
        ps.STATE_ENTERED_DATE AS PRIOR_STATE_ENTERED,
        ps.BARS_IN_STATE AS PRIOR_BARS_IN_STATE,
        pb.HIGHER_LOWS_15, pb.LOWER_HIGHS_15, pb.DOWN_BARS_5, pb.UP_BARS_5,
        pb.BAR0_CLOSE, pb.BAR0_OPEN, pb.BAR1_CLOSE, pb.BAR1_OPEN,
        pb.BAR2_CLOSE, pb.BAR2_OPEN, pb.BAR0_HIGH, pb.BAR0_LOW,
        pb.BAR1_HIGH, pb.BAR2_HIGH, pb.BAR2_LOW,

        -- 3-bar reversal detection
        CASE WHEN pb.BAR2_CLOSE < pb.BAR2_OPEN  -- bar[-2] is down
              AND (pb.BAR1_CLOSE < pb.BAR1_OPEN OR ABS(pb.BAR1_CLOSE - pb.BAR1_OPEN) < 0.3 * (pb.BAR1_HIGH - COALESCE(NULLIF(pb.BAR0_LOW,0), pb.BAR1_HIGH)))
              AND pb.BAR0_CLOSE > pb.BAR0_OPEN   -- bar[0] is up
              AND pb.BAR0_CLOSE > pb.BAR1_HIGH    -- closes above bar[-1] high
             THEN TRUE ELSE FALSE END AS THREE_BAR_BULLISH_REVERSAL,

        CASE WHEN pb.BAR2_CLOSE > pb.BAR2_OPEN  -- bar[-2] is up
              AND (pb.BAR1_CLOSE > pb.BAR1_OPEN OR ABS(pb.BAR1_CLOSE - pb.BAR1_OPEN) < 0.3 * (pb.BAR1_HIGH - COALESCE(NULLIF(pb.BAR0_LOW,0), pb.BAR1_HIGH)))
              AND pb.BAR0_CLOSE < pb.BAR0_OPEN   -- bar[0] is down
              AND pb.BAR0_CLOSE < COALESCE(NULLIF(pb.BAR0_LOW,0), pb.BAR0_CLOSE)
             THEN TRUE ELSE FALSE END AS THREE_BAR_BEARISH_REVERSAL,

        -- State classification
        CASE
            -- FAILED_MOVE: prior state was BREAKOUT/BREAKDOWN_EXPANSION and price reversed back
            WHEN COALESCE(ps.STRUCTURAL_STATE, '') = 'BREAKOUT_EXPANSION'
                 AND cb.CLOSE < COALESCE(str.NEAREST_RESISTANCE, cb.CLOSE + 1)
                 AND COALESCE(ps.BARS_IN_STATE, 0) <= 3
            THEN 'FAILED_MOVE'

            WHEN COALESCE(ps.STRUCTURAL_STATE, '') = 'BREAKDOWN_EXPANSION'
                 AND cb.CLOSE > COALESCE(str.NEAREST_SUPPORT, cb.CLOSE - 1)
                 AND COALESCE(ps.BARS_IN_STATE, 0) <= 3
            THEN 'FAILED_MOVE'

            -- BREAKOUT_EXPANSION: close above resistance zone
            WHEN cb.CLOSE > COALESCE(str.NEAREST_RESISTANCE_HIGH, cb.CLOSE + 1)
                 AND COALESCE(ps.STRUCTURAL_STATE, '') NOT IN ('BREAKOUT_EXPANSION', 'TREND_UP')
            THEN 'BREAKOUT_EXPANSION'

            -- Continue BREAKOUT_EXPANSION if already in it and still above (up to 5 bars)
            WHEN COALESCE(ps.STRUCTURAL_STATE, '') = 'BREAKOUT_EXPANSION'
                 AND cb.CLOSE > COALESCE(str.NEAREST_RESISTANCE, cb.CLOSE - 1)
                 AND COALESCE(ps.BARS_IN_STATE, 0) < 5
            THEN 'BREAKOUT_EXPANSION'

            -- BREAKDOWN_EXPANSION: close below support zone
            WHEN cb.CLOSE < COALESCE(str.NEAREST_SUPPORT_LOW, cb.CLOSE - 1)
                 AND COALESCE(ps.STRUCTURAL_STATE, '') NOT IN ('BREAKDOWN_EXPANSION', 'TREND_DOWN')
            THEN 'BREAKDOWN_EXPANSION'

            -- Continue BREAKDOWN_EXPANSION
            WHEN COALESCE(ps.STRUCTURAL_STATE, '') = 'BREAKDOWN_EXPANSION'
                 AND cb.CLOSE < COALESCE(str.NEAREST_SUPPORT, cb.CLOSE + 1)
                 AND COALESCE(ps.BARS_IN_STATE, 0) < 5
            THEN 'BREAKDOWN_EXPANSION'

            -- REVERSAL_FORMING: 3-bar reversal detected at a meaningful level
            WHEN pb.BAR2_CLOSE < pb.BAR2_OPEN AND pb.BAR0_CLOSE > pb.BAR0_OPEN
                 AND pb.BAR0_CLOSE > pb.BAR1_HIGH
                 AND cb.LOW <= COALESCE(str.NEAREST_SUPPORT_HIGH, cb.LOW - 1) + COALESCE(atr.ATR_20, 0) * 1.5
            THEN 'REVERSAL_FORMING'

            WHEN pb.BAR2_CLOSE > pb.BAR2_OPEN AND pb.BAR0_CLOSE < pb.BAR0_OPEN
                 AND pb.BAR0_CLOSE < COALESCE(NULLIF(pb.BAR0_LOW, 0), pb.BAR0_CLOSE)
                 AND cb.HIGH >= COALESCE(str.NEAREST_RESISTANCE_LOW, cb.HIGH + 1) - COALESCE(atr.ATR_20, 0) * 1.5
            THEN 'REVERSAL_FORMING'

            -- PULLBACK_IN_TREND: was trending, now counter-move without breaking structure
            -- Evaluated BEFORE TREND_UP/DOWN so that real pullbacks don't get absorbed by broad trend checks
            -- Uptrend pullback: was TREND_UP, close crossed below SMA_10 (real pullback signal),
            -- but swing low still intact (trend structure preserved)
            WHEN COALESCE(ps.STRUCTURAL_STATE, '') IN ('TREND_UP', 'BREAKOUT_EXPANSION')
                 AND cb.CLOSE < cb.SMA_10
                 AND cb.LOW > COALESCE(str.RECENT_SWING_LOW, 0)
                 AND pb.HIGHER_LOWS_15 >= 2
            THEN 'PULLBACK_IN_TREND'

            -- Downtrend pullback: was TREND_DOWN, close crossed above SMA_10, swing high intact
            WHEN COALESCE(ps.STRUCTURAL_STATE, '') IN ('TREND_DOWN', 'BREAKDOWN_EXPANSION')
                 AND cb.CLOSE > cb.SMA_10
                 AND cb.HIGH < COALESCE(str.RECENT_SWING_HIGH, 999999)
                 AND pb.LOWER_HIGHS_15 >= 2
            THEN 'PULLBACK_IN_TREND'

            -- Continue PULLBACK_IN_TREND: still between swing extremes, max 5 bars
            WHEN COALESCE(ps.STRUCTURAL_STATE, '') = 'PULLBACK_IN_TREND'
                 AND cb.LOW > COALESCE(str.RECENT_SWING_LOW, 0)
                 AND cb.HIGH < COALESCE(str.RECENT_SWING_HIGH, 999999)
                 AND COALESCE(ps.BARS_IN_STATE, 0) < 5
            THEN 'PULLBACK_IN_TREND'

            -- TREND_UP: higher lows, above SMA, or breakout matured (>= 3 bars)
            -- Also: pullback resolving back above SMA
            WHEN (pb.HIGHER_LOWS_15 >= 3 AND cb.CLOSE > cb.SMA_10)
                 OR (COALESCE(ps.STRUCTURAL_STATE, '') = 'BREAKOUT_EXPANSION'
                     AND COALESCE(ps.BARS_IN_STATE, 0) >= 3)
                 OR (COALESCE(ps.STRUCTURAL_STATE, '') = 'PULLBACK_IN_TREND'
                     AND cb.CLOSE > cb.SMA_10 AND pb.HIGHER_LOWS_15 >= 3)
            THEN 'TREND_UP'

            -- TREND_DOWN: lower highs, below SMA, or breakdown matured
            -- Also: downtrend pullback resolving back below SMA
            WHEN (pb.LOWER_HIGHS_15 >= 3 AND cb.CLOSE < cb.SMA_10)
                 OR (COALESCE(ps.STRUCTURAL_STATE, '') = 'BREAKDOWN_EXPANSION'
                     AND COALESCE(ps.BARS_IN_STATE, 0) >= 3)
                 OR (COALESCE(ps.STRUCTURAL_STATE, '') = 'PULLBACK_IN_TREND'
                     AND cb.CLOSE < cb.SMA_10 AND pb.LOWER_HIGHS_15 >= 3)
            THEN 'TREND_DOWN'

            -- RANGE_BOUND: default when no strong trend or breakout
            ELSE 'RANGE_BOUND'
        END AS NEW_STATE,

        -- Transition trigger description
        CASE
            WHEN COALESCE(ps.STRUCTURAL_STATE, '') = 'BREAKOUT_EXPANSION'
                 AND cb.CLOSE < COALESCE(str.NEAREST_RESISTANCE, cb.CLOSE + 1)
                 AND COALESCE(ps.BARS_IN_STATE, 0) <= 3
            THEN 'Failed breakout: close reversed back below resistance'
            WHEN cb.CLOSE > COALESCE(str.NEAREST_RESISTANCE_HIGH, cb.CLOSE + 1)
            THEN 'Close above resistance zone ' || COALESCE(TO_VARCHAR(str.NEAREST_RESISTANCE, '999999.99'), '?')
            WHEN cb.CLOSE < COALESCE(str.NEAREST_SUPPORT_LOW, cb.CLOSE - 1)
            THEN 'Close below support zone ' || COALESCE(TO_VARCHAR(str.NEAREST_SUPPORT, '999999.99'), '?')
            ELSE 'State classification from price structure'
        END AS TRIGGER_DESC

    FROM current_bar cb
    LEFT JOIN TMP_RECENT_STRUCTURE str
      ON str.SYMBOL = cb.SYMBOL AND str.MARKET_TYPE = cb.MARKET_TYPE
    LEFT JOIN TMP_STATE_ATR atr
      ON atr.SYMBOL = cb.SYMBOL AND atr.MARKET_TYPE = cb.MARKET_TYPE
    LEFT JOIN TMP_PRIOR_STATE ps
      ON ps.SYMBOL = cb.SYMBOL AND ps.MARKET_TYPE = cb.MARKET_TYPE
    LEFT JOIN prev_bars pb
      ON pb.SYMBOL = cb.SYMBOL AND pb.MARKET_TYPE = cb.MARKET_TYPE;

    -- ============================================================
    -- STEP 6: Merge into STRUCTURAL_STATE_LOG
    -- ============================================================
    MERGE INTO MIP.APP.STRUCTURAL_STATE_LOG tgt
    USING (
        SELECT
            ns.SYMBOL, ns.MARKET_TYPE, :v_as_of AS AS_OF_DATE,
            ns.NEW_STATE AS STRUCTURAL_STATE,
            CASE
                WHEN ns.NEW_STATE = COALESCE(ns.PRIOR_STATE_VAL, '')
                THEN COALESCE(ns.PRIOR_STATE_ENTERED, :v_as_of)
                ELSE :v_as_of
            END AS STATE_ENTERED_DATE,
            CASE
                WHEN ns.NEW_STATE = COALESCE(ns.PRIOR_STATE_VAL, '')
                THEN COALESCE(ns.PRIOR_BARS_IN_STATE, 0) + 1
                ELSE 1
            END AS BARS_IN_STATE,
            ns.PRIOR_STATE_VAL AS PRIOR_STATE,
            ns.TRIGGER_DESC AS TRANSITION_TRIGGER,
            OBJECT_CONSTRUCT(
                'nearest_support', ns.NEAREST_SUPPORT,
                'nearest_resistance', ns.NEAREST_RESISTANCE,
                'recent_swing_high', ns.RECENT_SWING_HIGH,
                'recent_swing_low', ns.RECENT_SWING_LOW
            ) AS KEY_LEVEL_CONTEXT,
            CASE
                WHEN ns.NEW_STATE IN ('TREND_UP', 'TREND_DOWN')
                     AND ns.HIGHER_LOWS_15 >= 5 THEN 0.9
                WHEN ns.NEW_STATE IN ('TREND_UP', 'TREND_DOWN')
                     AND ns.HIGHER_LOWS_15 >= 3 THEN 0.7
                WHEN ns.NEW_STATE = 'BREAKOUT_EXPANSION' THEN 0.8
                WHEN ns.NEW_STATE = 'BREAKDOWN_EXPANSION' THEN 0.8
                WHEN ns.NEW_STATE = 'REVERSAL_FORMING' THEN 0.6
                WHEN ns.NEW_STATE = 'FAILED_MOVE' THEN 0.7
                WHEN ns.NEW_STATE = 'PULLBACK_IN_TREND' THEN 0.7
                ELSE 0.5
            END AS STATE_CONFIDENCE
        FROM TMP_NEW_STATE ns
    ) src
    ON  tgt.SYMBOL      = src.SYMBOL
    AND tgt.MARKET_TYPE  = src.MARKET_TYPE
    AND tgt.AS_OF_DATE   = src.AS_OF_DATE
    WHEN MATCHED THEN UPDATE SET
        tgt.STRUCTURAL_STATE    = src.STRUCTURAL_STATE,
        tgt.STATE_ENTERED_DATE  = src.STATE_ENTERED_DATE,
        tgt.BARS_IN_STATE       = src.BARS_IN_STATE,
        tgt.PRIOR_STATE         = src.PRIOR_STATE,
        tgt.TRANSITION_TRIGGER  = src.TRANSITION_TRIGGER,
        tgt.KEY_LEVEL_CONTEXT   = src.KEY_LEVEL_CONTEXT,
        tgt.STATE_CONFIDENCE    = src.STATE_CONFIDENCE,
        tgt.CREATED_AT          = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        SYMBOL, MARKET_TYPE, AS_OF_DATE, STRUCTURAL_STATE, STATE_ENTERED_DATE,
        BARS_IN_STATE, PRIOR_STATE, TRANSITION_TRIGGER, KEY_LEVEL_CONTEXT,
        STATE_CONFIDENCE, DETECTOR_VERSION
    ) VALUES (
        src.SYMBOL, src.MARKET_TYPE, src.AS_OF_DATE, src.STRUCTURAL_STATE,
        src.STATE_ENTERED_DATE, src.BARS_IN_STATE, src.PRIOR_STATE,
        src.TRANSITION_TRIGGER, src.KEY_LEVEL_CONTEXT, src.STATE_CONFIDENCE, '1.0'
    );

    SELECT COUNT(*) INTO :v_rows_merged
    FROM MIP.APP.STRUCTURAL_STATE_LOG
    WHERE AS_OF_DATE = :v_as_of
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL);

    -- Clean up
    DROP TABLE IF EXISTS TMP_STATE_BARS;
    DROP TABLE IF EXISTS TMP_RECENT_STRUCTURE;
    DROP TABLE IF EXISTS TMP_STATE_ATR;
    DROP TABLE IF EXISTS TMP_PRIOR_STATE;
    DROP TABLE IF EXISTS TMP_NEW_STATE;

    RETURN OBJECT_CONSTRUCT(
        'status',        'SUCCESS',
        'as_of_date',    :v_as_of,
        'rows_merged',   :v_rows_merged,
        'elapsed_sec',   DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;

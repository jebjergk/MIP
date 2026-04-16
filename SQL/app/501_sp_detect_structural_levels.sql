/*  ================================================================
    501_sp_detect_structural_levels.sql
    MIP Structural Strategy Framework — Level Detection
    Phase 2b: Detect swing highs/lows, cluster into support/resistance
    zones, and compute 5-component significance scores.
    ================================================================ */

CREATE OR REPLACE PROCEDURE MIP.APP.SP_DETECT_STRUCTURAL_LEVELS(
    P_AS_OF_DATE   DATE     DEFAULT NULL,
    P_SYMBOL       VARCHAR  DEFAULT NULL,
    P_LOOKBACK     INTEGER  DEFAULT 10,
    P_ZONE_ATR_TOL FLOAT    DEFAULT 1.0
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_as_of       DATE     := COALESCE(P_AS_OF_DATE, CURRENT_DATE());
    v_run_start   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_swings_ins  INTEGER  := 0;
    v_zones_ins   INTEGER  := 0;
BEGIN

    -- ============================================================
    -- STEP 1: Compute 20-bar ATR per symbol as of the target date
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_ATR AS
    WITH bars AS (
        SELECT
            SYMBOL, MARKET_TYPE, TS,
            HIGH, LOW, CLOSE,
            LAG(CLOSE) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS PREV_CLOSE,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS DESC) AS RN
        FROM MIP.MART.MARKET_BARS
        WHERE INTERVAL_MINUTES = 1440
          AND TS <= :v_as_of
          AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
    ),
    true_range AS (
        SELECT SYMBOL, MARKET_TYPE, TS, RN,
            GREATEST(
                HIGH - LOW,
                ABS(HIGH - COALESCE(PREV_CLOSE, CLOSE)),
                ABS(LOW  - COALESCE(PREV_CLOSE, CLOSE))
            ) AS TR
        FROM bars
        WHERE RN <= 25
    )
    SELECT SYMBOL, MARKET_TYPE,
        AVG(TR) AS ATR_20
    FROM true_range
    WHERE RN <= 20
    GROUP BY SYMBOL, MARKET_TYPE;

    -- ============================================================
    -- STEP 2: Detect swing highs and swing lows
    --   A swing high: bar whose HIGH is >= HIGH of P_LOOKBACK bars
    --   on each side (within available data).
    --   A swing low:  bar whose LOW  is <= LOW  of P_LOOKBACK bars
    --   on each side.
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_SWINGS AS
    WITH bars AS (
        SELECT
            SYMBOL, MARKET_TYPE, TS, OPEN, HIGH, LOW, CLOSE,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS BAR_IDX
        FROM MIP.MART.MARKET_BARS
        WHERE INTERVAL_MINUTES = 1440
          AND TS <= :v_as_of
          AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
    ),
    swing_candidates AS (
        SELECT
            b.SYMBOL, b.MARKET_TYPE, b.TS, b.HIGH, b.LOW, b.CLOSE, b.BAR_IDX,
            -- Is this a swing high? HIGH >= all highs within lookback on each side
            CASE WHEN b.HIGH >= MAX(b2.HIGH)
                 THEN TRUE ELSE FALSE END AS IS_SWING_HIGH,
            -- Is this a swing low? LOW <= all lows within lookback on each side
            CASE WHEN b.LOW <= MIN(b2.LOW)
                 THEN TRUE ELSE FALSE END AS IS_SWING_LOW,
            COUNT(b2.BAR_IDX) AS NEIGHBOR_COUNT
        FROM bars b
        JOIN bars b2
          ON b2.SYMBOL = b.SYMBOL
         AND b2.MARKET_TYPE = b.MARKET_TYPE
         AND b2.BAR_IDX BETWEEN b.BAR_IDX - :P_LOOKBACK AND b.BAR_IDX + :P_LOOKBACK
         AND b2.BAR_IDX != b.BAR_IDX
        GROUP BY b.SYMBOL, b.MARKET_TYPE, b.TS, b.HIGH, b.LOW, b.CLOSE, b.BAR_IDX
        HAVING COUNT(b2.BAR_IDX) >= :P_LOOKBACK  -- need at least lookback neighbors (allows edge bars)
    )
    SELECT SYMBOL, MARKET_TYPE, TS, HIGH, LOW, CLOSE, BAR_IDX,
        IS_SWING_HIGH, IS_SWING_LOW
    FROM swing_candidates
    WHERE IS_SWING_HIGH OR IS_SWING_LOW;

    -- ============================================================
    -- STEP 3: Compute reaction magnitude for each swing
    --   Reaction = max move away from the swing within 3 bars after
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_SWING_REACTIONS AS
    WITH bars AS (
        SELECT SYMBOL, MARKET_TYPE, TS, HIGH, LOW,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS BAR_IDX
        FROM MIP.MART.MARKET_BARS
        WHERE INTERVAL_MINUTES = 1440
          AND TS <= :v_as_of
          AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
    ),
    reactions AS (
        SELECT
            s.SYMBOL, s.MARKET_TYPE, s.TS AS SWING_TS,
            s.IS_SWING_HIGH, s.IS_SWING_LOW,
            s.HIGH AS SWING_HIGH, s.LOW AS SWING_LOW,
            CASE WHEN s.IS_SWING_HIGH
                 THEN s.HIGH - MIN(b.LOW)
                 ELSE 0 END AS REACTION_FROM_HIGH,
            CASE WHEN s.IS_SWING_LOW
                 THEN MAX(b.HIGH) - s.LOW
                 ELSE 0 END AS REACTION_FROM_LOW
        FROM TMP_SWINGS s
        JOIN bars b
          ON b.SYMBOL = s.SYMBOL
         AND b.MARKET_TYPE = s.MARKET_TYPE
         AND b.BAR_IDX BETWEEN s.BAR_IDX + 1 AND s.BAR_IDX + 3
        GROUP BY s.SYMBOL, s.MARKET_TYPE, s.TS, s.IS_SWING_HIGH, s.IS_SWING_LOW,
                 s.HIGH, s.LOW
    )
    SELECT * FROM reactions;

    -- ============================================================
    -- STEP 4: Insert individual swing points into STRUCTURAL_LEVEL_CACHE
    -- ============================================================
    DELETE FROM MIP.APP.STRUCTURAL_LEVEL_CACHE
    WHERE AS_OF_DATE = :v_as_of
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
      AND LEVEL_TYPE IN ('SWING_HIGH', 'SWING_LOW');

    INSERT INTO MIP.APP.STRUCTURAL_LEVEL_CACHE (
        SYMBOL, MARKET_TYPE, AS_OF_DATE, LEVEL_TYPE, LEVEL_PRICE,
        LEVEL_LOW, LEVEL_HIGH, TOUCH_COUNT, FIRST_TOUCH_DATE, LAST_TOUCH_DATE,
        ATR_AT_DETECTION, TOUCH_SCORE, RECENCY_SCORE, REACTION_SCORE,
        HISTORY_SCORE, CLUSTER_SCORE, LEVEL_SIGNIFICANCE, TOUCH_DETAILS,
        DETECTOR_VERSION
    )
    SELECT
        s.SYMBOL, s.MARKET_TYPE, :v_as_of,
        CASE WHEN s.IS_SWING_HIGH THEN 'SWING_HIGH' ELSE 'SWING_LOW' END,
        CASE WHEN s.IS_SWING_HIGH THEN s.HIGH ELSE s.LOW END,
        NULL, NULL,  -- no zone boundaries for point levels
        1,           -- single touch for raw swing points
        s.TS, s.TS,  -- first and last touch are the same
        a.ATR_20,
        -- Individual swing points get simplified significance (full scoring on zones)
        0.3,  -- TOUCH_SCORE: 1 touch = 0.3
        -- RECENCY_SCORE
        CASE
            WHEN DATEDIFF('day', s.TS, :v_as_of) <= 5  THEN 1.0
            WHEN DATEDIFF('day', s.TS, :v_as_of) <= 10 THEN 0.8
            WHEN DATEDIFF('day', s.TS, :v_as_of) <= 20 THEN 0.6
            WHEN DATEDIFF('day', s.TS, :v_as_of) <= 40 THEN 0.3
            ELSE 0.1
        END,
        -- REACTION_SCORE: reaction / (3 * ATR), capped at 1.0
        LEAST(
            COALESCE(
                CASE WHEN s.IS_SWING_HIGH THEN r.REACTION_FROM_HIGH
                     ELSE r.REACTION_FROM_LOW END, 0
            ) / NULLIF(3.0 * a.ATR_20, 0),
            1.0
        ),
        1.0,  -- HISTORY_SCORE: never broken (just detected)
        1.0,  -- CLUSTER_SCORE: single point = perfect cluster
        NULL, -- LEVEL_SIGNIFICANCE computed below
        NULL, -- TOUCH_DETAILS populated for zones
        '1.0'
    FROM TMP_SWINGS s
    JOIN TMP_ATR a ON a.SYMBOL = s.SYMBOL AND a.MARKET_TYPE = s.MARKET_TYPE
    LEFT JOIN TMP_SWING_REACTIONS r
      ON r.SYMBOL = s.SYMBOL AND r.MARKET_TYPE = s.MARKET_TYPE AND r.SWING_TS = s.TS
    WHERE (s.IS_SWING_HIGH AND NOT s.IS_SWING_LOW)
       OR (s.IS_SWING_LOW AND NOT s.IS_SWING_HIGH);

    -- Also insert for points that are both swing high AND swing low (rare, doji-like extremes)
    INSERT INTO MIP.APP.STRUCTURAL_LEVEL_CACHE (
        SYMBOL, MARKET_TYPE, AS_OF_DATE, LEVEL_TYPE, LEVEL_PRICE,
        LEVEL_LOW, LEVEL_HIGH, TOUCH_COUNT, FIRST_TOUCH_DATE, LAST_TOUCH_DATE,
        ATR_AT_DETECTION, TOUCH_SCORE, RECENCY_SCORE, REACTION_SCORE,
        HISTORY_SCORE, CLUSTER_SCORE, LEVEL_SIGNIFICANCE, TOUCH_DETAILS,
        DETECTOR_VERSION
    )
    SELECT
        s.SYMBOL, s.MARKET_TYPE, :v_as_of, lt.LEVEL_TYPE,
        CASE WHEN lt.LEVEL_TYPE = 'SWING_HIGH' THEN s.HIGH ELSE s.LOW END,
        NULL, NULL, 1, s.TS, s.TS, a.ATR_20,
        0.3,
        CASE
            WHEN DATEDIFF('day', s.TS, :v_as_of) <= 5  THEN 1.0
            WHEN DATEDIFF('day', s.TS, :v_as_of) <= 10 THEN 0.8
            WHEN DATEDIFF('day', s.TS, :v_as_of) <= 20 THEN 0.6
            WHEN DATEDIFF('day', s.TS, :v_as_of) <= 40 THEN 0.3
            ELSE 0.1
        END,
        LEAST(
            COALESCE(
                CASE WHEN lt.LEVEL_TYPE = 'SWING_HIGH' THEN r.REACTION_FROM_HIGH
                     ELSE r.REACTION_FROM_LOW END, 0
            ) / NULLIF(3.0 * a.ATR_20, 0),
            1.0
        ),
        1.0, 1.0, NULL, NULL, '1.0'
    FROM TMP_SWINGS s
    JOIN TMP_ATR a ON a.SYMBOL = s.SYMBOL AND a.MARKET_TYPE = s.MARKET_TYPE
    LEFT JOIN TMP_SWING_REACTIONS r
      ON r.SYMBOL = s.SYMBOL AND r.MARKET_TYPE = s.MARKET_TYPE AND r.SWING_TS = s.TS
    CROSS JOIN (SELECT 'SWING_HIGH' AS LEVEL_TYPE UNION ALL SELECT 'SWING_LOW') lt
    WHERE s.IS_SWING_HIGH AND s.IS_SWING_LOW;

    SELECT COUNT(*) INTO :v_swings_ins
    FROM MIP.APP.STRUCTURAL_LEVEL_CACHE
    WHERE AS_OF_DATE = :v_as_of
      AND LEVEL_TYPE IN ('SWING_HIGH', 'SWING_LOW')
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL);

    -- ============================================================
    -- STEP 5: Cluster swing points into support/resistance zones
    --   Group swing lows within ATR tolerance into SUPPORT_ZONE
    --   Group swing highs within ATR tolerance into RESISTANCE_ZONE
    -- ============================================================
    DELETE FROM MIP.APP.STRUCTURAL_LEVEL_CACHE
    WHERE AS_OF_DATE = :v_as_of
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
      AND LEVEL_TYPE IN ('SUPPORT_ZONE', 'RESISTANCE_ZONE');

    -- Support zones: cluster swing lows
    INSERT INTO MIP.APP.STRUCTURAL_LEVEL_CACHE (
        SYMBOL, MARKET_TYPE, AS_OF_DATE, LEVEL_TYPE, LEVEL_PRICE,
        LEVEL_LOW, LEVEL_HIGH, TOUCH_COUNT, FIRST_TOUCH_DATE, LAST_TOUCH_DATE,
        ATR_AT_DETECTION, TOUCH_SCORE, RECENCY_SCORE, REACTION_SCORE,
        HISTORY_SCORE, CLUSTER_SCORE, LEVEL_SIGNIFICANCE, TOUCH_DETAILS,
        DETECTOR_VERSION
    )
    WITH swing_lows AS (
        SELECT SYMBOL, MARKET_TYPE, LEVEL_PRICE, FIRST_TOUCH_DATE,
               ATR_AT_DETECTION, REACTION_SCORE
        FROM MIP.APP.STRUCTURAL_LEVEL_CACHE
        WHERE AS_OF_DATE = :v_as_of
          AND LEVEL_TYPE = 'SWING_LOW'
          AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
    ),
    -- Self-join to find swing lows within ATR tolerance of each other
    -- Use the lowest-priced swing in each cluster as the anchor
    clusters AS (
        SELECT
            a.SYMBOL, a.MARKET_TYPE,
            MIN(a.LEVEL_PRICE) AS ZONE_LOW,
            MAX(a.LEVEL_PRICE) AS ZONE_HIGH,
            AVG(a.LEVEL_PRICE) AS ZONE_MID,
            COUNT(*)            AS TOUCH_CNT,
            MIN(a.FIRST_TOUCH_DATE) AS FIRST_TOUCH,
            MAX(a.FIRST_TOUCH_DATE) AS LAST_TOUCH,
            MAX(a.ATR_AT_DETECTION) AS ATR_VAL,
            AVG(a.REACTION_SCORE)   AS AVG_REACTION
        FROM swing_lows a
        JOIN swing_lows b
          ON b.SYMBOL = a.SYMBOL
         AND b.MARKET_TYPE = a.MARKET_TYPE
         AND ABS(a.LEVEL_PRICE - b.LEVEL_PRICE) <= :P_ZONE_ATR_TOL * a.ATR_AT_DETECTION
        GROUP BY a.SYMBOL, a.MARKET_TYPE,
                 ROUND(a.LEVEL_PRICE / NULLIF(a.ATR_AT_DETECTION, 0), 0)
        HAVING COUNT(*) >= 2
    )
    SELECT
        c.SYMBOL, c.MARKET_TYPE, :v_as_of, 'SUPPORT_ZONE',
        c.ZONE_MID,  -- LEVEL_PRICE = zone midpoint
        c.ZONE_LOW, c.ZONE_HIGH,
        c.TOUCH_CNT,
        c.FIRST_TOUCH, c.LAST_TOUCH,
        c.ATR_VAL,

        -- TOUCH_SCORE: 1->0.0, 2->0.3, 3->0.6, 4->0.8, 5+->1.0
        CASE
            WHEN c.TOUCH_CNT >= 5 THEN 1.0
            WHEN c.TOUCH_CNT = 4  THEN 0.8
            WHEN c.TOUCH_CNT = 3  THEN 0.6
            WHEN c.TOUCH_CNT = 2  THEN 0.3
            ELSE 0.0
        END,

        -- RECENCY_SCORE (based on last touch)
        CASE
            WHEN DATEDIFF('day', c.LAST_TOUCH, :v_as_of) <= 5  THEN 1.0
            WHEN DATEDIFF('day', c.LAST_TOUCH, :v_as_of) <= 10 THEN 0.8
            WHEN DATEDIFF('day', c.LAST_TOUCH, :v_as_of) <= 20 THEN 0.6
            WHEN DATEDIFF('day', c.LAST_TOUCH, :v_as_of) <= 40 THEN 0.3
            ELSE 0.1
        END,

        -- REACTION_SCORE: avg of member reaction scores
        c.AVG_REACTION,

        -- HISTORY_SCORE: default 1.0 (never broken — updated in future passes)
        1.0,

        -- CLUSTER_SCORE: tightness of the zone
        -- spread = (zone_high - zone_low) / ATR; score = MAX(1 - spread/2, 0)
        GREATEST(
            1.0 - (c.ZONE_HIGH - c.ZONE_LOW) / NULLIF(2.0 * c.ATR_VAL, 0),
            0.0
        ),

        NULL,  -- LEVEL_SIGNIFICANCE computed below
        NULL,  -- TOUCH_DETAILS
        '1.0'
    FROM clusters c;

    -- Resistance zones: cluster swing highs
    INSERT INTO MIP.APP.STRUCTURAL_LEVEL_CACHE (
        SYMBOL, MARKET_TYPE, AS_OF_DATE, LEVEL_TYPE, LEVEL_PRICE,
        LEVEL_LOW, LEVEL_HIGH, TOUCH_COUNT, FIRST_TOUCH_DATE, LAST_TOUCH_DATE,
        ATR_AT_DETECTION, TOUCH_SCORE, RECENCY_SCORE, REACTION_SCORE,
        HISTORY_SCORE, CLUSTER_SCORE, LEVEL_SIGNIFICANCE, TOUCH_DETAILS,
        DETECTOR_VERSION
    )
    WITH swing_highs AS (
        SELECT SYMBOL, MARKET_TYPE, LEVEL_PRICE, FIRST_TOUCH_DATE,
               ATR_AT_DETECTION, REACTION_SCORE
        FROM MIP.APP.STRUCTURAL_LEVEL_CACHE
        WHERE AS_OF_DATE = :v_as_of
          AND LEVEL_TYPE = 'SWING_HIGH'
          AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
    ),
    clusters AS (
        SELECT
            a.SYMBOL, a.MARKET_TYPE,
            MIN(a.LEVEL_PRICE) AS ZONE_LOW,
            MAX(a.LEVEL_PRICE) AS ZONE_HIGH,
            AVG(a.LEVEL_PRICE) AS ZONE_MID,
            COUNT(*)            AS TOUCH_CNT,
            MIN(a.FIRST_TOUCH_DATE) AS FIRST_TOUCH,
            MAX(a.FIRST_TOUCH_DATE) AS LAST_TOUCH,
            MAX(a.ATR_AT_DETECTION) AS ATR_VAL,
            AVG(a.REACTION_SCORE)   AS AVG_REACTION
        FROM swing_highs a
        JOIN swing_highs b
          ON b.SYMBOL = a.SYMBOL
         AND b.MARKET_TYPE = a.MARKET_TYPE
         AND ABS(a.LEVEL_PRICE - b.LEVEL_PRICE) <= :P_ZONE_ATR_TOL * a.ATR_AT_DETECTION
        GROUP BY a.SYMBOL, a.MARKET_TYPE,
                 ROUND(a.LEVEL_PRICE / NULLIF(a.ATR_AT_DETECTION, 0), 0)
        HAVING COUNT(*) >= 2
    )
    SELECT
        c.SYMBOL, c.MARKET_TYPE, :v_as_of, 'RESISTANCE_ZONE',
        c.ZONE_MID, c.ZONE_LOW, c.ZONE_HIGH,
        c.TOUCH_CNT, c.FIRST_TOUCH, c.LAST_TOUCH, c.ATR_VAL,
        CASE
            WHEN c.TOUCH_CNT >= 5 THEN 1.0
            WHEN c.TOUCH_CNT = 4  THEN 0.8
            WHEN c.TOUCH_CNT = 3  THEN 0.6
            WHEN c.TOUCH_CNT = 2  THEN 0.3
            ELSE 0.0
        END,
        CASE
            WHEN DATEDIFF('day', c.LAST_TOUCH, :v_as_of) <= 5  THEN 1.0
            WHEN DATEDIFF('day', c.LAST_TOUCH, :v_as_of) <= 10 THEN 0.8
            WHEN DATEDIFF('day', c.LAST_TOUCH, :v_as_of) <= 20 THEN 0.6
            WHEN DATEDIFF('day', c.LAST_TOUCH, :v_as_of) <= 40 THEN 0.3
            ELSE 0.1
        END,
        c.AVG_REACTION,
        1.0,
        GREATEST(1.0 - (c.ZONE_HIGH - c.ZONE_LOW) / NULLIF(2.0 * c.ATR_VAL, 0), 0.0),
        NULL, NULL, '1.0'
    FROM clusters c;

    SELECT COUNT(*) INTO :v_zones_ins
    FROM MIP.APP.STRUCTURAL_LEVEL_CACHE
    WHERE AS_OF_DATE = :v_as_of
      AND LEVEL_TYPE IN ('SUPPORT_ZONE', 'RESISTANCE_ZONE')
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL);

    -- ============================================================
    -- STEP 6: Compute composite LEVEL_SIGNIFICANCE for all rows
    --   = 0.30*TOUCH + 0.25*RECENCY + 0.20*REACTION + 0.15*HISTORY + 0.10*CLUSTER
    -- ============================================================
    UPDATE MIP.APP.STRUCTURAL_LEVEL_CACHE
    SET LEVEL_SIGNIFICANCE =
        0.30 * COALESCE(TOUCH_SCORE, 0)
      + 0.25 * COALESCE(RECENCY_SCORE, 0)
      + 0.20 * COALESCE(REACTION_SCORE, 0)
      + 0.15 * COALESCE(HISTORY_SCORE, 0)
      + 0.10 * COALESCE(CLUSTER_SCORE, 0)
    WHERE AS_OF_DATE = :v_as_of
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
      AND LEVEL_SIGNIFICANCE IS NULL;

    -- Clean up temp tables
    DROP TABLE IF EXISTS TMP_ATR;
    DROP TABLE IF EXISTS TMP_SWINGS;
    DROP TABLE IF EXISTS TMP_SWING_REACTIONS;

    RETURN OBJECT_CONSTRUCT(
        'status',      'SUCCESS',
        'as_of_date',  :v_as_of,
        'swings_inserted',  :v_swings_ins,
        'zones_inserted',   :v_zones_ins,
        'elapsed_sec',      DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;

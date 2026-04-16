/*  ================================================================
    505_sp_update_setup_lifecycle.sql
    MIP Structural Strategy Framework — Setup Lifecycle
    Phase 2f: Update setup lifecycle status based on time, price
    distance, invalidation, and structural state changes.
    ================================================================ */

CREATE OR REPLACE PROCEDURE MIP.APP.SP_UPDATE_SETUP_LIFECYCLE(
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
    v_updated     INTEGER  := 0;
BEGIN

    -- ============================================================
    -- STEP 1: Get current price and ATR for each symbol
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_LC_PRICES AS
    SELECT SYMBOL, MARKET_TYPE, CLOSE AS CURRENT_CLOSE
    FROM (
        SELECT SYMBOL, MARKET_TYPE, CLOSE,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS DESC) AS RK
        FROM MIP.MART.MARKET_BARS
        WHERE INTERVAL_MINUTES = 1440
          AND TS <= :v_as_of
          AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL)
    )
    WHERE RK = 1;

    -- ============================================================
    -- STEP 2: Get current structural state for each symbol
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_LC_STATE AS
    SELECT SYMBOL, MARKET_TYPE, STRUCTURAL_STATE
    FROM MIP.APP.STRUCTURAL_STATE_LOG
    WHERE AS_OF_DATE = :v_as_of
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL);

    -- ============================================================
    -- STEP 3: Update lifecycle for all active (non-terminal) setups
    -- ============================================================
    -- Build the lifecycle update in a temp table to avoid Snowflake alias issues
    CREATE OR REPLACE TEMPORARY TABLE TMP_LC_UPDATE AS
    SELECT
        se.SETUP_EVENT_ID,
        DATEDIFF('day', se.SETUP_DATE, :v_as_of) AS NEW_BARS_SINCE,

        CASE WHEN se.DIRECTION = 'LONG'
            THEN (p.CURRENT_CLOSE - (se.ENTRY_ZONE_LOW + se.ENTRY_ZONE_HIGH) / 2.0) / NULLIF(se.VOLATILITY_CONTEXT, 0)
            ELSE ((se.ENTRY_ZONE_LOW + se.ENTRY_ZONE_HIGH) / 2.0 - p.CURRENT_CLOSE) / NULLIF(se.VOLATILITY_CONTEXT, 0)
        END AS NEW_DIST,

        CASE
            WHEN se.DIRECTION = 'LONG'  AND p.CURRENT_CLOSE < se.PRICE_INVALIDATION_LEVEL THEN 'INVALIDATED'
            WHEN se.DIRECTION = 'SHORT' AND p.CURRENT_CLOSE > se.PRICE_INVALIDATION_LEVEL THEN 'INVALIDATED'
            WHEN DATEDIFF('day', se.SETUP_DATE, :v_as_of) > 5 THEN 'EXPIRED'
            WHEN se.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'
                 AND COALESCE(st.STRUCTURAL_STATE, '') IN ('BREAKDOWN_EXPANSION','TREND_DOWN') THEN 'EXPIRED'
            WHEN se.SETUP_FAMILY = 'SUPPORT_WICK_LONG'
                 AND COALESCE(st.STRUCTURAL_STATE, '') = 'BREAKDOWN_EXPANSION' THEN 'EXPIRED'
            WHEN se.SETUP_FAMILY = 'BREAKDOWN_RETEST_SHORT'
                 AND COALESCE(st.STRUCTURAL_STATE, '') IN ('BREAKOUT_EXPANSION','TREND_UP') THEN 'EXPIRED'
            WHEN se.SETUP_FAMILY = 'RESISTANCE_WICK_SHORT'
                 AND COALESCE(st.STRUCTURAL_STATE, '') = 'BREAKOUT_EXPANSION' THEN 'EXPIRED'
            WHEN DATEDIFF('day', se.SETUP_DATE, :v_as_of) > 3 THEN 'STALE'
            WHEN se.DIRECTION = 'LONG'
                 AND p.CURRENT_CLOSE > se.ENTRY_ZONE_HIGH + 1.5 * COALESCE(se.VOLATILITY_CONTEXT, 1) THEN 'WAITING'
            WHEN se.DIRECTION = 'SHORT'
                 AND p.CURRENT_CLOSE < se.ENTRY_ZONE_LOW - 1.5 * COALESCE(se.VOLATILITY_CONTEXT, 1) THEN 'WAITING'
            WHEN DATEDIFF('day', se.SETUP_DATE, :v_as_of) >= 1 THEN 'ELIGIBLE'
            ELSE 'DETECTED'
        END AS NEW_STATUS,

        CASE
            WHEN se.ELIGIBLE_SINCE IS NOT NULL THEN se.ELIGIBLE_SINCE
            WHEN DATEDIFF('day', se.SETUP_DATE, :v_as_of) >= 1 THEN :v_as_of
            ELSE NULL
        END AS NEW_ELIGIBLE_SINCE

    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
    JOIN TMP_LC_PRICES p ON p.SYMBOL = se.SYMBOL AND p.MARKET_TYPE = se.MARKET_TYPE
    LEFT JOIN TMP_LC_STATE st ON st.SYMBOL = se.SYMBOL AND st.MARKET_TYPE = se.MARKET_TYPE
    WHERE se.SETUP_STATUS IN ('DETECTED', 'ELIGIBLE', 'WAITING', 'STALE')
      AND (:P_SYMBOL IS NULL OR se.SYMBOL = :P_SYMBOL);

    UPDATE MIP.APP.STRUCTURAL_SETUP_EVENTS tgt
    SET
        tgt.BARS_SINCE_DETECTION     = u.NEW_BARS_SINCE,
        tgt.DISTANCE_FROM_ENTRY_ZONE = u.NEW_DIST,
        tgt.SETUP_STATUS             = u.NEW_STATUS,
        tgt.STATUS_UPDATED_AT        = CURRENT_TIMESTAMP(),
        tgt.ELIGIBLE_SINCE           = u.NEW_ELIGIBLE_SINCE
    FROM TMP_LC_UPDATE u
    WHERE tgt.SETUP_EVENT_ID = u.SETUP_EVENT_ID;

    v_updated := SQLROWCOUNT;

    -- Set ELIGIBLE_SINCE for newly eligible setups
    UPDATE MIP.APP.STRUCTURAL_SETUP_EVENTS
    SET ELIGIBLE_SINCE = :v_as_of
    WHERE SETUP_STATUS = 'ELIGIBLE'
      AND ELIGIBLE_SINCE IS NULL
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL);

    DROP TABLE IF EXISTS TMP_LC_PRICES;
    DROP TABLE IF EXISTS TMP_LC_STATE;

    RETURN OBJECT_CONSTRUCT(
        'status',       'SUCCESS',
        'as_of_date',   :v_as_of,
        'rows_updated', :v_updated,
        'elapsed_sec',  DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;

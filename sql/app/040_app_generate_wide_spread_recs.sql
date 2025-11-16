-- Stored procedure to generate recommendations for FX ticks with spreads above a threshold.
-- It evaluates FX ticks within a lookback window and logs them into MIP.APP.RECOMMENDATION_LOG.
CREATE OR REPLACE PROCEDURE MIP.APP.SP_GENERATE_WIDE_SPREAD_RECS(
    P_MIN_SPREAD NUMBER,
    P_LOOKBACK_MINUTES NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_pattern_id NUMBER;
    v_rows_inserted NUMBER := 0;
    v_now TIMESTAMP_NTZ := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;
    v_window_start TIMESTAMP_NTZ;
BEGIN
    -- Determine the pattern identifier for the wide-spread demo pattern.
    SELECT PATTERN_ID
    INTO v_pattern_id
    FROM MIP.APP.PATTERN_DEFINITION
    WHERE NAME = 'WIDE_SPREAD_DEMO'
    ORDER BY PATTERN_ID
    LIMIT 1;

    if (:v_pattern_id IS NULL) THEN
        RETURN 'Pattern WIDE_SPREAD_DEMO not found.';
    END if;

    v_window_start := DATEADD(minute, -P_LOOKBACK_MINUTES, :v_now);

    -- Insert recommendations for qualifying FX ticks, skipping duplicates.
    INSERT INTO MIP.APP.RECOMMENDATION_LOG (PATTERN_ID, PAIR, TS, SCORE, DETAILS, CREATED_AT)
    SELECT
        :v_pattern_id,
        ft.PAIR,
        ft.TS,
        ft.SPREAD AS SCORE,
        OBJECT_CONSTRUCT('spread', ft.SPREAD) AS DETAILS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS CREATED_AT
    FROM MIP.MART.FX_TICKS ft
    WHERE ft.TS >= :v_window_start
      AND ft.TS <= :v_now
      AND ft.SPREAD > P_MIN_SPREAD
      AND NOT EXISTS (
          SELECT 1
          FROM MIP.APP.RECOMMENDATION_LOG rl
          WHERE rl.PATTERN_ID = v_pattern_id
            AND rl.PAIR = ft.PAIR
            AND rl.TS = ft.TS
      );

    v_rows_inserted := SQLROWCOUNT;

    RETURN 'Inserted ' || :v_rows_inserted || ' recommendations';
END;
$$;

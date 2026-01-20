-- /sql/app/105_sp_evaluate_recommendations.sql
-- Purpose: Evaluate forward returns for recommendations over trading-bar horizons

use role MIP_ADMIN_ROLE;
use database MIP;

CREATE OR REPLACE PROCEDURE MIP.APP.SP_EVALUATE_RECOMMENDATIONS(
    P_FROM_DATE TIMESTAMP_NTZ,
    P_TO_DATE   TIMESTAMP_NTZ,
    P_MIN_RETURN_THRESHOLD FLOAT DEFAULT 0.0
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_from_ts TIMESTAMP_NTZ := COALESCE(:P_FROM_DATE, DATEADD(day, -90, current_timestamp()::timestamp_ntz));
    v_to_ts   TIMESTAMP_NTZ := COALESCE(:P_TO_DATE, current_timestamp()::timestamp_ntz);
    v_thr     FLOAT := COALESCE(:P_MIN_RETURN_THRESHOLD, 0.0);
    v_merged  NUMBER := 0;
    v_horizon_counts VARIANT;
    v_run_id  STRING := COALESCE(NULLIF(CURRENT_QUERY_TAG(), ''), UUID_STRING());
BEGIN
    CALL MIP.APP.SP_LOG_EVENT(
        'EVALUATION',
        'SP_EVALUATE_RECOMMENDATIONS',
        'START',
        NULL,
        OBJECT_CONSTRUCT('from_ts', :v_from_ts, 'to_ts', :v_to_ts, 'min_return_threshold', :v_thr),
        NULL,
        :v_run_id,
        NULL
    );

    MERGE INTO MIP.APP.RECOMMENDATION_OUTCOMES t
    USING (
        WITH horizons AS (
            SELECT column1::NUMBER AS HORIZON_BARS
            FROM VALUES (1), (3), (5), (10), (20)
        ),
        entry_bars AS (
            SELECT
                r.RECOMMENDATION_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.TS AS ENTRY_TS,
                b.CLOSE::FLOAT AS ENTRY_PRICE
            FROM MIP.APP.RECOMMENDATION_LOG r
            LEFT JOIN MIP.MART.MARKET_BARS b
              ON b.SYMBOL = r.SYMBOL
             AND b.MARKET_TYPE = r.MARKET_TYPE
             AND b.INTERVAL_MINUTES = r.INTERVAL_MINUTES
             AND b.TS = r.TS                      -- strict entry bar
            WHERE r.TS >= :v_from_ts
              AND r.TS <= :v_to_ts
        ),
        future_ranked AS (
            SELECT
                e.RECOMMENDATION_ID,
                b.TS AS EXIT_TS,
                b.CLOSE::FLOAT AS EXIT_PRICE,
                ROW_NUMBER() OVER (
                    PARTITION BY e.RECOMMENDATION_ID
                    ORDER BY b.TS
                ) AS FUTURE_RN
            FROM entry_bars e
            JOIN MIP.MART.MARKET_BARS b
              ON b.SYMBOL = e.SYMBOL
             AND b.MARKET_TYPE = e.MARKET_TYPE
             AND b.INTERVAL_MINUTES = e.INTERVAL_MINUTES
             AND b.TS > e.ENTRY_TS                 -- strict future: lookahead-safe
            WHERE e.ENTRY_PRICE IS NOT NULL
              AND e.ENTRY_PRICE <> 0
        ),
        future_bars AS (
            SELECT
                e.RECOMMENDATION_ID,
                e.ENTRY_TS,
                e.ENTRY_PRICE,
                h.HORIZON_BARS,
                fr.EXIT_TS,
                fr.EXIT_PRICE
            FROM entry_bars e
            JOIN horizons h ON 1=1
            LEFT JOIN future_ranked fr
              ON fr.RECOMMENDATION_ID = e.RECOMMENDATION_ID
             AND fr.FUTURE_RN = h.HORIZON_BARS
        )
        SELECT
            fb.RECOMMENDATION_ID,
            fb.HORIZON_BARS,
            fb.ENTRY_TS,
            fb.EXIT_TS,
            fb.ENTRY_PRICE,
            fb.EXIT_PRICE,
            CASE
                WHEN fb.ENTRY_PRICE IS NOT NULL AND fb.ENTRY_PRICE <> 0
                 AND fb.EXIT_PRICE  IS NOT NULL AND fb.EXIT_PRICE  <> 0
                THEN (fb.EXIT_PRICE / fb.ENTRY_PRICE) - 1
                ELSE NULL
            END AS REALIZED_RETURN,
            'LONG' AS DIRECTION,
            CASE
                WHEN fb.ENTRY_PRICE IS NOT NULL AND fb.ENTRY_PRICE <> 0
                 AND fb.EXIT_PRICE  IS NOT NULL AND fb.EXIT_PRICE  <> 0
                THEN ((fb.EXIT_PRICE / fb.ENTRY_PRICE) - 1) >= :v_thr
                ELSE NULL
            END AS HIT_FLAG,
            'THRESHOLD' AS HIT_RULE,
            :v_thr AS MIN_RETURN_THRESHOLD,
            CASE
                WHEN fb.ENTRY_PRICE IS NULL OR fb.ENTRY_PRICE = 0 THEN 'FAILED_NO_ENTRY_BAR'
                WHEN fb.EXIT_PRICE  IS NULL OR fb.EXIT_PRICE  = 0 THEN 'INSUFFICIENT_FUTURE_DATA'
                ELSE 'SUCCESS'
            END AS EVAL_STATUS,
            MIP.APP.F_NOW_BERLIN_NTZ() AS CALCULATED_AT
        FROM future_bars fb
    ) s
      ON t.RECOMMENDATION_ID = s.RECOMMENDATION_ID
     AND t.HORIZON_BARS      = s.HORIZON_BARS
    WHEN MATCHED THEN UPDATE SET
        t.ENTRY_TS             = s.ENTRY_TS,
        t.EXIT_TS              = s.EXIT_TS,
        t.ENTRY_PRICE          = s.ENTRY_PRICE,
        t.EXIT_PRICE           = s.EXIT_PRICE,
        t.REALIZED_RETURN      = s.REALIZED_RETURN,
        t.DIRECTION            = s.DIRECTION,
        t.HIT_FLAG             = s.HIT_FLAG,
        t.HIT_RULE             = s.HIT_RULE,
        t.MIN_RETURN_THRESHOLD = s.MIN_RETURN_THRESHOLD,
        t.EVAL_STATUS          = s.EVAL_STATUS,
        t.CALCULATED_AT        = s.CALCULATED_AT
    WHEN NOT MATCHED THEN INSERT (
        RECOMMENDATION_ID,
        HORIZON_BARS,
        ENTRY_TS,
        EXIT_TS,
        ENTRY_PRICE,
        EXIT_PRICE,
        REALIZED_RETURN,
        DIRECTION,
        HIT_FLAG,
        HIT_RULE,
        MIN_RETURN_THRESHOLD,
        EVAL_STATUS,
        CALCULATED_AT
    ) VALUES (
        s.RECOMMENDATION_ID,
        s.HORIZON_BARS,
        s.ENTRY_TS,
        s.EXIT_TS,
        s.ENTRY_PRICE,
        s.EXIT_PRICE,
        s.REALIZED_RETURN,
        s.DIRECTION,
        s.HIT_FLAG,
        s.HIT_RULE,
        s.MIN_RETURN_THRESHOLD,
        s.EVAL_STATUS,
        s.CALCULATED_AT
    );

    v_merged := SQLROWCOUNT;

    -- counts by horizon for this run's window
    SELECT OBJECT_AGG(HORIZON_BARS, CNT)
      INTO :v_horizon_counts
    FROM (
        SELECT HORIZON_BARS, COUNT(*) AS CNT
        FROM MIP.APP.RECOMMENDATION_OUTCOMES
        WHERE ENTRY_TS >= :v_from_ts
          AND ENTRY_TS <= :v_to_ts
          AND EVAL_STATUS = 'SUCCESS'
        GROUP BY HORIZON_BARS
    );

    CALL MIP.APP.SP_LOG_EVENT(
        'EVALUATION',
        'SP_EVALUATE_RECOMMENDATIONS',
        'SUCCESS',
        :v_merged,
        OBJECT_CONSTRUCT('from_ts', :v_from_ts, 'to_ts', :v_to_ts, 'horizon_counts', :v_horizon_counts),
        NULL,
        :v_run_id,
        NULL
    );

    RETURN 'Upserted ' || :v_merged || ' recommendation outcomes from ' || :v_from_ts || ' to ' || :v_to_ts || '.';

EXCEPTION
    WHEN OTHER THEN
        CALL MIP.APP.SP_LOG_EVENT(
            'EVALUATION',
            'SP_EVALUATE_RECOMMENDATIONS',
            'FAIL',
            :v_merged,
            OBJECT_CONSTRUCT('from_ts', :v_from_ts, 'to_ts', :v_to_ts),
            :SQLERRM,
            :v_run_id,
            NULL
        );
        RAISE;
END;
$$;

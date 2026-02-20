-- /sql/app/105_sp_evaluate_recommendations.sql
-- Purpose: Evaluate forward returns for recommendations over trading-bar horizons
-- Reads horizons from HORIZON_DEFINITION: BAR/DAY use n-th future bar, SESSION uses last bar of the day

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
        OBJECT_CONSTRUCT(
            'scope', 'AGG',
            'step_name', 'evaluation',
            'from_ts', :v_from_ts,
            'to_ts', :v_to_ts,
            'min_return_threshold', :v_thr
        ),
        NULL,
        :v_run_id,
        NULL
    );

    MERGE INTO MIP.APP.RECOMMENDATION_OUTCOMES t
    USING (
        WITH entry_bars AS (
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
             AND b.TS = r.TS
            WHERE r.TS >= :v_from_ts
              AND r.TS <= :v_to_ts
        ),

        -- Match each rec to its bar-based horizons from HORIZON_DEFINITION
        rec_bar_horizons AS (
            SELECT DISTINCT
                e.RECOMMENDATION_ID,
                h.HORIZON_LENGTH AS HORIZON_BARS
            FROM entry_bars e
            JOIN MIP.APP.HORIZON_DEFINITION h
              ON h.INTERVAL_MINUTES = e.INTERVAL_MINUTES
             AND h.IS_ACTIVE = TRUE
             AND h.HORIZON_TYPE IN ('BAR', 'DAY')
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
             AND b.TS > e.ENTRY_TS
            WHERE e.ENTRY_PRICE IS NOT NULL
              AND e.ENTRY_PRICE <> 0
        ),

        -- Bar-based outcomes (BAR / DAY horizons)
        bar_outcomes AS (
            SELECT
                e.RECOMMENDATION_ID,
                e.ENTRY_TS,
                e.ENTRY_PRICE,
                rh.HORIZON_BARS,
                fr.EXIT_TS,
                fr.EXIT_PRICE
            FROM entry_bars e
            JOIN rec_bar_horizons rh ON rh.RECOMMENDATION_ID = e.RECOMMENDATION_ID
            LEFT JOIN future_ranked fr
              ON fr.RECOMMENDATION_ID = e.RECOMMENDATION_ID
             AND fr.FUTURE_RN = rh.HORIZON_BARS
        ),

        -- Session-end (EOD) outcomes: last bar of the same trading day
        eod_exits AS (
            SELECT
                e.RECOMMENDATION_ID,
                b.TS AS EXIT_TS,
                b.CLOSE::FLOAT AS EXIT_PRICE
            FROM entry_bars e
            JOIN MIP.APP.HORIZON_DEFINITION h
              ON h.INTERVAL_MINUTES = e.INTERVAL_MINUTES
             AND h.IS_ACTIVE = TRUE
             AND h.HORIZON_TYPE = 'SESSION'
            JOIN MIP.MART.MARKET_BARS b
              ON b.SYMBOL = e.SYMBOL
             AND b.MARKET_TYPE = e.MARKET_TYPE
             AND b.INTERVAL_MINUTES = e.INTERVAL_MINUTES
             AND b.TS::DATE = e.ENTRY_TS::DATE
             AND b.TS > e.ENTRY_TS
            WHERE e.ENTRY_PRICE IS NOT NULL
              AND e.ENTRY_PRICE <> 0
            QUALIFY ROW_NUMBER() OVER (PARTITION BY e.RECOMMENDATION_ID ORDER BY b.TS DESC) = 1
        ),

        eod_outcomes AS (
            SELECT
                e.RECOMMENDATION_ID,
                e.ENTRY_TS,
                e.ENTRY_PRICE,
                -1 AS HORIZON_BARS,
                eod.EXIT_TS,
                eod.EXIT_PRICE
            FROM entry_bars e
            JOIN eod_exits eod ON eod.RECOMMENDATION_ID = e.RECOMMENDATION_ID
        ),

        -- Combine bar-based + session-end outcomes
        all_outcomes AS (
            SELECT * FROM bar_outcomes
            UNION ALL
            SELECT * FROM eod_outcomes
        )

        SELECT
            ao.RECOMMENDATION_ID,
            ao.HORIZON_BARS,
            ao.ENTRY_TS,
            ao.EXIT_TS,
            ao.ENTRY_PRICE,
            ao.EXIT_PRICE,
            CASE
                WHEN ao.ENTRY_PRICE IS NOT NULL AND ao.ENTRY_PRICE <> 0
                 AND ao.EXIT_PRICE  IS NOT NULL AND ao.EXIT_PRICE  <> 0
                THEN (ao.EXIT_PRICE / ao.ENTRY_PRICE) - 1
                ELSE NULL
            END AS REALIZED_RETURN,
            'LONG' AS DIRECTION,
            CASE
                WHEN ao.ENTRY_PRICE IS NOT NULL AND ao.ENTRY_PRICE <> 0
                 AND ao.EXIT_PRICE  IS NOT NULL AND ao.EXIT_PRICE  <> 0
                THEN ((ao.EXIT_PRICE / ao.ENTRY_PRICE) - 1) >= :v_thr
                ELSE NULL
            END AS HIT_FLAG,
            'THRESHOLD' AS HIT_RULE,
            :v_thr AS MIN_RETURN_THRESHOLD,
            CASE
                WHEN ao.ENTRY_PRICE IS NULL OR ao.ENTRY_PRICE = 0 THEN 'FAILED_NO_ENTRY_BAR'
                WHEN ao.EXIT_PRICE  IS NULL OR ao.EXIT_PRICE  = 0 THEN 'INSUFFICIENT_FUTURE_DATA'
                ELSE 'SUCCESS'
            END AS EVAL_STATUS,
            CURRENT_TIMESTAMP() AS CALCULATED_AT
        FROM all_outcomes ao
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
        OBJECT_CONSTRUCT(
            'scope', 'AGG',
            'step_name', 'evaluation',
            'from_ts', :v_from_ts,
            'to_ts', :v_to_ts,
            'horizon_counts', :v_horizon_counts
        ),
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
            OBJECT_CONSTRUCT(
                'scope', 'AGG',
                'step_name', 'evaluation',
                'from_ts', :v_from_ts,
                'to_ts', :v_to_ts
            ),
            :SQLERRM,
            :v_run_id,
            NULL
        );
        RAISE;
END;
$$;

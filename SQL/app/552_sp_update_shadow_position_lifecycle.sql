/* ================================================================
   552_sp_update_shadow_position_lifecycle.sql
   Daily Position Health V1 - SP_UPDATE_SHADOW_POSITION_LIFECYCLE.

   Two responsibilities:
     1) For every open position in DAILY_POSITION_VERDICT on AS_OF_DATE
        that does not yet have a SHADOW_POSITION_LIFECYCLE row,
        insert one with SHADOW_STATUS = OPEN keyed by POSITION_EPISODE_KEY.
        SHADOW_ENTRY_DATE/PRICE/QUANTITY mirror the real entry.

     2) For every shadow review on AS_OF_DATE with
        SHADOW_VERDICT = EXIT_REVIEW and SHADOW_ACTION_BIAS = EXIT_NOW
        and SHADOW_RUN_STATUS = SUCCESS,
        flip its lifecycle row from OPEN to SIM_EXITED at the official
        AS_OF_DATE close from MIP.MART.MARKET_BARS.

   No re-entry in V1. No real position is touched.
   Idempotent: re-running on the same AS_OF_DATE is a no-op for already
   SIM_EXITED rows, and fills any newly opened lifecycle rows.

   Called from the Python orchestrator after shadow reviews are persisted.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

CREATE OR REPLACE PROCEDURE MIP.APP.SP_UPDATE_SHADOW_POSITION_LIFECYCLE(
    P_AS_OF_DATE DATE
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_inserted NUMBER := 0;
    v_exited   NUMBER := 0;
    v_started  TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_summary  VARIANT;
BEGIN
    /* ---- 1) Insert OPEN lifecycle rows for any new positions ---- */
    MERGE INTO MIP.APP.SHADOW_POSITION_LIFECYCLE tgt
    USING (
        SELECT
            v.POSITION_EPISODE_KEY,
            v.PORTFOLIO_ID,
            v.EPISODE_ID,
            v.SYMBOL,
            v.SIDE,
            COALESCE(p.ENTRY_TS::DATE, v.ENTRY_DATE)        AS SHADOW_ENTRY_DATE,
            COALESCE(p.ENTRY_PRICE, v.ENTRY_PRICE)          AS SHADOW_ENTRY_PRICE,
            COALESCE(p.QUANTITY, 1)                         AS SHADOW_QUANTITY,
            v.AS_OF_DATE                                    AS FIRST_REVIEW_DATE
        FROM MIP.APP.DAILY_POSITION_VERDICT v
        LEFT JOIN MIP.APP.PORTFOLIO_POSITIONS p
               ON p.PORTFOLIO_ID = v.PORTFOLIO_ID
              AND p.SYMBOL = v.SYMBOL
              AND p.ENTRY_TS::DATE = v.ENTRY_DATE
        WHERE v.AS_OF_DATE = :P_AS_OF_DATE
    ) src
    ON tgt.POSITION_EPISODE_KEY = src.POSITION_EPISODE_KEY
    WHEN MATCHED THEN UPDATE SET
        LAST_REVIEW_DATE = :P_AS_OF_DATE,
        UPDATED_AT       = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        POSITION_EPISODE_KEY, PORTFOLIO_ID, EPISODE_ID, SYMBOL, SIDE,
        SHADOW_ENTRY_DATE, SHADOW_ENTRY_PRICE, SHADOW_QUANTITY,
        SHADOW_STATUS, REAL_POSITION_STILL_OPEN_FLAG,
        FIRST_REVIEW_DATE, LAST_REVIEW_DATE,
        SOURCE_RUN_TS, UPDATED_AT
    ) VALUES (
        src.POSITION_EPISODE_KEY, src.PORTFOLIO_ID, src.EPISODE_ID, src.SYMBOL, src.SIDE,
        src.SHADOW_ENTRY_DATE, src.SHADOW_ENTRY_PRICE, src.SHADOW_QUANTITY,
        'OPEN', TRUE,
        src.FIRST_REVIEW_DATE, src.FIRST_REVIEW_DATE,
        :v_started, CURRENT_TIMESTAMP()
    );

    SELECT COUNT(*)
      INTO :v_inserted
      FROM MIP.APP.SHADOW_POSITION_LIFECYCLE
     WHERE FIRST_REVIEW_DATE = :P_AS_OF_DATE;

    /* ---- 2) Apply EXIT_NOW sim-exit at AS_OF_DATE official close ---- */
    MERGE INTO MIP.APP.SHADOW_POSITION_LIFECYCLE tgt
    USING (
        SELECT
            r.POSITION_EPISODE_KEY,
            r.AS_OF_DATE,
            mb.CLOSE                                        AS EXIT_PRICE,
            v.SIDE                                          AS SIDE,
            l.SHADOW_ENTRY_PRICE                            AS SHADOW_ENTRY_PRICE,
            l.SHADOW_ENTRY_DATE                             AS SHADOW_ENTRY_DATE
        FROM MIP.APP.DAILY_POSITION_SHADOW_REVIEW r
        JOIN MIP.APP.SHADOW_POSITION_LIFECYCLE   l
              ON l.POSITION_EPISODE_KEY = r.POSITION_EPISODE_KEY
        JOIN MIP.APP.DAILY_POSITION_VERDICT       v
              ON v.POSITION_EPISODE_KEY = r.POSITION_EPISODE_KEY
             AND v.AS_OF_DATE = r.AS_OF_DATE
        LEFT JOIN MIP.MART.MARKET_BARS mb
              ON mb.SYMBOL = r.SYMBOL
             AND mb.INTERVAL_MINUTES = 1440
             AND mb.TS::DATE = r.AS_OF_DATE
        WHERE r.AS_OF_DATE = :P_AS_OF_DATE
          AND r.SHADOW_VERDICT = 'EXIT_REVIEW'
          AND r.SHADOW_ACTION_BIAS = 'EXIT_NOW'
          AND r.SHADOW_RUN_STATUS = 'SUCCESS'
          AND l.SHADOW_STATUS = 'OPEN'
          AND mb.CLOSE IS NOT NULL
    ) src
    ON tgt.POSITION_EPISODE_KEY = src.POSITION_EPISODE_KEY
    WHEN MATCHED THEN UPDATE SET
        SHADOW_STATUS                = 'SIM_EXITED',
        SHADOW_EXIT_DATE             = src.AS_OF_DATE,
        SHADOW_EXIT_PRICE            = src.EXIT_PRICE,
        SHADOW_EXIT_TRIGGER          = 'SHADOW_COMMITTEE_EXIT_AT_CLOSE',
        SHADOW_EXIT_REASON_CODE      = 'SHADOW_VERDICT_EXIT_REVIEW_EXIT_NOW',
        EXIT_FROM_VERDICT_DATE       = src.AS_OF_DATE,
        DAYS_HELD_SHADOW             = DATEDIFF('day', src.SHADOW_ENTRY_DATE, src.AS_OF_DATE),
        REALIZED_RETURN_SHADOW_PCT   = CASE
                                          WHEN src.SHADOW_ENTRY_PRICE IS NOT NULL
                                               AND src.SHADOW_ENTRY_PRICE > 0
                                               AND src.EXIT_PRICE IS NOT NULL
                                          THEN ((src.EXIT_PRICE - src.SHADOW_ENTRY_PRICE) / src.SHADOW_ENTRY_PRICE) * 100.0
                                          ELSE NULL
                                       END,
        UPDATED_AT                   = CURRENT_TIMESTAMP();

    v_exited := SQLROWCOUNT;

    v_summary := OBJECT_CONSTRUCT(
        'as_of_date', :P_AS_OF_DATE,
        'opened_or_touched_lifecycles', :v_inserted,
        'sim_exited_today', :v_exited,
        'started_at', :v_started,
        'completed_at', CURRENT_TIMESTAMP(),
        'status', 'SUCCESS'
    );

    RETURN :v_summary;

EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'as_of_date', :P_AS_OF_DATE,
            'status', 'FAIL',
            'error', SQLERRM,
            'sqlstate', SQLSTATE,
            'started_at', :v_started,
            'completed_at', CURRENT_TIMESTAMP()
        );
END;
$$;

GRANT USAGE ON PROCEDURE MIP.APP.SP_UPDATE_SHADOW_POSITION_LIFECYCLE(DATE) TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE MIP.APP.SP_UPDATE_SHADOW_POSITION_LIFECYCLE(DATE) TO ROLE MIP_UI_API_ROLE;

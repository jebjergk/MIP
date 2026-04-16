/*  ================================================================
    506_sp_backfill_structural_setups.sql
    MIP Structural Strategy Framework — Historical Backfill
    Phase 2g: Run the full structural pipeline for every trading day
    in the historical dataset to build the setup catalog.
    ================================================================ */

CREATE OR REPLACE PROCEDURE MIP.APP.SP_BACKFILL_STRUCTURAL_SETUPS(
    P_START_DATE  DATE    DEFAULT '2025-02-01',
    P_END_DATE    DATE    DEFAULT NULL,
    P_SYMBOL      VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_end_date    DATE := COALESCE(P_END_DATE, CURRENT_DATE());
    v_run_start   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_current     DATE;
    v_total_days  INTEGER := 0;
    v_total_setups INTEGER := 0;
    v_num_dates   INTEGER := 0;
    c1 CURSOR FOR
        SELECT BAR_DATE FROM TMP_BACKFILL_DATES ORDER BY BAR_DATE;
BEGIN

    -- Build date list
    CREATE OR REPLACE TEMPORARY TABLE TMP_BACKFILL_DATES AS
    SELECT DISTINCT TS::DATE AS BAR_DATE
    FROM MIP.MART.MARKET_BARS
    WHERE INTERVAL_MINUTES = 1440
      AND TS >= :P_START_DATE
      AND TS <= :v_end_date
      AND (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL);

    SELECT COUNT(*) INTO :v_num_dates FROM TMP_BACKFILL_DATES;

    FOR rec IN c1 DO
        v_current := rec.BAR_DATE;
        v_total_days := v_total_days + 1;

        CALL MIP.APP.SP_DETECT_STRUCTURAL_LEVELS(:v_current, :P_SYMBOL, 10, 1.0);
        CALL MIP.APP.SP_COMPUTE_STRUCTURAL_STATE(:v_current, :P_SYMBOL);
        CALL MIP.APP.SP_COMPUTE_REGIME_TAGS(:v_current, :P_SYMBOL);
        CALL MIP.APP.SP_DETECT_STRUCTURAL_SETUPS(:v_current, :P_SYMBOL);
        CALL MIP.APP.SP_UPDATE_SETUP_LIFECYCLE(:v_current, :P_SYMBOL);

        IF (MOD(v_total_days, 50) = 0) THEN
            INSERT INTO MIP.APP.MIP_AUDIT_LOG (EVENT_TYPE, EVENT_TS, DETAILS)
            SELECT 'STRUCTURAL_BACKFILL_PROGRESS', CURRENT_TIMESTAMP(),
                PARSE_JSON('{"days_processed":' || :v_total_days ||
                           ',"current_date":"' || :v_current || '"' ||
                           ',"total_dates":' || :v_num_dates || '}');
        END IF;
    END FOR;

    SELECT COUNT(*) INTO :v_total_setups
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS
    WHERE (:P_SYMBOL IS NULL OR SYMBOL = :P_SYMBOL);

    DROP TABLE IF EXISTS TMP_BACKFILL_DATES;

    RETURN OBJECT_CONSTRUCT(
        'status',         'SUCCESS',
        'start_date',     :P_START_DATE,
        'end_date',       :v_end_date,
        'days_processed', :v_total_days,
        'total_setups',   :v_total_setups,
        'elapsed_sec',    DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;

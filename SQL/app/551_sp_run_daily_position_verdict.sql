/* ================================================================
   551_sp_run_daily_position_verdict.sql
   Daily Position Health V1 - SP_RUN_DAILY_POSITION_VERDICT.

   Deterministic real verdict engine. Produces one row per real open
   position per AS_OF_DATE in MIP.APP.DAILY_POSITION_VERDICT.

   Phase 1 RE-ANCHOR (2026-04-26):
     - Open-position source is now MIP.MART.V_LIVE_OPEN_POSITIONS,
       which is anchored on MIP.LIVE.BROKER_SNAPSHOTS (real broker
       truth) - NOT the legacy horizon-based research book
       (PORTFOLIO_POSITIONS / PORTFOLIO_TRADES with HOLD_UNTIL_INDEX).
     - All horizon math (EXPECTED_HORIZON_DAYS, HORIZON_SOURCE_CODE,
       TIME_EFFICIENCY) is REMOVED from the verdict derivation. The
       physical columns on MIP.APP.DAILY_POSITION_VERDICT remain for
       one validation cycle but are explicitly written as NULL on
       new rows; they will be dropped in Phase 2 once parity is proven.
     - POSITION_EPISODE_KEY is now derived from
       (PORTFOLIO_ID, IBKR_ACCOUNT_ID, SYMBOL, ENTRY_DATE) so it is
       stable across daily snapshots without needing a sim EPISODE_ID.
     - PROPOSAL_ID for the committee baseline link comes directly
       from V_LIVE_OPEN_POSITIONS (sourced from LIVE_ACTIONS), not
       from PORTFOLIO_TRADES.

   No weighted scoring. Precedence rules only.
   No LLM. Template-string summaries.
   No real position changes. Advisory only.

   Idempotency: MERGE on (AS_OF_DATE, POSITION_EPISODE_KEY).
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

CREATE OR REPLACE PROCEDURE MIP.APP.SP_RUN_DAILY_POSITION_VERDICT(
    P_AS_OF_DATE DATE
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_engine_version VARCHAR := '1.1.0-live-anchor';
    v_rows_written NUMBER := 0;
    v_run_started TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_summary VARIANT;
BEGIN
    /* ----------------------------------------------------------------
       Stage the verdict rows into a temp object (CTAS) so the MERGE
       remains a single statement with stable identity.
       ---------------------------------------------------------------- */
    CREATE OR REPLACE TEMPORARY TABLE TMP_DAILY_POSITION_VERDICT_STAGE AS
    WITH
    /* ---- 1) Live open positions today (broker truth) ---- */
    open_positions AS (
        SELECT
            p.PORTFOLIO_ID,
            p.IBKR_ACCOUNT_ID,
            p.SYMBOL,
            p.MARKET_TYPE,
            p.INTERVAL_MINUTES,
            p.ENTRY_TS,
            p.ENTRY_DATE,
            p.ENTRY_PRICE,
            p.QUANTITY,
            'LONG'::VARCHAR(8)                                     AS SIDE,
            DATEDIFF('day', p.ENTRY_DATE, :P_AS_OF_DATE)           AS DAYS_HELD,
            SHA2(
                p.PORTFOLIO_ID::STRING || '|' ||
                COALESCE(p.IBKR_ACCOUNT_ID, 'NO_ACCOUNT') || '|' ||
                p.SYMBOL || '|' ||
                TO_CHAR(p.ENTRY_DATE, 'YYYY-MM-DD'),
                256
            )                                                      AS POSITION_EPISODE_KEY,
            'LIVE_BROKER'::VARCHAR                                 AS POSITION_IDENTITY_SOURCE,
            p.PROPOSAL_ID                                          AS LIVE_PROPOSAL_ID
        FROM MIP.MART.V_LIVE_OPEN_POSITIONS p
        WHERE p.IS_OPEN = TRUE
    ),
    /* ---- 2) Committee baseline context (latest decision per PROPOSAL_ID) ---- */
    committee_baseline AS (
        SELECT
            cfd.PROPOSAL_ID,
            cfd.STANCE,
            cfd.CONFIDENCE,
            cfd.CHAIR_OUTPUT_JSON,
            ROW_NUMBER() OVER (
                PARTITION BY cfd.PROPOSAL_ID
                ORDER BY cfd.DECISION_TS DESC
            ) AS RN
        FROM MIP.APP.COMMITTEE_FINAL_DECISION cfd
    ),
    committee_baseline_latest AS (
        SELECT PROPOSAL_ID, STANCE, CONFIDENCE, CHAIR_OUTPUT_JSON
        FROM committee_baseline
        WHERE RN = 1
    ),
    /* ---- 3) Latest daily bar per symbol on or before AS_OF_DATE ---- */
    latest_bar AS (
        SELECT
            mb.SYMBOL,
            mb.MARKET_TYPE,
            mb.TS::DATE AS BAR_DATE,
            mb.OPEN,
            mb.HIGH,
            mb.LOW,
            mb.CLOSE,
            mb.VOLUME,
            ROW_NUMBER() OVER (
                PARTITION BY mb.SYMBOL, mb.MARKET_TYPE
                ORDER BY mb.TS DESC
            ) AS RN
        FROM MIP.MART.MARKET_BARS mb
        WHERE mb.INTERVAL_MINUTES = 1440
          AND mb.TS::DATE <= :P_AS_OF_DATE
    ),
    latest_bar_one AS (
        SELECT SYMBOL, MARKET_TYPE, BAR_DATE, OPEN, HIGH, LOW, CLOSE
        FROM latest_bar
        WHERE RN = 1
    ),
    /* ---- 4) Today's regime per symbol ---- */
    regime_today AS (
        SELECT
            r.SYMBOL,
            r.MARKET_TYPE,
            r.AS_OF_DATE,
            r.VOL_REGIME,
            r.TREND_REGIME,
            r.RANGE_REGIME,
            ROW_NUMBER() OVER (
                PARTITION BY r.SYMBOL, r.MARKET_TYPE
                ORDER BY r.AS_OF_DATE DESC
            ) AS RN
        FROM MIP.APP.STRUCTURAL_REGIME_TAG r
        WHERE r.AS_OF_DATE <= :P_AS_OF_DATE
    ),
    regime_one AS (
        SELECT SYMBOL, MARKET_TYPE, VOL_REGIME, TREND_REGIME, RANGE_REGIME
        FROM regime_today
        WHERE RN = 1
    ),
    /* ---- 5) Today's structural state per symbol ---- */
    state_today AS (
        SELECT
            s.SYMBOL,
            s.MARKET_TYPE,
            s.AS_OF_DATE,
            s.STRUCTURAL_STATE,
            s.STATE_CONFIDENCE,
            ROW_NUMBER() OVER (
                PARTITION BY s.SYMBOL, s.MARKET_TYPE
                ORDER BY s.AS_OF_DATE DESC
            ) AS RN
        FROM MIP.APP.STRUCTURAL_STATE_LOG s
        WHERE s.AS_OF_DATE <= :P_AS_OF_DATE
    ),
    state_one AS (
        SELECT SYMBOL, MARKET_TYPE, STRUCTURAL_STATE, STATE_CONFIDENCE
        FROM state_today
        WHERE RN = 1
    ),
    /* ---- 6) Nearest support level (proxy for invalidation distance) ---- */
    nearest_support AS (
        SELECT
            l.SYMBOL,
            l.MARKET_TYPE,
            l.LEVEL_PRICE,
            ROW_NUMBER() OVER (
                PARTITION BY l.SYMBOL, l.MARKET_TYPE
                ORDER BY l.LEVEL_SIGNIFICANCE DESC NULLS LAST, l.LAST_TOUCH_DATE DESC NULLS LAST
            ) AS RN
        FROM MIP.APP.STRUCTURAL_LEVEL_CACHE l
        WHERE l.LEVEL_TYPE IN ('SUPPORT', 'support')
          AND l.AS_OF_DATE <= :P_AS_OF_DATE
    ),
    nearest_support_one AS (
        SELECT SYMBOL, MARKET_TYPE, LEVEL_PRICE
        FROM nearest_support
        WHERE RN = 1
    ),
    /* ---- 7) Path metrics: bars from ENTRY_DATE to AS_OF_DATE per position ---- */
    path_bars AS (
        SELECT
            op.POSITION_EPISODE_KEY,
            mb.TS::DATE AS BAR_DATE,
            mb.CLOSE,
            LAG(mb.CLOSE) OVER (
                PARTITION BY op.POSITION_EPISODE_KEY
                ORDER BY mb.TS
            ) AS PREV_CLOSE
        FROM open_positions op
        JOIN MIP.MART.MARKET_BARS mb
          ON mb.SYMBOL = op.SYMBOL
         AND mb.MARKET_TYPE = op.MARKET_TYPE
         AND mb.INTERVAL_MINUTES = 1440
         AND mb.TS::DATE >= op.ENTRY_DATE
         AND mb.TS::DATE <= :P_AS_OF_DATE
    ),
    path_metrics AS (
        SELECT
            POSITION_EPISODE_KEY,
            COUNT(*)                                              AS BARS_OBSERVED,
            SUM(CASE WHEN CLOSE > PREV_CLOSE THEN 1 ELSE 0 END)   AS UP_DAYS,
            COUNT(PREV_CLOSE)                                     AS COMPARABLE_DAYS,
            MAX(CLOSE)                                            AS HIGH_WATER_CLOSE,
            MIN(CLOSE)                                            AS TROUGH_CLOSE,
            MAX_BY(CLOSE, BAR_DATE)                               AS LAST_CLOSE,
            MIN_BY(CLOSE, BAR_DATE)                               AS FIRST_CLOSE
        FROM path_bars
        GROUP BY POSITION_EPISODE_KEY
    )
    /* ---- 8) Stitch everything together and derive verdict ---- */
    SELECT
        :P_AS_OF_DATE                                  AS AS_OF_DATE,
        op.POSITION_EPISODE_KEY,
        op.PORTFOLIO_ID,
        CAST(NULL AS NUMBER(38,0))                     AS EPISODE_ID,
        op.SYMBOL,
        op.SIDE,
        op.ENTRY_DATE,
        op.ENTRY_TS,
        op.ENTRY_PRICE,
        op.DAYS_HELD,

        /* ---- BASELINE_QUALITY: anchored to committee record only ---- */
        CASE
            WHEN cb.PROPOSAL_ID IS NULL THEN 'LOW'
            WHEN cb.STANCE = 'APPROVE' AND cb.CONFIDENCE >= 0.70 THEN 'HIGH'
            WHEN cb.STANCE = 'APPROVE' AND COALESCE(cb.CONFIDENCE, 0.0) < 0.70 THEN 'MEDIUM'
            ELSE 'MEDIUM'
        END                                            AS BASELINE_QUALITY,

        /* ---- THESIS_INTEGRITY: derived from current structural state, NOT from baseline ---- */
        CASE
            WHEN st.STRUCTURAL_STATE IS NULL THEN 'PARTIAL'
            WHEN UPPER(st.STRUCTURAL_STATE) LIKE '%BEAR%'
                 AND UPPER(COALESCE(rg.TREND_REGIME, 'NEUTRAL')) LIKE '%BEAR%' THEN 'FAILED'
            WHEN UPPER(st.STRUCTURAL_STATE) LIKE '%BEAR%' THEN 'WEAKENING'
            WHEN UPPER(st.STRUCTURAL_STATE) LIKE '%BULL%' THEN 'ALIGNED'
            ELSE 'PARTIAL'
        END                                            AS THESIS_INTEGRITY,

        /* ---- PATH_QUALITY: from bar sequence since entry ---- */
        CASE
            WHEN pm.COMPARABLE_DAYS IS NULL OR pm.COMPARABLE_DAYS = 0 THEN 'ACCEPTABLE'
            WHEN pm.LAST_CLOSE >= COALESCE(pm.HIGH_WATER_CLOSE, pm.LAST_CLOSE) * 0.97
                 AND (pm.UP_DAYS::FLOAT / NULLIF(pm.COMPARABLE_DAYS, 0)) >= 0.55
                 AND pm.LAST_CLOSE > op.ENTRY_PRICE THEN 'CONSTRUCTIVE'
            WHEN (pm.UP_DAYS::FLOAT / NULLIF(pm.COMPARABLE_DAYS, 0)) < 0.40
                 OR pm.LAST_CLOSE < pm.FIRST_CLOSE * 0.93 THEN 'DETERIORATING'
            WHEN (pm.UP_DAYS::FLOAT / NULLIF(pm.COMPARABLE_DAYS, 0)) BETWEEN 0.40 AND 0.50
                 AND ABS(pm.LAST_CLOSE - pm.FIRST_CLOSE) / NULLIF(pm.FIRST_CLOSE, 0) < 0.03 THEN 'NOISY'
            ELSE 'ACCEPTABLE'
        END                                            AS PATH_QUALITY,

        /* ---- REGIME_ALIGNMENT: TREND_REGIME match for LONG ---- */
        CASE
            WHEN rg.TREND_REGIME IS NULL THEN 'NEUTRAL'
            WHEN UPPER(rg.TREND_REGIME) LIKE '%BULL%' THEN 'SUPPORTIVE'
            WHEN UPPER(rg.TREND_REGIME) LIKE '%BEAR%' THEN 'ADVERSE'
            ELSE 'NEUTRAL'
        END                                            AS REGIME_ALIGNMENT,

        /* ---- DISTANCE_TO_INVALIDATION_PCT and FRAGILITY ---- */
        CASE
            WHEN ns.LEVEL_PRICE IS NOT NULL AND lb.CLOSE IS NOT NULL AND lb.CLOSE > 0
                 AND ns.LEVEL_PRICE < lb.CLOSE
                 THEN ((lb.CLOSE - ns.LEVEL_PRICE) / lb.CLOSE) * 100.0
            ELSE NULL
        END                                            AS DISTANCE_TO_INVALIDATION_PCT,

        CASE
            WHEN ns.LEVEL_PRICE IS NULL OR lb.CLOSE IS NULL OR lb.CLOSE <= 0 THEN 'COMFORTABLE'
            WHEN ns.LEVEL_PRICE >= lb.CLOSE THEN 'FRAGILE'
            WHEN ((lb.CLOSE - ns.LEVEL_PRICE) / lb.CLOSE) * 100.0 < 5.0 THEN 'FRAGILE'
            WHEN ((lb.CLOSE - ns.LEVEL_PRICE) / lb.CLOSE) * 100.0 < 10.0 THEN 'TIGHTENING'
            ELSE 'COMFORTABLE'
        END                                            AS FRAGILITY,

        /* ---- UNREALIZED_PNL_PCT (LONG-only V1) ---- */
        CASE
            WHEN op.ENTRY_PRICE IS NOT NULL AND op.ENTRY_PRICE > 0 AND lb.CLOSE IS NOT NULL
                 THEN ((lb.CLOSE - op.ENTRY_PRICE) / op.ENTRY_PRICE) * 100.0
            ELSE NULL
        END                                            AS UNREALIZED_PNL_PCT,

        /* ---- Side artifacts forwarded for downstream summary CTE ---- */
        cb.PROPOSAL_ID                                 AS BASELINE_PROPOSAL_ID,
        cb.STANCE                                      AS BASELINE_STANCE,
        cb.CONFIDENCE                                  AS BASELINE_CONFIDENCE,
        st.STRUCTURAL_STATE                            AS STRUCTURAL_STATE_NOW,
        rg.TREND_REGIME                                AS TREND_REGIME_NOW,
        rg.VOL_REGIME                                  AS VOL_REGIME_NOW,
        lb.CLOSE                                       AS LATEST_CLOSE,
        lb.BAR_DATE                                    AS LATEST_BAR_DATE,
        ns.LEVEL_PRICE                                 AS NEAREST_SUPPORT_PRICE,
        pm.BARS_OBSERVED                               AS PATH_BARS_OBSERVED,
        pm.UP_DAYS                                     AS PATH_UP_DAYS,
        pm.COMPARABLE_DAYS                             AS PATH_COMPARABLE_DAYS,
        pm.HIGH_WATER_CLOSE                            AS PATH_HIGH_WATER_CLOSE,
        pm.LAST_CLOSE                                  AS PATH_LAST_CLOSE,
        op.POSITION_IDENTITY_SOURCE                    AS POSITION_IDENTITY_SOURCE,
        op.IBKR_ACCOUNT_ID                             AS IBKR_ACCOUNT_ID,
        op.QUANTITY                                    AS QUANTITY
    FROM open_positions op
    LEFT JOIN committee_baseline_latest cb
           ON cb.PROPOSAL_ID = op.LIVE_PROPOSAL_ID
    LEFT JOIN latest_bar_one lb
           ON lb.SYMBOL = op.SYMBOL
          AND lb.MARKET_TYPE = op.MARKET_TYPE
    LEFT JOIN regime_one rg
           ON rg.SYMBOL = op.SYMBOL
          AND rg.MARKET_TYPE = op.MARKET_TYPE
    LEFT JOIN state_one st
           ON st.SYMBOL = op.SYMBOL
          AND st.MARKET_TYPE = op.MARKET_TYPE
    LEFT JOIN nearest_support_one ns
           ON ns.SYMBOL = op.SYMBOL
          AND ns.MARKET_TYPE = op.MARKET_TYPE
    LEFT JOIN path_metrics pm
           ON pm.POSITION_EPISODE_KEY = op.POSITION_EPISODE_KEY;

    /* ----------------------------------------------------------------
       Derive HEALTH_STATE, VERDICT, severity, and summaries from the
       staged dimension columns. Use precedence rules - no scoring.
       Horizon-based TIME_EFFICIENCY rules removed in Phase 1.
       ---------------------------------------------------------------- */
    CREATE OR REPLACE TEMPORARY TABLE TMP_DAILY_POSITION_VERDICT_FINAL AS
    SELECT
        s.*,
        CASE
            WHEN s.THESIS_INTEGRITY = 'FAILED' THEN 'BROKEN'
            WHEN s.PATH_QUALITY = 'DETERIORATING'
                 AND s.REGIME_ALIGNMENT = 'ADVERSE'
                 AND s.FRAGILITY = 'FRAGILE' THEN 'BROKEN'
            WHEN s.THESIS_INTEGRITY = 'WEAKENING' AND s.FRAGILITY = 'FRAGILE' THEN 'BROKEN'
            WHEN s.PATH_QUALITY = 'DETERIORATING' THEN 'DRIFTING'
            WHEN s.REGIME_ALIGNMENT = 'ADVERSE' THEN 'SLOW'
            WHEN s.PATH_QUALITY = 'NOISY' THEN 'SLOW'
            WHEN s.THESIS_INTEGRITY = 'ALIGNED' AND s.PATH_QUALITY = 'CONSTRUCTIVE' THEN 'STRENGTHENING'
            ELSE 'INTACT'
        END AS HEALTH_STATE_DERIVED
    FROM TMP_DAILY_POSITION_VERDICT_STAGE s;

    /* ----------------------------------------------------------------
       MERGE into the canonical table by (AS_OF_DATE, POSITION_EPISODE_KEY).

       Horizon columns (EXPECTED_HORIZON_DAYS, HORIZON_SOURCE_CODE,
       TIME_EFFICIENCY) are intentionally written as NULL on every
       new live-anchored row. The columns themselves remain on the
       table for one validation cycle and will be dropped in Phase 2.
       ---------------------------------------------------------------- */
    MERGE INTO MIP.APP.DAILY_POSITION_VERDICT tgt
    USING (
        SELECT
            AS_OF_DATE,
            POSITION_EPISODE_KEY,
            PORTFOLIO_ID,
            EPISODE_ID,
            SYMBOL,
            SIDE,
            ENTRY_DATE,
            ENTRY_TS,
            ENTRY_PRICE,
            DAYS_HELD,

            CASE
                WHEN HEALTH_STATE_DERIVED IN ('BROKEN') THEN 'EXIT_REVIEW'
                WHEN HEALTH_STATE_DERIVED IN ('SLOW', 'DRIFTING') THEN 'WATCH'
                ELSE 'KEEP'
            END                                                  AS VERDICT,
            HEALTH_STATE_DERIVED                                 AS HEALTH_STATE,

            BASELINE_QUALITY,
            THESIS_INTEGRITY,
            PATH_QUALITY,
            REGIME_ALIGNMENT,
            CAST(NULL AS VARCHAR(20))                            AS TIME_EFFICIENCY,
            FRAGILITY,

            CASE
                WHEN HEALTH_STATE_DERIVED = 'BROKEN' THEN 'HIGH'
                WHEN HEALTH_STATE_DERIVED IN ('DRIFTING') THEN 'MEDIUM'
                WHEN HEALTH_STATE_DERIVED IN ('SLOW') THEN 'MEDIUM'
                ELSE 'LOW'
            END                                                  AS SEVERITY,

            CAST(NULL AS NUMBER(18,0))                           AS EXPECTED_HORIZON_DAYS,
            CAST(NULL AS VARCHAR(30))                            AS HORIZON_SOURCE_CODE,
            DISTANCE_TO_INVALIDATION_PCT,
            UNREALIZED_PNL_PCT,

            /* ---- PRIMARY_REASON_CODE: which precedence rule fired ---- */
            CASE
                WHEN THESIS_INTEGRITY = 'FAILED' THEN 'THESIS_FAILED'
                WHEN PATH_QUALITY = 'DETERIORATING'
                     AND REGIME_ALIGNMENT = 'ADVERSE'
                     AND FRAGILITY = 'FRAGILE' THEN 'PATH_REGIME_FRAGILITY_BROKEN'
                WHEN THESIS_INTEGRITY = 'WEAKENING' AND FRAGILITY = 'FRAGILE' THEN 'WEAKENING_AND_FRAGILE'
                WHEN PATH_QUALITY = 'DETERIORATING' THEN 'PATH_DETERIORATING'
                WHEN REGIME_ALIGNMENT = 'ADVERSE' THEN 'REGIME_ADVERSE'
                WHEN PATH_QUALITY = 'NOISY' THEN 'PATH_NOISY'
                WHEN THESIS_INTEGRITY = 'ALIGNED' AND PATH_QUALITY = 'CONSTRUCTIVE' THEN 'THESIS_PATH_STRONG'
                ELSE 'THESIS_INTACT'
            END                                                  AS PRIMARY_REASON_CODE,

            CASE
                WHEN THESIS_INTEGRITY = 'FAILED' THEN 'Thesis broken: structural state has inverted vs the original direction.'
                WHEN PATH_QUALITY = 'DETERIORATING'
                     AND REGIME_ALIGNMENT = 'ADVERSE'
                     AND FRAGILITY = 'FRAGILE' THEN 'Position is deteriorating into adverse regime with thin invalidation cushion.'
                WHEN THESIS_INTEGRITY = 'WEAKENING' AND FRAGILITY = 'FRAGILE' THEN 'Thesis weakening while invalidation cushion has thinned.'
                WHEN PATH_QUALITY = 'DETERIORATING' THEN 'Realized path since entry is deteriorating.'
                WHEN REGIME_ALIGNMENT = 'ADVERSE' THEN 'Macro and trend regime is adverse to the position direction.'
                WHEN PATH_QUALITY = 'NOISY' THEN 'Path since entry has been choppy with no clear progress.'
                WHEN THESIS_INTEGRITY = 'ALIGNED' AND PATH_QUALITY = 'CONSTRUCTIVE' THEN 'Thesis intact and realized path is constructive.'
                ELSE 'Thesis intact and structure remains supportive.'
            END                                                  AS PRIMARY_REASON_TEXT,

            /* ---- OBSERVATION_SUMMARY: what we observe today ---- */
            'Held ' || DAYS_HELD || 'd. Path: ' || PATH_QUALITY
              || '. Regime: ' || REGIME_ALIGNMENT
              || '. Fragility: ' || FRAGILITY
              || '. Thesis: ' || THESIS_INTEGRITY
              || '. PnL ' || COALESCE(TO_VARCHAR(ROUND(UNREALIZED_PNL_PCT, 2)), 'n/a') || '%.'
                                                                AS OBSERVATION_SUMMARY,

            /* ---- VERDICT_SUMMARY: what the verdict is ---- */
            CASE
                WHEN HEALTH_STATE_DERIVED IN ('BROKEN') THEN 'EXIT_REVIEW: ' || HEALTH_STATE_DERIVED || ' health state.'
                WHEN HEALTH_STATE_DERIVED IN ('SLOW', 'DRIFTING') THEN 'WATCH: ' || HEALTH_STATE_DERIVED || ' health state, monitor next sessions.'
                ELSE 'KEEP: ' || HEALTH_STATE_DERIVED || ' health state, no action.'
            END                                                  AS VERDICT_SUMMARY,

            /* ---- WHY_SUMMARY: the dominant evidence ---- */
            CASE
                WHEN THESIS_INTEGRITY = 'FAILED' THEN 'Structural state has inverted vs entry direction.'
                WHEN PATH_QUALITY = 'DETERIORATING'
                     AND REGIME_ALIGNMENT = 'ADVERSE'
                     AND FRAGILITY = 'FRAGILE' THEN 'Triple-confluence: deteriorating path, adverse regime, thin cushion.'
                WHEN THESIS_INTEGRITY = 'WEAKENING' AND FRAGILITY = 'FRAGILE' THEN 'Weakening thesis combined with thin invalidation cushion.'
                WHEN PATH_QUALITY = 'DETERIORATING' THEN 'Realized closes trending downward since entry.'
                WHEN REGIME_ALIGNMENT = 'ADVERSE' THEN 'Trend regime ' || COALESCE(TREND_REGIME_NOW, 'unknown') || ' opposes position direction.'
                WHEN PATH_QUALITY = 'NOISY' THEN 'Up-day ratio low and net move negligible.'
                WHEN THESIS_INTEGRITY = 'ALIGNED' AND PATH_QUALITY = 'CONSTRUCTIVE' THEN 'Structure ' || COALESCE(STRUCTURAL_STATE_NOW, 'unknown') || ' and path constructive.'
                ELSE 'Structure ' || COALESCE(STRUCTURAL_STATE_NOW, 'unknown') || ' remains supportive.'
            END                                                  AS WHY_SUMMARY,

            OBJECT_CONSTRUCT(
                'baseline_proposal_id',         BASELINE_PROPOSAL_ID,
                'baseline_stance',              BASELINE_STANCE,
                'baseline_confidence',          BASELINE_CONFIDENCE,
                'structural_state_now',         STRUCTURAL_STATE_NOW,
                'trend_regime_now',             TREND_REGIME_NOW,
                'vol_regime_now',               VOL_REGIME_NOW,
                'latest_close',                 LATEST_CLOSE,
                'latest_bar_date',              LATEST_BAR_DATE,
                'nearest_support_price',        NEAREST_SUPPORT_PRICE,
                'path_bars_observed',           PATH_BARS_OBSERVED,
                'path_up_days',                 PATH_UP_DAYS,
                'path_comparable_days',         PATH_COMPARABLE_DAYS,
                'path_high_water_close',        PATH_HIGH_WATER_CLOSE,
                'path_last_close',              PATH_LAST_CLOSE,
                'position_identity_source',     POSITION_IDENTITY_SOURCE,
                'ibkr_account_id',              IBKR_ACCOUNT_ID,
                'live_quantity',                QUANTITY
            )                                                    AS DETAIL_JSON,

            :v_engine_version                                    AS ENGINE_VERSION,
            :v_run_started                                       AS SOURCE_RUN_TS
        FROM TMP_DAILY_POSITION_VERDICT_FINAL
    ) src
    ON tgt.AS_OF_DATE = src.AS_OF_DATE
       AND tgt.POSITION_EPISODE_KEY = src.POSITION_EPISODE_KEY
    WHEN MATCHED THEN UPDATE SET
        PORTFOLIO_ID = src.PORTFOLIO_ID,
        EPISODE_ID = src.EPISODE_ID,
        SYMBOL = src.SYMBOL,
        SIDE = src.SIDE,
        ENTRY_DATE = src.ENTRY_DATE,
        ENTRY_TS = src.ENTRY_TS,
        ENTRY_PRICE = src.ENTRY_PRICE,
        DAYS_HELD = src.DAYS_HELD,
        VERDICT = src.VERDICT,
        HEALTH_STATE = src.HEALTH_STATE,
        BASELINE_QUALITY = src.BASELINE_QUALITY,
        THESIS_INTEGRITY = src.THESIS_INTEGRITY,
        PATH_QUALITY = src.PATH_QUALITY,
        REGIME_ALIGNMENT = src.REGIME_ALIGNMENT,
        TIME_EFFICIENCY = src.TIME_EFFICIENCY,
        FRAGILITY = src.FRAGILITY,
        SEVERITY = src.SEVERITY,
        EXPECTED_HORIZON_DAYS = src.EXPECTED_HORIZON_DAYS,
        HORIZON_SOURCE_CODE = src.HORIZON_SOURCE_CODE,
        DISTANCE_TO_INVALIDATION_PCT = src.DISTANCE_TO_INVALIDATION_PCT,
        UNREALIZED_PNL_PCT = src.UNREALIZED_PNL_PCT,
        PRIMARY_REASON_CODE = src.PRIMARY_REASON_CODE,
        PRIMARY_REASON_TEXT = src.PRIMARY_REASON_TEXT,
        OBSERVATION_SUMMARY = src.OBSERVATION_SUMMARY,
        VERDICT_SUMMARY = src.VERDICT_SUMMARY,
        WHY_SUMMARY = src.WHY_SUMMARY,
        DETAIL_JSON = src.DETAIL_JSON,
        ENGINE_VERSION = src.ENGINE_VERSION,
        SOURCE_RUN_TS = src.SOURCE_RUN_TS,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        AS_OF_DATE, POSITION_EPISODE_KEY, PORTFOLIO_ID, EPISODE_ID, SYMBOL, SIDE,
        ENTRY_DATE, ENTRY_TS, ENTRY_PRICE, DAYS_HELD,
        VERDICT, HEALTH_STATE,
        BASELINE_QUALITY, THESIS_INTEGRITY, PATH_QUALITY, REGIME_ALIGNMENT,
        TIME_EFFICIENCY, FRAGILITY, SEVERITY,
        EXPECTED_HORIZON_DAYS, HORIZON_SOURCE_CODE,
        DISTANCE_TO_INVALIDATION_PCT, UNREALIZED_PNL_PCT,
        PRIMARY_REASON_CODE, PRIMARY_REASON_TEXT,
        OBSERVATION_SUMMARY, VERDICT_SUMMARY, WHY_SUMMARY,
        DETAIL_JSON, ENGINE_VERSION, SOURCE_RUN_TS
    ) VALUES (
        src.AS_OF_DATE, src.POSITION_EPISODE_KEY, src.PORTFOLIO_ID, src.EPISODE_ID, src.SYMBOL, src.SIDE,
        src.ENTRY_DATE, src.ENTRY_TS, src.ENTRY_PRICE, src.DAYS_HELD,
        src.VERDICT, src.HEALTH_STATE,
        src.BASELINE_QUALITY, src.THESIS_INTEGRITY, src.PATH_QUALITY, src.REGIME_ALIGNMENT,
        src.TIME_EFFICIENCY, src.FRAGILITY, src.SEVERITY,
        src.EXPECTED_HORIZON_DAYS, src.HORIZON_SOURCE_CODE,
        src.DISTANCE_TO_INVALIDATION_PCT, src.UNREALIZED_PNL_PCT,
        src.PRIMARY_REASON_CODE, src.PRIMARY_REASON_TEXT,
        src.OBSERVATION_SUMMARY, src.VERDICT_SUMMARY, src.WHY_SUMMARY,
        src.DETAIL_JSON, src.ENGINE_VERSION, src.SOURCE_RUN_TS
    );

    SELECT COUNT(*) INTO :v_rows_written FROM TMP_DAILY_POSITION_VERDICT_FINAL;

    v_summary := OBJECT_CONSTRUCT(
        'as_of_date', :P_AS_OF_DATE,
        'rows_written', :v_rows_written,
        'engine_version', :v_engine_version,
        'started_at', :v_run_started,
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
            'started_at', :v_run_started,
            'completed_at', CURRENT_TIMESTAMP()
        );
END;
$$;

GRANT USAGE ON PROCEDURE MIP.APP.SP_RUN_DAILY_POSITION_VERDICT(DATE) TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE MIP.APP.SP_RUN_DAILY_POSITION_VERDICT(DATE) TO ROLE MIP_UI_API_ROLE;

/* ================================================================
   601_sp_refresh_committee_bakeoff.sql
   Pure-SQL refresh procedure for the Committee Bake-off Sidecar.

   Idempotent. Reads only. Writes only to:
     - MIP.APP.COMMITTEE_BAKEOFF_LATCH
     - MIP.APP.COMMITTEE_BAKEOFF_OUTCOME

   No other side effects. Never touches LIVE_ACTIONS, COMMITTEE_*,
   SHADOW_*, broker tables, or proposal tables.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

CREATE OR REPLACE PROCEDURE MIP.APP.SP_REFRESH_COMMITTEE_BAKEOFF(
    P_START_DATE DATE DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start_date     DATE := COALESCE(
                                P_START_DATE,
                                TRY_TO_DATE(
                                    (SELECT CONFIG_VALUE
                                       FROM MIP.APP.COMMITTEE_BAKEOFF_CONFIG
                                      WHERE CONFIG_KEY = 'BAKEOFF_START_DATE')
                                ),
                                DATE'2026-04-19'
                            );
    v_latch_count    NUMBER := 0;
    v_outcome_count  NUMBER := 0;
    v_excluded_count NUMBER := 0;
    v_summary        VARIANT;
BEGIN

    -- ============================================================
    -- 1. Refresh LATCH table
    --    Compute first-ENTER (or first terminal non-enter) per board.
    -- ============================================================

    -- Wipe rows for any proposal that has at least one in-scope shadow session,
    -- so re-runs are fully idempotent (covers latches whose own decision_ts
    -- pre-dates the shadow cutoff but whose proposal is in scope).
    DELETE FROM MIP.APP.COMMITTEE_BAKEOFF_OUTCOME
     WHERE PROPOSAL_ID IN (
        SELECT PROPOSAL_ID FROM MIP.APP.SHADOW_BOARD_SESSION
         WHERE CREATED_AT::DATE >= :v_start_date
     );
    DELETE FROM MIP.APP.COMMITTEE_BAKEOFF_LATCH
     WHERE PROPOSAL_ID IN (
        SELECT PROPOSAL_ID FROM MIP.APP.SHADOW_BOARD_SESSION
         WHERE CREATED_AT::DATE >= :v_start_date
     );

    -- ----- REAL board candidates (durable final decisions) ----------
    INSERT INTO MIP.APP.COMMITTEE_BAKEOFF_LATCH (
        PROPOSAL_ID, BOARD_KIND, SYMBOL, SIDE, DIRECTION,
        DECISION_TS, HEARING_ID, SNAPSHOT_ID, ACTION_ID, SHADOW_SESSION_ID,
        LATCH_SOURCE_TABLE, LATCH_SOURCE_ID, LATCH_REASON,
        RAW_STANCE, NORMALIZED_ACTION, CONFIDENCE,
        EVAL_CONFIG_JSON, CONFIG_STATUS, CONFIG_STATUS_REASON
    )
    WITH shared_proposals AS (
        SELECT DISTINCT s.PROPOSAL_ID
          FROM MIP.APP.SHADOW_BOARD_SESSION s
         WHERE s.CREATED_AT::DATE >= :v_start_date
    ),
    -- Real decisions: include ANY decision on a shared proposal, regardless of
    -- date. The real board's verdict is durable; it may have been recorded
    -- the day before shadow first ran, but the proposal is still in scope.
    real_decisions AS (
        SELECT
            fd.PROPOSAL_ID,
            fd.HEARING_ID,
            fd.SNAPSHOT_ID,
            fd.ACTION_ID,
            fd.STANCE                        AS RAW_STANCE,
            CASE WHEN UPPER(fd.STANCE) IN ('APPROVE','APPROVE_REDUCED')
                 THEN 'ENTER' ELSE 'CASH' END AS NORMALIZED_ACTION,
            fd.CONFIDENCE,
            fd.DECISION_TS,
            fd.FINAL_DECISION_ID
        FROM MIP.APP.COMMITTEE_FINAL_DECISION fd
        JOIN shared_proposals sp ON sp.PROPOSAL_ID = fd.PROPOSAL_ID
    ),
    real_first_enter AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY PROPOSAL_ID
                   ORDER BY DECISION_TS, FINAL_DECISION_ID
               ) AS RN_ENTER
          FROM real_decisions
         WHERE NORMALIZED_ACTION = 'ENTER'
    ),
    real_first_any AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY PROPOSAL_ID
                   ORDER BY DECISION_TS, FINAL_DECISION_ID
               ) AS RN_ANY
          FROM real_decisions
    ),
    real_latched AS (
        SELECT
            COALESCE(e.PROPOSAL_ID, a.PROPOSAL_ID)        AS PROPOSAL_ID,
            COALESCE(e.HEARING_ID, a.HEARING_ID)          AS HEARING_ID,
            COALESCE(e.SNAPSHOT_ID, a.SNAPSHOT_ID)        AS SNAPSHOT_ID,
            COALESCE(e.ACTION_ID, a.ACTION_ID)            AS ACTION_ID,
            COALESCE(e.RAW_STANCE, a.RAW_STANCE)          AS RAW_STANCE,
            COALESCE(e.NORMALIZED_ACTION, a.NORMALIZED_ACTION) AS NORMALIZED_ACTION,
            COALESCE(e.CONFIDENCE, a.CONFIDENCE)          AS CONFIDENCE,
            COALESCE(e.DECISION_TS, a.DECISION_TS)        AS DECISION_TS,
            COALESCE(e.FINAL_DECISION_ID, a.FINAL_DECISION_ID) AS FINAL_DECISION_ID,
            CASE WHEN e.PROPOSAL_ID IS NOT NULL
                 THEN 'FIRST_ENTER' ELSE 'FIRST_TERMINAL_NON_ENTER' END AS LATCH_REASON
        FROM (SELECT * FROM real_first_enter WHERE RN_ENTER = 1) e
        FULL OUTER JOIN (SELECT * FROM real_first_any WHERE RN_ANY = 1) a
            ON a.PROPOSAL_ID = e.PROPOSAL_ID
    ),
    real_with_action AS (
        SELECT rl.*,
               sn.SYMBOL, sn.SIDE,
               la.DIRECTION,
               la.ENTRY_ZONE_LOW, la.ENTRY_ZONE_HIGH,
               la.INVALIDATION_LEVEL, la.INVALIDATION_RULE,
               la.TRAIL_STYLE, la.TRAIL_PARAMS,
               la.MAX_HOLD_BARS, la.EXIT_STYLE,
               la.MARKET_TYPE
          FROM real_latched rl
          JOIN shared_proposals sp ON sp.PROPOSAL_ID = rl.PROPOSAL_ID
          LEFT JOIN MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT sn ON sn.PROPOSAL_ID = rl.PROPOSAL_ID
          LEFT JOIN MIP.LIVE.LIVE_ACTIONS la ON la.ACTION_ID = rl.ACTION_ID
    )
    SELECT
        r.PROPOSAL_ID,
        'REAL'                    AS BOARD_KIND,
        COALESCE(r.SYMBOL, 'UNKNOWN'),
        COALESCE(r.SIDE,
                 CASE UPPER(r.DIRECTION) WHEN 'LONG' THEN 'BUY' WHEN 'SHORT' THEN 'SELL' ELSE 'BUY' END
        )                         AS SIDE,
        COALESCE(UPPER(r.DIRECTION),
                 CASE UPPER(r.SIDE) WHEN 'BUY' THEN 'LONG' WHEN 'SELL' THEN 'SHORT' ELSE 'LONG' END
        )                         AS DIRECTION,
        r.DECISION_TS,
        r.HEARING_ID,
        r.SNAPSHOT_ID,
        r.ACTION_ID,
        NULL                      AS SHADOW_SESSION_ID,
        'MIP.APP.COMMITTEE_FINAL_DECISION' AS LATCH_SOURCE_TABLE,
        r.FINAL_DECISION_ID::VARCHAR       AS LATCH_SOURCE_ID,
        r.LATCH_REASON,
        r.RAW_STANCE,
        r.NORMALIZED_ACTION,
        r.CONFIDENCE,
        OBJECT_CONSTRUCT(
            'symbol',              r.SYMBOL,
            'direction',           UPPER(r.DIRECTION),
            'market_type',         r.MARKET_TYPE,
            'entry_zone_low',      r.ENTRY_ZONE_LOW,
            'entry_zone_high',     r.ENTRY_ZONE_HIGH,
            'stop_price',          r.INVALIDATION_LEVEL,
            'stop_rule',           r.INVALIDATION_RULE,
            'tp_price',            NULL,
            'trail_style',         r.TRAIL_STYLE,
            'trail_params',        r.TRAIL_PARAMS,
            'exit_style',          r.EXIT_STYLE,
            'max_hold_bars',       COALESCE(r.MAX_HOLD_BARS, 20),
            'fill_validity_bars',  5,
            'provenance', OBJECT_CONSTRUCT(
                'entry_zone',  CASE WHEN r.ENTRY_ZONE_LOW IS NOT NULL
                                    THEN 'LIVE_ACTIONS' ELSE 'MISSING' END,
                'stop_price',  CASE WHEN r.INVALIDATION_LEVEL IS NOT NULL
                                    THEN 'LIVE_ACTIONS' ELSE 'MISSING' END,
                'tp_price',    'NOT_SPECIFIED_IN_SOURCE',
                'trail',       CASE WHEN r.TRAIL_STYLE IS NOT NULL
                                    THEN 'LIVE_ACTIONS' ELSE 'MISSING' END,
                'max_hold',    CASE WHEN r.MAX_HOLD_BARS IS NOT NULL
                                    THEN 'LIVE_ACTIONS' ELSE 'DEFAULT_20' END
            )
        )                         AS EVAL_CONFIG_JSON,
        CASE
            WHEN r.NORMALIZED_ACTION = 'CASH' THEN 'NOT_APPLICABLE'
            WHEN r.SYMBOL IS NULL OR r.ENTRY_ZONE_LOW IS NULL OR r.INVALIDATION_LEVEL IS NULL
                THEN 'UNSCORABLE'
            WHEN r.TRAIL_STYLE IS NULL OR r.MAX_HOLD_BARS IS NULL
                THEN 'PARTIAL'
            ELSE 'SCORABLE'
        END                       AS CONFIG_STATUS,
        CASE
            WHEN r.NORMALIZED_ACTION = 'CASH'
                THEN 'No position; scoring not applicable.'
            WHEN r.SYMBOL IS NULL
                THEN 'Missing symbol on STRUCTURAL_PROPOSAL_SNAPSHOT.'
            WHEN r.ENTRY_ZONE_LOW IS NULL
                THEN 'Missing entry zone on LIVE_ACTIONS.'
            WHEN r.INVALIDATION_LEVEL IS NULL
                THEN 'Missing stop (INVALIDATION_LEVEL) on LIVE_ACTIONS.'
            WHEN r.TRAIL_STYLE IS NULL AND r.MAX_HOLD_BARS IS NULL
                THEN 'No trail style and no MAX_HOLD_BARS; horizon defaulted to 20.'
            WHEN r.TRAIL_STYLE IS NULL
                THEN 'No trail style on LIVE_ACTIONS; stop+horizon scoring only.'
            WHEN r.MAX_HOLD_BARS IS NULL
                THEN 'No MAX_HOLD_BARS on LIVE_ACTIONS; defaulted to 20.'
            ELSE 'All required fields sourced from LIVE_ACTIONS.'
        END                       AS CONFIG_STATUS_REASON
      FROM real_with_action r
     WHERE r.PROPOSAL_ID IS NOT NULL;

    -- ----- SHADOW board candidates (sessions + chair rulings) ------
    INSERT INTO MIP.APP.COMMITTEE_BAKEOFF_LATCH (
        PROPOSAL_ID, BOARD_KIND, SYMBOL, SIDE, DIRECTION,
        DECISION_TS, HEARING_ID, SNAPSHOT_ID, ACTION_ID, SHADOW_SESSION_ID,
        LATCH_SOURCE_TABLE, LATCH_SOURCE_ID, LATCH_REASON,
        RAW_STANCE, NORMALIZED_ACTION, CONFIDENCE,
        EVAL_CONFIG_JSON, CONFIG_STATUS, CONFIG_STATUS_REASON
    )
    WITH shadow_decisions AS (
        SELECT
            s.PROPOSAL_ID,
            s.HEARING_ID,
            s.SNAPSHOT_ID,
            s.SESSION_ID                     AS SHADOW_SESSION_ID,
            s.SHADOW_STANCE                  AS RAW_STANCE,
            CASE WHEN UPPER(s.SHADOW_STANCE) IN ('APPROVE','APPROVE_REDUCED')
                 THEN 'ENTER' ELSE 'CASH' END AS NORMALIZED_ACTION,
            s.SHADOW_CONFIDENCE              AS CONFIDENCE,
            COALESCE(s.COMPLETED_AT, s.CREATED_AT) AS DECISION_TS
        FROM MIP.APP.SHADOW_BOARD_SESSION s
        WHERE s.CREATED_AT::DATE >= :v_start_date
          AND s.STATUS IN ('COMPLETE','DEGRADED')
          AND s.SHADOW_STANCE IS NOT NULL
    ),
    shadow_first_enter AS (
        SELECT *, ROW_NUMBER() OVER (
                       PARTITION BY PROPOSAL_ID
                       ORDER BY DECISION_TS, SHADOW_SESSION_ID
                  ) AS RN_ENTER
          FROM shadow_decisions WHERE NORMALIZED_ACTION = 'ENTER'
    ),
    shadow_first_any AS (
        SELECT *, ROW_NUMBER() OVER (
                       PARTITION BY PROPOSAL_ID
                       ORDER BY DECISION_TS, SHADOW_SESSION_ID
                  ) AS RN_ANY
          FROM shadow_decisions
    ),
    shadow_latched AS (
        SELECT
            COALESCE(e.PROPOSAL_ID, a.PROPOSAL_ID)               AS PROPOSAL_ID,
            COALESCE(e.HEARING_ID, a.HEARING_ID)                 AS HEARING_ID,
            COALESCE(e.SNAPSHOT_ID, a.SNAPSHOT_ID)               AS SNAPSHOT_ID,
            COALESCE(e.SHADOW_SESSION_ID, a.SHADOW_SESSION_ID)   AS SHADOW_SESSION_ID,
            COALESCE(e.RAW_STANCE, a.RAW_STANCE)                 AS RAW_STANCE,
            COALESCE(e.NORMALIZED_ACTION, a.NORMALIZED_ACTION)   AS NORMALIZED_ACTION,
            COALESCE(e.CONFIDENCE, a.CONFIDENCE)                 AS CONFIDENCE,
            COALESCE(e.DECISION_TS, a.DECISION_TS)               AS DECISION_TS,
            CASE WHEN e.PROPOSAL_ID IS NOT NULL
                 THEN 'FIRST_ENTER' ELSE 'FIRST_TERMINAL_NON_ENTER' END AS LATCH_REASON
        FROM (SELECT * FROM shadow_first_enter WHERE RN_ENTER = 1) e
        FULL OUTER JOIN (SELECT * FROM shadow_first_any WHERE RN_ANY = 1) a
            ON a.PROPOSAL_ID = e.PROPOSAL_ID
    ),
    -- For shadow we ALWAYS source numeric levels from STRUCTURAL_PROPOSAL_SNAPSHOT
    -- (the shadow board itself produces only advisory text). Per design, this
    -- forces CONFIG_STATUS = PARTIAL with explicit provenance — never silent.
    shadow_with_levels AS (
        SELECT sl.*,
               sn.SYMBOL, sn.SIDE,
               UPPER(sn.SIDE)                       AS DIRECTION,
               -- numeric fallback from proposal-time snapshot
               TRY_TO_DOUBLE(sn.ENTRY_ZONE_JSON:low::STRING)        AS ENTRY_ZONE_LOW,
               TRY_TO_DOUBLE(sn.ENTRY_ZONE_JSON:high::STRING)       AS ENTRY_ZONE_HIGH,
               TRY_TO_DOUBLE(sn.INVALIDATION_JSON:level::STRING)    AS STOP_PRICE,
               sn.INVALIDATION_JSON:rule::STRING                    AS STOP_RULE,
               sn.TRAILING_STYLE                                    AS TRAIL_STYLE
          FROM shadow_latched sl
          LEFT JOIN MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT sn
                 ON sn.PROPOSAL_ID = sl.PROPOSAL_ID
    )
    SELECT
        s.PROPOSAL_ID,
        'SHADOW'                  AS BOARD_KIND,
        COALESCE(s.SYMBOL, 'UNKNOWN'),
        COALESCE(s.SIDE,
                 CASE UPPER(s.DIRECTION) WHEN 'LONG' THEN 'BUY' WHEN 'SHORT' THEN 'SELL' ELSE 'BUY' END
        )                         AS SIDE,
        COALESCE(UPPER(s.DIRECTION), 'LONG') AS DIRECTION,
        s.DECISION_TS,
        s.HEARING_ID,
        s.SNAPSHOT_ID,
        NULL                      AS ACTION_ID,
        s.SHADOW_SESSION_ID,
        'MIP.APP.SHADOW_BOARD_SESSION' AS LATCH_SOURCE_TABLE,
        s.SHADOW_SESSION_ID       AS LATCH_SOURCE_ID,
        s.LATCH_REASON,
        s.RAW_STANCE,
        s.NORMALIZED_ACTION,
        s.CONFIDENCE,
        OBJECT_CONSTRUCT(
            'symbol',             s.SYMBOL,
            'direction',          UPPER(s.DIRECTION),
            'market_type',        NULL,
            'entry_zone_low',     s.ENTRY_ZONE_LOW,
            'entry_zone_high',    s.ENTRY_ZONE_HIGH,
            'stop_price',         s.STOP_PRICE,
            'stop_rule',          s.STOP_RULE,
            'tp_price',           NULL,
            'trail_style',        s.TRAIL_STYLE,
            'trail_params',       NULL,
            'exit_style',         NULL,
            'max_hold_bars',      20,
            'fill_validity_bars', 5,
            'provenance', OBJECT_CONSTRUCT(
                'entry_zone', 'STRUCTURAL_PROPOSAL_SNAPSHOT (shadow board did not emit numeric levels)',
                'stop_price', 'STRUCTURAL_PROPOSAL_SNAPSHOT (shadow board did not emit numeric levels)',
                'tp_price',   'NOT_SPECIFIED_IN_SOURCE',
                'trail',      CASE WHEN s.TRAIL_STYLE IS NOT NULL
                                   THEN 'STRUCTURAL_PROPOSAL_SNAPSHOT' ELSE 'MISSING' END,
                'max_hold',   'DEFAULT_20'
            )
        )                         AS EVAL_CONFIG_JSON,
        CASE
            WHEN s.NORMALIZED_ACTION = 'CASH' THEN 'NOT_APPLICABLE'
            WHEN s.SYMBOL IS NULL OR s.ENTRY_ZONE_LOW IS NULL OR s.STOP_PRICE IS NULL
                THEN 'UNSCORABLE'
            -- Shadow always falls back to proposal-snapshot levels by design,
            -- so any ENTER row is at best PARTIAL.
            ELSE 'PARTIAL'
        END                       AS CONFIG_STATUS,
        CASE
            WHEN s.NORMALIZED_ACTION = 'CASH'
                THEN 'No position; scoring not applicable.'
            WHEN s.SYMBOL IS NULL
                THEN 'Missing symbol on STRUCTURAL_PROPOSAL_SNAPSHOT.'
            WHEN s.ENTRY_ZONE_LOW IS NULL
                THEN 'Missing entry zone on STRUCTURAL_PROPOSAL_SNAPSHOT.'
            WHEN s.STOP_PRICE IS NULL
                THEN 'Missing stop on STRUCTURAL_PROPOSAL_SNAPSHOT.'
            ELSE 'Numeric stop/entry inherited from STRUCTURAL_PROPOSAL_SNAPSHOT (shadow board did not emit own numeric config). No TP from shadow source.'
        END                       AS CONFIG_STATUS_REASON
      FROM shadow_with_levels s
     WHERE s.PROPOSAL_ID IS NOT NULL;

    SELECT COUNT(*) INTO :v_latch_count
      FROM MIP.APP.COMMITTEE_BAKEOFF_LATCH
     WHERE PROPOSAL_ID IN (
        SELECT PROPOSAL_ID FROM MIP.APP.SHADOW_BOARD_SESSION
         WHERE CREATED_AT::DATE >= :v_start_date
     );

    -- ============================================================
    -- 2. Build OUTCOME rows
    -- ============================================================

    -- For ENTER rows we evaluate forward daily bars (INTERVAL_MINUTES=1440).
    -- For CASH rows the outcome is trivially CASH.

    INSERT INTO MIP.APP.COMMITTEE_BAKEOFF_OUTCOME (
        LATCH_ID, PROPOSAL_ID, BOARD_KIND, SYMBOL, DIRECTION,
        DECISION_TS, NORMALIZED_ACTION, LATCH_CONFIG_STATUS,
        ENTRY_BAR_TS, ENTRY_PRICE, EXIT_BAR_TS, EXIT_PRICE, BARS_HELD,
        RAW_OUTCOME,
        REALIZED_RETURN_PCT, MAX_FAVORABLE_PCT, MAX_ADVERSE_PCT,
        COMPARISON_LABEL,
        SCORING_EXCLUDED_FLAG, SCORING_EXCLUDED_REASON,
        EVAL_NOTES_JSON
    )
    WITH latched AS (
        SELECT *
          FROM MIP.APP.COMMITTEE_BAKEOFF_LATCH
         WHERE PROPOSAL_ID IN (
            SELECT PROPOSAL_ID FROM MIP.APP.SHADOW_BOARD_SESSION
             WHERE CREATED_AT::DATE >= :v_start_date
         )
    ),
    -- Forward daily bars per latched ENTER candidate.
    enter_candidates AS (
        SELECT l.LATCH_ID, l.PROPOSAL_ID, l.BOARD_KIND, l.SYMBOL,
               l.DIRECTION, l.DECISION_TS, l.NORMALIZED_ACTION, l.CONFIG_STATUS,
               l.EVAL_CONFIG_JSON,
               TRY_TO_DOUBLE(l.EVAL_CONFIG_JSON:entry_zone_low::STRING)  AS ENTRY_LOW,
               TRY_TO_DOUBLE(l.EVAL_CONFIG_JSON:entry_zone_high::STRING) AS ENTRY_HIGH,
               TRY_TO_DOUBLE(l.EVAL_CONFIG_JSON:stop_price::STRING)      AS STOP_PRICE,
               TRY_TO_DOUBLE(l.EVAL_CONFIG_JSON:tp_price::STRING)        AS TP_PRICE,
               COALESCE(l.EVAL_CONFIG_JSON:max_hold_bars::INTEGER, 20)    AS MAX_HOLD_BARS,
               COALESCE(l.EVAL_CONFIG_JSON:fill_validity_bars::INTEGER, 5) AS FILL_VALIDITY_BARS
          FROM latched l
         WHERE l.NORMALIZED_ACTION = 'ENTER'
           AND l.CONFIG_STATUS IN ('SCORABLE','PARTIAL')
    ),
    -- Dedupe daily bars to one row per (latch, date), THEN assign contiguous RN.
    forward_bars_dedup AS (
        SELECT
            ec.LATCH_ID,
            ec.PROPOSAL_ID,
            ec.BOARD_KIND,
            ec.SYMBOL,
            ec.DIRECTION,
            ec.DECISION_TS,
            ec.ENTRY_LOW, ec.ENTRY_HIGH, ec.STOP_PRICE, ec.TP_PRICE,
            ec.MAX_HOLD_BARS, ec.FILL_VALIDITY_BARS,
            mb.TS,
            mb.OPEN, mb.HIGH, mb.LOW, mb.CLOSE
        FROM enter_candidates ec
        JOIN MIP.MART.MARKET_BARS mb
          ON mb.SYMBOL = ec.SYMBOL
         AND mb.INTERVAL_MINUTES = 1440
         AND mb.TS::DATE >= ec.DECISION_TS::DATE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ec.LATCH_ID, mb.TS::DATE
            ORDER BY mb.INGESTED_AT DESC, mb.SOURCE DESC
        ) = 1
    ),
    forward_bars AS (
        SELECT
            d.*,
            ROW_NUMBER() OVER (PARTITION BY d.LATCH_ID ORDER BY d.TS) AS RN
          FROM forward_bars_dedup d
    ),
    -- Detect first fill bar (LONG: bar.LOW <= entry_high; SHORT: bar.HIGH >= entry_low)
    fills AS (
        SELECT *,
               CASE
                   WHEN DIRECTION = 'LONG'  AND LOW  <= ENTRY_HIGH THEN TRUE
                   WHEN DIRECTION = 'SHORT' AND HIGH >= ENTRY_LOW  THEN TRUE
                   ELSE FALSE
               END AS FILL_FLAG
          FROM forward_bars
    ),
    first_fill AS (
        SELECT LATCH_ID,
               MIN_BY(TS, RN)        AS ENTRY_BAR_TS,
               MIN_BY(OPEN, RN)      AS ENTRY_BAR_OPEN,
               MIN_BY(LOW, RN)       AS ENTRY_BAR_LOW,
               MIN_BY(HIGH, RN)      AS ENTRY_BAR_HIGH,
               MIN(RN)               AS ENTRY_RN
          FROM fills
         WHERE FILL_FLAG = TRUE
         GROUP BY LATCH_ID
    ),
    -- Did we actually fill within the validity window?
    fill_resolution AS (
        SELECT ec.*,
               ff.ENTRY_BAR_TS, ff.ENTRY_BAR_OPEN, ff.ENTRY_BAR_LOW, ff.ENTRY_BAR_HIGH, ff.ENTRY_RN,
               CASE
                   WHEN ff.LATCH_ID IS NULL THEN 'NO_BARS'
                   WHEN ff.ENTRY_RN <= ec.FILL_VALIDITY_BARS THEN 'FILLED'
                   ELSE 'NO_FILL'
               END AS FILL_STATE,
               CASE
                   WHEN ec.DIRECTION = 'LONG'
                       THEN LEAST(ff.ENTRY_BAR_OPEN, ec.ENTRY_HIGH)
                   WHEN ec.DIRECTION = 'SHORT'
                       THEN GREATEST(ff.ENTRY_BAR_OPEN, ec.ENTRY_LOW)
               END AS ENTRY_PRICE
          FROM enter_candidates ec
          LEFT JOIN first_fill ff ON ff.LATCH_ID = ec.LATCH_ID
    ),
    -- Forward bars limited to evaluation window (entry bar .. entry+max_hold)
    eval_bars AS (
        SELECT fb.LATCH_ID, fb.PROPOSAL_ID, fb.BOARD_KIND, fb.SYMBOL, fb.DIRECTION,
               fb.DECISION_TS, fb.STOP_PRICE, fb.TP_PRICE, fb.MAX_HOLD_BARS,
               fb.TS, fb.OPEN, fb.HIGH, fb.LOW, fb.CLOSE,
               fb.RN,
               fr.ENTRY_RN, fr.ENTRY_PRICE, fr.FILL_STATE,
               (fb.RN - fr.ENTRY_RN) AS BARS_SINCE_ENTRY,
               -- Stop hit (close-based: SINGLE_CLOSE_BEYOND)
               CASE
                   WHEN fr.FILL_STATE = 'FILLED' AND fb.RN >= fr.ENTRY_RN
                        AND fb.STOP_PRICE IS NOT NULL
                   THEN
                       CASE
                           WHEN fb.DIRECTION = 'LONG'  AND fb.CLOSE < fb.STOP_PRICE THEN TRUE
                           WHEN fb.DIRECTION = 'SHORT' AND fb.CLOSE > fb.STOP_PRICE THEN TRUE
                           ELSE FALSE
                       END
                   ELSE FALSE
               END AS STOP_HIT,
               -- TP hit (intrabar high/low touch)
               CASE
                   WHEN fr.FILL_STATE = 'FILLED' AND fb.RN >= fr.ENTRY_RN
                        AND fb.TP_PRICE IS NOT NULL
                   THEN
                       CASE
                           WHEN fb.DIRECTION = 'LONG'  AND fb.HIGH >= fb.TP_PRICE THEN TRUE
                           WHEN fb.DIRECTION = 'SHORT' AND fb.LOW  <= fb.TP_PRICE THEN TRUE
                           ELSE FALSE
                       END
                   ELSE FALSE
               END AS TP_HIT
          FROM forward_bars fb
          JOIN fill_resolution fr ON fr.LATCH_ID = fb.LATCH_ID
         WHERE fr.FILL_STATE = 'FILLED'
           AND fb.RN BETWEEN fr.ENTRY_RN AND (fr.ENTRY_RN + fb.MAX_HOLD_BARS)
    ),
    -- First exit bar per latch (stop OR tp)
    exit_events AS (
        SELECT LATCH_ID,
               MIN(CASE WHEN STOP_HIT THEN RN END) AS STOP_RN,
               MIN(CASE WHEN TP_HIT   THEN RN END) AS TP_RN,
               MAX(RN)                              AS LAST_RN,
               MAX(MAX_HOLD_BARS)                   AS MAX_HOLD_BARS
          FROM eval_bars
         GROUP BY LATCH_ID
    ),
    exit_resolved AS (
        SELECT
            ee.LATCH_ID,
            ee.STOP_RN, ee.TP_RN, ee.LAST_RN,
            CASE
                WHEN ee.STOP_RN IS NOT NULL AND ee.TP_RN IS NOT NULL
                     AND ee.STOP_RN = ee.TP_RN
                    THEN 'TIE_BOTH_TOUCHED'
                WHEN ee.STOP_RN IS NOT NULL AND ee.TP_RN IS NULL
                    THEN 'STOPPED'
                WHEN ee.TP_RN IS NOT NULL AND ee.STOP_RN IS NULL
                    THEN 'TP_HIT'
                WHEN ee.STOP_RN IS NOT NULL AND ee.TP_RN IS NOT NULL AND ee.STOP_RN < ee.TP_RN
                    THEN 'STOPPED'
                WHEN ee.STOP_RN IS NOT NULL AND ee.TP_RN IS NOT NULL AND ee.TP_RN < ee.STOP_RN
                    THEN 'TP_HIT'
                ELSE 'OPEN_AT_HORIZON'
            END AS RAW_OUTCOME,
            CASE
                WHEN ee.STOP_RN IS NOT NULL AND ee.TP_RN IS NOT NULL
                     AND ee.STOP_RN = ee.TP_RN THEN ee.STOP_RN
                WHEN ee.STOP_RN IS NOT NULL AND ee.TP_RN IS NULL    THEN ee.STOP_RN
                WHEN ee.TP_RN IS NOT NULL   AND ee.STOP_RN IS NULL  THEN ee.TP_RN
                WHEN ee.STOP_RN IS NOT NULL AND ee.TP_RN IS NOT NULL AND ee.STOP_RN < ee.TP_RN THEN ee.STOP_RN
                WHEN ee.STOP_RN IS NOT NULL AND ee.TP_RN IS NOT NULL AND ee.TP_RN < ee.STOP_RN THEN ee.TP_RN
                ELSE ee.LAST_RN
            END AS EXIT_RN
          FROM exit_events ee
    ),
    -- Pull the bar at the exit rn for price
    exit_prices AS (
        SELECT er.LATCH_ID, er.RAW_OUTCOME, er.EXIT_RN,
               eb.TS    AS EXIT_BAR_TS,
               eb.OPEN  AS EXIT_OPEN,
               eb.HIGH  AS EXIT_HIGH,
               eb.LOW   AS EXIT_LOW,
               eb.CLOSE AS EXIT_CLOSE,
               eb.DIRECTION,
               eb.STOP_PRICE, eb.TP_PRICE,
               eb.ENTRY_PRICE, eb.ENTRY_RN
          FROM exit_resolved er
          JOIN eval_bars eb ON eb.LATCH_ID = er.LATCH_ID AND eb.RN = er.EXIT_RN
    ),
    -- Path stats per latch (MFE/MAE on intrabar high/low vs entry, sizing-agnostic %)
    path_stats AS (
        SELECT
            eb.LATCH_ID,
            eb.DIRECTION,
            eb.ENTRY_PRICE,
            CASE
                WHEN eb.DIRECTION = 'LONG'
                    THEN MAX((eb.HIGH - eb.ENTRY_PRICE) / NULLIF(eb.ENTRY_PRICE, 0))
                ELSE MAX((eb.ENTRY_PRICE - eb.LOW) / NULLIF(eb.ENTRY_PRICE, 0))
            END AS MAX_FAVORABLE_PCT,
            CASE
                WHEN eb.DIRECTION = 'LONG'
                    THEN MIN((eb.LOW  - eb.ENTRY_PRICE) / NULLIF(eb.ENTRY_PRICE, 0))
                ELSE MIN((eb.ENTRY_PRICE - eb.HIGH) / NULLIF(eb.ENTRY_PRICE, 0))
            END AS MAX_ADVERSE_PCT
          FROM eval_bars eb
         GROUP BY eb.LATCH_ID, eb.DIRECTION, eb.ENTRY_PRICE
    ),
    -- Final per-latch outcome row (ENTER path)
    enter_outcomes AS (
        SELECT
            fr.LATCH_ID, fr.PROPOSAL_ID, fr.BOARD_KIND, fr.SYMBOL,
            fr.DIRECTION, fr.DECISION_TS, fr.CONFIG_STATUS,
            fr.FILL_STATE,
            fr.ENTRY_BAR_TS, fr.ENTRY_PRICE,
            ep.EXIT_BAR_TS, 
            CASE
                WHEN ep.RAW_OUTCOME = 'STOPPED' THEN
                    -- Conservative: assume stop fill at next-day open or stop price worst-case
                    -- For SINGLE_CLOSE_BEYOND, stop is signaled at close → exit at close.
                    ep.EXIT_CLOSE
                WHEN ep.RAW_OUTCOME = 'TP_HIT' THEN ep.TP_PRICE
                WHEN ep.RAW_OUTCOME = 'TIE_BOTH_TOUCHED' THEN ep.EXIT_CLOSE
                WHEN ep.RAW_OUTCOME = 'OPEN_AT_HORIZON' THEN ep.EXIT_CLOSE
                ELSE ep.EXIT_CLOSE
            END AS EXIT_PRICE,
            (ep.EXIT_RN - ep.ENTRY_RN) AS BARS_HELD,
            COALESCE(ep.RAW_OUTCOME,
                CASE fr.FILL_STATE
                    WHEN 'NO_FILL' THEN 'NO_FILL'
                    WHEN 'NO_BARS' THEN 'NO_FILL'
                    ELSE 'OPEN_AT_HORIZON'
                END
            ) AS RAW_OUTCOME,
            ps.MAX_FAVORABLE_PCT, ps.MAX_ADVERSE_PCT
          FROM fill_resolution fr
          LEFT JOIN exit_prices ep ON ep.LATCH_ID = fr.LATCH_ID
          LEFT JOIN path_stats  ps ON ps.LATCH_ID = fr.LATCH_ID
    )
    SELECT
        l.LATCH_ID,
        l.PROPOSAL_ID,
        l.BOARD_KIND,
        l.SYMBOL,
        l.DIRECTION,
        l.DECISION_TS,
        l.NORMALIZED_ACTION,
        l.CONFIG_STATUS                AS LATCH_CONFIG_STATUS,
        eo.ENTRY_BAR_TS,
        eo.ENTRY_PRICE,
        eo.EXIT_BAR_TS,
        eo.EXIT_PRICE,
        eo.BARS_HELD,
        CASE
            WHEN l.NORMALIZED_ACTION = 'CASH'      THEN 'CASH'
            WHEN l.CONFIG_STATUS = 'UNSCORABLE'    THEN 'NO_FILL'  -- placeholder; excluded
            WHEN eo.RAW_OUTCOME IS NULL            THEN 'NO_FILL'
            ELSE eo.RAW_OUTCOME
        END                            AS RAW_OUTCOME,
        CASE
            WHEN l.NORMALIZED_ACTION = 'CASH' THEN 0.0
            WHEN eo.RAW_OUTCOME IS NULL OR eo.ENTRY_PRICE IS NULL OR eo.EXIT_PRICE IS NULL THEN NULL
            WHEN l.DIRECTION = 'LONG'
                THEN (eo.EXIT_PRICE - eo.ENTRY_PRICE) / NULLIF(eo.ENTRY_PRICE, 0)
            ELSE (eo.ENTRY_PRICE - eo.EXIT_PRICE) / NULLIF(eo.ENTRY_PRICE, 0)
        END                            AS REALIZED_RETURN_PCT,
        eo.MAX_FAVORABLE_PCT,
        eo.MAX_ADVERSE_PCT,
        NULL                           AS COMPARISON_LABEL,  -- filled in pass 2
        CASE
            WHEN l.CONFIG_STATUS = 'UNSCORABLE' THEN TRUE
            WHEN l.NORMALIZED_ACTION = 'ENTER' AND eo.RAW_OUTCOME IS NULL
                 AND eo.FILL_STATE = 'NO_BARS' THEN TRUE
            WHEN eo.RAW_OUTCOME = 'TIE_BOTH_TOUCHED' THEN TRUE
            ELSE FALSE
        END                            AS SCORING_EXCLUDED_FLAG,
        CASE
            WHEN l.CONFIG_STATUS = 'UNSCORABLE'
                THEN 'CONFIG_UNSCORABLE: ' || COALESCE(l.CONFIG_STATUS_REASON, '')
            WHEN l.NORMALIZED_ACTION = 'ENTER' AND eo.RAW_OUTCOME IS NULL
                 AND eo.FILL_STATE = 'NO_BARS'
                THEN 'NO_FORWARD_MARKET_DATA for symbol after decision date'
            WHEN eo.RAW_OUTCOME = 'TIE_BOTH_TOUCHED'
                THEN 'TIE_BOTH_TOUCHED: stop and TP both touched in the same daily bar; sequence cannot be determined from daily OHLC'
            ELSE NULL
        END                            AS SCORING_EXCLUDED_REASON,
        OBJECT_CONSTRUCT(
            'fill_state', eo.FILL_STATE,
            'eval_window_bars', eo.BARS_HELD,
            'evaluator', 'daily_ohlc_v1',
            'evaluator_notes',
                'LONG fill: bar.LOW<=entry_high. Stop: SINGLE_CLOSE_BEYOND on close. ' ||
                'TIE_BOTH_TOUCHED never invents sequencing.'
        )                              AS EVAL_NOTES_JSON
      FROM MIP.APP.COMMITTEE_BAKEOFF_LATCH l
      LEFT JOIN enter_outcomes eo ON eo.LATCH_ID = l.LATCH_ID
     WHERE l.PROPOSAL_ID IN (
        SELECT PROPOSAL_ID FROM MIP.APP.SHADOW_BOARD_SESSION
         WHERE CREATED_AT::DATE >= :v_start_date
     );

    -- ============================================================
    -- 3. Pass 2: fill in COMPARISON_LABEL based on sibling row.
    -- ============================================================

    -- Helper inline: compute label from (real_action, real_return, shadow_action, shadow_return)
    -- Null returns → UNCOMPARABLE.

    UPDATE MIP.APP.COMMITTEE_BAKEOFF_OUTCOME tgt
       SET COMPARISON_LABEL = src.COMPARISON_LABEL
      FROM (
        WITH pivoted AS (
            SELECT
                o.PROPOSAL_ID,
                MAX(CASE WHEN o.BOARD_KIND='REAL'   THEN o.NORMALIZED_ACTION END) AS REAL_ACTION,
                MAX(CASE WHEN o.BOARD_KIND='SHADOW' THEN o.NORMALIZED_ACTION END) AS SHADOW_ACTION,
                MAX(CASE WHEN o.BOARD_KIND='REAL'   THEN o.REALIZED_RETURN_PCT END) AS REAL_RET,
                MAX(CASE WHEN o.BOARD_KIND='SHADOW' THEN o.REALIZED_RETURN_PCT END) AS SHADOW_RET,
                MAX(CASE WHEN o.BOARD_KIND='REAL'   THEN o.SCORING_EXCLUDED_FLAG END) AS REAL_EXCL,
                MAX(CASE WHEN o.BOARD_KIND='SHADOW' THEN o.SCORING_EXCLUDED_FLAG END) AS SHADOW_EXCL
              FROM MIP.APP.COMMITTEE_BAKEOFF_OUTCOME o
              GROUP BY o.PROPOSAL_ID
        )
        SELECT
            PROPOSAL_ID,
            CASE
                WHEN COALESCE(REAL_EXCL, TRUE) OR COALESCE(SHADOW_EXCL, TRUE) THEN 'UNCOMPARABLE'
                WHEN REAL_ACTION='CASH' AND SHADOW_ACTION='CASH' THEN 'BOTH_CASH'
                WHEN REAL_ACTION='ENTER' AND SHADOW_ACTION='CASH' AND REAL_RET >= 0 THEN 'REAL_WIN__SHADOW_CASH'
                WHEN REAL_ACTION='ENTER' AND SHADOW_ACTION='CASH' AND REAL_RET <  0 THEN 'REAL_LOSS__SHADOW_CASH'
                WHEN REAL_ACTION='CASH'  AND SHADOW_ACTION='ENTER' AND SHADOW_RET >= 0 THEN 'REAL_CASH__SHADOW_WIN'
                WHEN REAL_ACTION='CASH'  AND SHADOW_ACTION='ENTER' AND SHADOW_RET <  0 THEN 'REAL_CASH__SHADOW_LOSS'
                WHEN REAL_ACTION='ENTER' AND SHADOW_ACTION='ENTER' AND REAL_RET >= 0 AND SHADOW_RET >= 0 THEN 'REAL_WIN__SHADOW_WIN'
                WHEN REAL_ACTION='ENTER' AND SHADOW_ACTION='ENTER' AND REAL_RET <  0 AND SHADOW_RET <  0 THEN 'REAL_LOSS__SHADOW_LOSS'
                WHEN REAL_ACTION='ENTER' AND SHADOW_ACTION='ENTER' AND REAL_RET >= 0 AND SHADOW_RET <  0 THEN 'REAL_WIN__SHADOW_LOSS'
                WHEN REAL_ACTION='ENTER' AND SHADOW_ACTION='ENTER' AND REAL_RET <  0 AND SHADOW_RET >= 0 THEN 'REAL_LOSS__SHADOW_WIN'
                ELSE 'UNCOMPARABLE'
            END AS COMPARISON_LABEL
          FROM pivoted
      ) src
     WHERE src.PROPOSAL_ID = tgt.PROPOSAL_ID;

    SELECT COUNT(*) INTO :v_outcome_count
      FROM MIP.APP.COMMITTEE_BAKEOFF_OUTCOME
     WHERE PROPOSAL_ID IN (
        SELECT PROPOSAL_ID FROM MIP.APP.SHADOW_BOARD_SESSION
         WHERE CREATED_AT::DATE >= :v_start_date
     );
    SELECT COUNT(*) INTO :v_excluded_count
      FROM MIP.APP.COMMITTEE_BAKEOFF_OUTCOME
     WHERE PROPOSAL_ID IN (
        SELECT PROPOSAL_ID FROM MIP.APP.SHADOW_BOARD_SESSION
         WHERE CREATED_AT::DATE >= :v_start_date
     )
       AND SCORING_EXCLUDED_FLAG = TRUE;

    v_summary := OBJECT_CONSTRUCT(
        'start_date',     :v_start_date::STRING,
        'latch_rows',     :v_latch_count,
        'outcome_rows',   :v_outcome_count,
        'excluded_rows',  :v_excluded_count,
        'refreshed_at',   CURRENT_TIMESTAMP()::STRING
    );
    RETURN v_summary;
END;
$$;

GRANT USAGE ON PROCEDURE MIP.APP.SP_REFRESH_COMMITTEE_BAKEOFF(DATE) TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE MIP.APP.SP_REFRESH_COMMITTEE_BAKEOFF(DATE) TO ROLE MIP_UI_API_ROLE;

-- v_proposal_board_symbol_dossier.sql
-- Phase 4 active Agentic Proposal Board evidence contract.
--
-- Grain: one row per symbol / market_type / as_of_date / portfolio context.
-- This view deliberately does not expose a top-level proposed direction,
-- setup family, candidate id, or final entry zone.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

CREATE OR REPLACE VIEW MIP.MART.V_PROPOSAL_BOARD_SYMBOL_DOSSIER AS
WITH cfg AS (
    SELECT
        COALESCE(MAX(IFF(CONFIG_KEY = 'PROPOSAL_BOARD_SHORT_RESEARCH_VISIBLE', TRY_TO_BOOLEAN(CONFIG_VALUE), NULL)), TRUE) AS SHORT_RESEARCH_VISIBLE,
        COALESCE(MAX(IFF(CONFIG_KEY = 'PROPOSAL_BOARD_SHORT_LIVE_ENABLED', TRY_TO_BOOLEAN(CONFIG_VALUE), NULL)), FALSE) AS SHORT_LIVE_ENABLED,
        COALESCE(MAX(IFF(CONFIG_KEY = 'PROPOSAL_BOARD_FX_LIVE_ENABLED', TRY_TO_BOOLEAN(CONFIG_VALUE), NULL)), FALSE) AS FX_LIVE_ENABLED
    FROM MIP.APP.APP_CONFIG
),
latest_daily AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        TS AS LATEST_DAILY_TS,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME
    FROM MIP.MART.MARKET_BARS
    WHERE INTERVAL_MINUTES = 1440
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SYMBOL, MARKET_TYPE
        ORDER BY TS DESC
    ) = 1
),
enabled_universe AS (
    SELECT
        UPPER(TRIM(SYMBOL)) AS SYMBOL,
        UPPER(TRIM(MARKET_TYPE)) AS MARKET_TYPE
    FROM MIP.APP.INGEST_UNIVERSE
    WHERE INTERVAL_MINUTES = 1440
      AND COALESCE(IS_ENABLED, TRUE)
    GROUP BY 1, 2
),
symbol_universe AS (
    SELECT SYMBOL, MARKET_TYPE FROM latest_daily
    UNION
    SELECT SYMBOL, MARKET_TYPE FROM enabled_universe
    UNION
    SELECT SYMBOL, MARKET_TYPE
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS
    WHERE SETUP_DATE >= DATEADD('day', -30, CURRENT_DATE())
),
recent_bars AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        ARRAY_AGG(
            OBJECT_CONSTRUCT_KEEP_NULL(
                'bar_date', TS::DATE,
                'open', OPEN,
                'high', HIGH,
                'low', LOW,
                'close', CLOSE,
                'volume', VOLUME
            )
        ) WITHIN GROUP (ORDER BY TS DESC) AS RECENT_DAILY_BARS_JSON,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'bars', COUNT(*),
            'latest_close', MAX_BY(CLOSE, TS),
            'close_5_bars_ago', MIN_BY(CLOSE, TS),
            'high_10', MAX(HIGH),
            'low_10', MIN(LOW),
            'return_window_pct',
                100 * (MAX_BY(CLOSE, TS) - MIN_BY(CLOSE, TS)) / NULLIF(MIN_BY(CLOSE, TS), 0),
            'range_window_pct',
                100 * (MAX(HIGH) - MIN(LOW)) / NULLIF(MAX_BY(CLOSE, TS), 0)
        ) AS RECENT_PRICE_ACTION_SUMMARY_JSON,
        ARRAY_AGG(
            OBJECT_CONSTRUCT_KEEP_NULL(
                'bar_date', TS::DATE,
                'open', OPEN,
                'high', HIGH,
                'low', LOW,
                'close', CLOSE,
                'body_direction', IFF(CLOSE >= OPEN, 'UP', 'DOWN'),
                'body_ratio',
                    IFF(HIGH = LOW, NULL, ABS(CLOSE - OPEN) / NULLIF(HIGH - LOW, 0)),
                'close_location',
                    IFF(HIGH = LOW, NULL, (CLOSE - LOW) / NULLIF(HIGH - LOW, 0)),
                'upper_wick_ratio',
                    IFF(HIGH = LOW, NULL, (HIGH - GREATEST(OPEN, CLOSE)) / NULLIF(HIGH - LOW, 0)),
                'lower_wick_ratio',
                    IFF(HIGH = LOW, NULL, (LEAST(OPEN, CLOSE) - LOW) / NULLIF(HIGH - LOW, 0))
            )
        ) WITHIN GROUP (ORDER BY TS DESC) AS CANDLE_SEQUENCE_JSON
    FROM (
        SELECT *
        FROM MIP.MART.MARKET_BARS
        WHERE INTERVAL_MINUTES = 1440
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY SYMBOL, MARKET_TYPE
            ORDER BY TS DESC
        ) <= 20
    )
    GROUP BY SYMBOL, MARKET_TYPE
),
latest_state AS (
    SELECT *
    FROM MIP.APP.STRUCTURAL_STATE_LOG
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SYMBOL, MARKET_TYPE
        ORDER BY AS_OF_DATE DESC
    ) = 1
),
latest_regime AS (
    SELECT *
    FROM MIP.APP.STRUCTURAL_REGIME_TAG
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SYMBOL, MARKET_TYPE
        ORDER BY AS_OF_DATE DESC
    ) = 1
),
level_base AS (
    SELECT
        l.*,
        ld.CLOSE AS CURRENT_PRICE
    FROM MIP.APP.STRUCTURAL_LEVEL_CACHE l
    JOIN latest_daily ld
      ON ld.SYMBOL = l.SYMBOL
     AND ld.MARKET_TYPE = l.MARKET_TYPE
    WHERE l.AS_OF_DATE = (
        SELECT MAX(l2.AS_OF_DATE)
        FROM MIP.APP.STRUCTURAL_LEVEL_CACHE l2
        WHERE l2.SYMBOL = l.SYMBOL
          AND l2.MARKET_TYPE = l.MARKET_TYPE
    )
),
nearest_support AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'level_id', LEVEL_ID,
            'level_type', LEVEL_TYPE,
            'level_price', LEVEL_PRICE,
            'level_low', LEVEL_LOW,
            'level_high', LEVEL_HIGH,
            'touch_count', TOUCH_COUNT,
            'level_significance', LEVEL_SIGNIFICANCE,
            'first_touch_date', FIRST_TOUCH_DATE,
            'last_touch_date', LAST_TOUCH_DATE,
            'distance_pct', 100 * (CURRENT_PRICE - LEVEL_PRICE) / NULLIF(CURRENT_PRICE, 0)
        ) AS NEAREST_SUPPORT_JSON,
        100 * (CURRENT_PRICE - LEVEL_PRICE) / NULLIF(CURRENT_PRICE, 0) AS DISTANCE_TO_SUPPORT_PCT
    FROM level_base
    WHERE LEVEL_TYPE IN ('SUPPORT_ZONE', 'SWING_LOW')
      AND LEVEL_PRICE <= CURRENT_PRICE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SYMBOL, MARKET_TYPE
        ORDER BY CURRENT_PRICE - LEVEL_PRICE
    ) = 1
),
nearest_resistance AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'level_id', LEVEL_ID,
            'level_type', LEVEL_TYPE,
            'level_price', LEVEL_PRICE,
            'level_low', LEVEL_LOW,
            'level_high', LEVEL_HIGH,
            'touch_count', TOUCH_COUNT,
            'level_significance', LEVEL_SIGNIFICANCE,
            'first_touch_date', FIRST_TOUCH_DATE,
            'last_touch_date', LAST_TOUCH_DATE,
            'distance_pct', 100 * (LEVEL_PRICE - CURRENT_PRICE) / NULLIF(CURRENT_PRICE, 0)
        ) AS NEAREST_RESISTANCE_JSON,
        100 * (LEVEL_PRICE - CURRENT_PRICE) / NULLIF(CURRENT_PRICE, 0) AS DISTANCE_TO_RESISTANCE_PCT
    FROM level_base
    WHERE LEVEL_TYPE IN ('RESISTANCE_ZONE', 'SWING_HIGH')
      AND LEVEL_PRICE >= CURRENT_PRICE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SYMBOL, MARKET_TYPE
        ORDER BY LEVEL_PRICE - CURRENT_PRICE
    ) = 1
),
broken_resistance AS (
    -- Phase 4 evidence-hardening v1: widened from 3% to 5% with explicit
    -- role label and a confidence value that decays linearly with distance.
    -- Confidence: 1.0 at <=1% distance, 0.3 at 5% distance.
    SELECT
        SYMBOL,
        MARKET_TYPE,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'level_id', LEVEL_ID,
            'level_type', LEVEL_TYPE,
            'level_price', LEVEL_PRICE,
            'level_low', LEVEL_LOW,
            'level_high', LEVEL_HIGH,
            'touch_count', TOUCH_COUNT,
            'level_significance', LEVEL_SIGNIFICANCE,
            'role', 'BROKEN_RESISTANCE_NOW_SUPPORT',
            'distance_below_current_pct',
                100 * (CURRENT_PRICE - LEVEL_PRICE) / NULLIF(CURRENT_PRICE, 0),
            'confidence',
                GREATEST(0.3,
                    LEAST(1.0,
                        1.0 - 0.175 *
                        ((100 * (CURRENT_PRICE - LEVEL_PRICE)
                          / NULLIF(CURRENT_PRICE, 0)) - 1.0)
                    )
                )
        ) AS BROKEN_RESISTANCE_AS_SUPPORT_JSON
    FROM level_base
    WHERE LEVEL_TYPE IN ('RESISTANCE_ZONE', 'SWING_HIGH')
      AND LEVEL_PRICE < CURRENT_PRICE
      AND 100 * (CURRENT_PRICE - LEVEL_PRICE) / NULLIF(CURRENT_PRICE, 0) <= 5.0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SYMBOL, MARKET_TYPE
        ORDER BY CURRENT_PRICE - LEVEL_PRICE
    ) = 1
),
-- ----------------------------------------------------------------
-- Phase 4 evidence-hardening v1: structural_timeline_90d
-- Anchored to the latest ingested daily bar per symbol (not wall-clock
-- date) so the 90D window is stable across late/missing ingests.
-- Emits two pieces:
--   - STRUCTURAL_TIMELINE_SUMMARY_JSON  (compact stats; default agent slice)
--   - STRUCTURAL_TIMELINE_BARS_JSON     (90 daily OHLC bars; Chair/audit only)
-- ----------------------------------------------------------------
structural_timeline_anchor AS (
    SELECT SYMBOL, MARKET_TYPE, MAX(TS) AS ANCHOR_TS
    FROM MIP.MART.MARKET_BARS
    WHERE INTERVAL_MINUTES = 1440
    GROUP BY SYMBOL, MARKET_TYPE
),
structural_timeline_90d AS (
    SELECT
        b.SYMBOL,
        b.MARKET_TYPE,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'lookback_days', 90,
            'anchor_date', MAX(b.TS::DATE),
            'start_date', MIN(b.TS::DATE),
            'bar_count', COUNT(*),
            'start_price', MIN_BY(b.CLOSE, b.TS),
            'end_price', MAX_BY(b.CLOSE, b.TS),
            'range_low', MIN(b.LOW),
            'range_high', MAX(b.HIGH),
            'current_range_position_pct',
                ROUND(100.0 * (MAX_BY(b.CLOSE, b.TS) - MIN(b.LOW))
                      / NULLIF(MAX(b.HIGH) - MIN(b.LOW), 0), 2),
            'distance_from_range_high_pct',
                ROUND(100.0 * (MAX(b.HIGH) - MAX_BY(b.CLOSE, b.TS))
                      / NULLIF(MAX_BY(b.CLOSE, b.TS), 0), 2),
            'distance_from_range_low_pct',
                ROUND(100.0 * (MAX_BY(b.CLOSE, b.TS) - MIN(b.LOW))
                      / NULLIF(MAX_BY(b.CLOSE, b.TS), 0), 2),
            'window_return_pct',
                ROUND(100.0 * (MAX_BY(b.CLOSE, b.TS) - MIN_BY(b.CLOSE, b.TS))
                      / NULLIF(MIN_BY(b.CLOSE, b.TS), 0), 2),
            'anchor_note',
                'Anchored to latest ingested daily bar per symbol, not wall-clock date.'
        ) AS STRUCTURAL_TIMELINE_SUMMARY_JSON,
        ARRAY_AGG(
            OBJECT_CONSTRUCT_KEEP_NULL(
                'bar_date', b.TS::DATE,
                'open', b.OPEN,
                'high', b.HIGH,
                'low', b.LOW,
                'close', b.CLOSE,
                'volume', b.VOLUME
            )
        ) WITHIN GROUP (ORDER BY b.TS) AS STRUCTURAL_TIMELINE_BARS_JSON
    FROM MIP.MART.MARKET_BARS b
    JOIN structural_timeline_anchor a
      ON a.SYMBOL = b.SYMBOL AND a.MARKET_TYPE = b.MARKET_TYPE
    WHERE b.INTERVAL_MINUTES = 1440
      AND b.TS >= DATEADD('day', -90, a.ANCHOR_TS)
    GROUP BY b.SYMBOL, b.MARKET_TYPE
),
-- ----------------------------------------------------------------
-- Phase 4 evidence-hardening v1: candle_psychology
-- Per-bar multi-label psychology over the 20 most-recent daily bars,
-- plus a most-recent-5-bar cluster classification. Labels are derived
-- deterministically from body_direction, body_ratio, close_location,
-- and wick ratios; bars can carry multiple labels.
-- ----------------------------------------------------------------
candle_psychology_bars AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        b.value:bar_date::DATE AS BAR_DATE,
        ROW_NUMBER() OVER (
            PARTITION BY SYMBOL, MARKET_TYPE
            ORDER BY b.value:bar_date::DATE DESC
        ) AS BAR_RANK_DESC,
        b.value:body_direction::STRING AS BODY_DIRECTION,
        b.value:body_ratio::FLOAT AS BODY_RATIO,
        b.value:close_location::FLOAT AS CLOSE_LOCATION,
        b.value:upper_wick_ratio::FLOAT AS UPPER_WICK_RATIO,
        b.value:lower_wick_ratio::FLOAT AS LOWER_WICK_RATIO
    FROM recent_bars rb,
         LATERAL FLATTEN(input => rb.CANDLE_SEQUENCE_JSON) b
),
candle_psychology_labeled AS (
    SELECT
        SYMBOL, MARKET_TYPE, BAR_DATE, BAR_RANK_DESC, BODY_DIRECTION,
        BODY_RATIO, CLOSE_LOCATION, UPPER_WICK_RATIO, LOWER_WICK_RATIO,
        ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(BODY_DIRECTION = 'UP'   AND BODY_RATIO >= 0.55 AND CLOSE_LOCATION >= 0.75, 'BULLISH_PUSH', NULL),
            IFF(BODY_DIRECTION = 'DOWN' AND BODY_RATIO >= 0.55 AND CLOSE_LOCATION <= 0.25, 'BEARISH_PUSH', NULL),
            IFF(UPPER_WICK_RATIO >= 0.35, 'UPPER_WICK_REJECTION', NULL),
            IFF(LOWER_WICK_RATIO >= 0.35, 'LOWER_WICK_SUPPORT', NULL),
            IFF(BODY_RATIO < 0.30 AND UPPER_WICK_RATIO >= 0.25 AND LOWER_WICK_RATIO >= 0.25, 'INDECISION_BATTLE', NULL),
            IFF(CLOSE_LOCATION >= 0.75, 'STRONG_CLOSE_NEAR_HIGH', NULL),
            IFF(CLOSE_LOCATION <= 0.25, 'WEAK_CLOSE_NEAR_LOW', NULL),
            IFF(BODY_DIRECTION = 'UP'   AND CLOSE_LOCATION < 0.40, 'FAILED_UP_DAY', NULL),
            IFF(BODY_DIRECTION = 'DOWN' AND CLOSE_LOCATION >= 0.50, 'PULLBACK_DIGESTION', NULL)
        )) AS LABELS
    FROM candle_psychology_bars
),
candle_psychology_with_primary AS (
    SELECT
        SYMBOL, MARKET_TYPE, BAR_DATE, BAR_RANK_DESC, BODY_DIRECTION,
        BODY_RATIO, CLOSE_LOCATION, UPPER_WICK_RATIO, LOWER_WICK_RATIO, LABELS,
        CASE
            WHEN ARRAY_CONTAINS('BULLISH_PUSH'::VARIANT, LABELS) THEN 'BULLISH_PUSH'
            WHEN ARRAY_CONTAINS('BEARISH_PUSH'::VARIANT, LABELS) THEN 'BEARISH_PUSH'
            WHEN ARRAY_CONTAINS('UPPER_WICK_REJECTION'::VARIANT, LABELS) THEN 'UPPER_WICK_REJECTION'
            WHEN ARRAY_CONTAINS('LOWER_WICK_SUPPORT'::VARIANT, LABELS) THEN 'LOWER_WICK_SUPPORT'
            WHEN ARRAY_CONTAINS('INDECISION_BATTLE'::VARIANT, LABELS) THEN 'INDECISION_BATTLE'
            WHEN ARRAY_CONTAINS('STRONG_CLOSE_NEAR_HIGH'::VARIANT, LABELS) THEN 'STRONG_CLOSE_NEAR_HIGH'
            WHEN ARRAY_CONTAINS('WEAK_CLOSE_NEAR_LOW'::VARIANT, LABELS) THEN 'WEAK_CLOSE_NEAR_LOW'
            WHEN ARRAY_CONTAINS('FAILED_UP_DAY'::VARIANT, LABELS) THEN 'FAILED_UP_DAY'
            WHEN ARRAY_CONTAINS('PULLBACK_DIGESTION'::VARIANT, LABELS) THEN 'PULLBACK_DIGESTION'
            ELSE 'NEUTRAL'
        END AS PRIMARY_LABEL
    FROM candle_psychology_labeled
),
candle_psychology_clusters AS (
    -- Aggregate metrics over the most-recent-5 bars to derive a cluster label.
    SELECT
        SYMBOL,
        MARKET_TYPE,
        SUM(IFF(ARRAY_CONTAINS('BEARISH_PUSH'::VARIANT, LABELS) OR ARRAY_CONTAINS('UPPER_WICK_REJECTION'::VARIANT, LABELS), 1, 0)) AS REJECTION_HITS,
        SUM(IFF(ARRAY_CONTAINS('BULLISH_PUSH'::VARIANT, LABELS), 1, 0)) AS BULLISH_PUSH_HITS,
        SUM(IFF(ARRAY_CONTAINS('LOWER_WICK_SUPPORT'::VARIANT, LABELS), 1, 0)) AS LOWER_WICK_HITS,
        SUM(IFF(BODY_DIRECTION = 'DOWN', 1, 0)) AS DOWN_BODY_COUNT,
        SUM(IFF(BODY_DIRECTION = 'UP',   1, 0)) AS UP_BODY_COUNT,
        SUM(IFF(ARRAY_CONTAINS('PULLBACK_DIGESTION'::VARIANT, LABELS) OR ARRAY_CONTAINS('STRONG_CLOSE_NEAR_HIGH'::VARIANT, LABELS), 1, 0)) AS DIGESTION_HITS,
        AVG(CLOSE_LOCATION) AS AVG_CLOSE_LOCATION,
        COUNT(*) AS BARS_ASSESSED
    FROM candle_psychology_with_primary
    WHERE BAR_RANK_DESC <= 5
    GROUP BY SYMBOL, MARKET_TYPE
),
candle_psychology_streak_grp AS (
    -- Build a "streak group" for consecutive DOWN bodies.
    -- This first level computes the row numbers separately so the
    -- subtraction can be done without nested window functions.
    SELECT
        SYMBOL, MARKET_TYPE, BAR_RANK_DESC, BODY_DIRECTION,
        ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY BAR_RANK_DESC) AS RN_ALL,
        ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE, BODY_DIRECTION ORDER BY BAR_RANK_DESC) AS RN_DIR
    FROM candle_psychology_with_primary
    WHERE BAR_RANK_DESC <= 5
),
candle_psychology_consec_down AS (
    -- Detect "3+ consecutive DOWN bodies in the last 5 bars" deterministically.
    -- The DOWN-only filter happens AFTER the row numbers are computed, so the
    -- (RN_ALL - RN_DIR) trick correctly groups runs of DOWN bodies together
    -- (since RN_DIR is monotonic only within the DOWN partition).
    SELECT
        SYMBOL,
        MARKET_TYPE,
        MAX(STREAK_LEN) AS MAX_DOWN_STREAK
    FROM (
        SELECT
            SYMBOL, MARKET_TYPE,
            (RN_ALL - RN_DIR) AS GRP,
            COUNT(*) OVER (PARTITION BY SYMBOL, MARKET_TYPE, (RN_ALL - RN_DIR)) AS STREAK_LEN
        FROM candle_psychology_streak_grp
        WHERE BODY_DIRECTION = 'DOWN'
    )
    GROUP BY SYMBOL, MARKET_TYPE
),
candle_psychology AS (
    SELECT
        c.SYMBOL,
        c.MARKET_TYPE,
        -- Cluster label exposed as a column for actionability_context.
        CASE
            WHEN c.REJECTION_HITS >= 3
                 THEN 'UPPER_ZONE_REJECTION_CLUSTER'
            WHEN COALESCE(d.MAX_DOWN_STREAK, 0) >= 3
                 THEN 'SELLER_PRESSURE_AFTER_ADVANCE'
            WHEN c.DOWN_BODY_COUNT >= 2
                 AND c.DIGESTION_HITS >= 2
                 AND c.REJECTION_HITS = 0
                 THEN 'ORDERLY_PULLBACK'
            WHEN c.LOWER_WICK_HITS >= 3
                 THEN 'LOWER_WICK_ACCUMULATION'
            WHEN c.BULLISH_PUSH_HITS >= 2 AND c.REJECTION_HITS = 0
                 THEN 'BREAKOUT_FOLLOW_THROUGH'
            WHEN ABS(c.UP_BODY_COUNT - c.DOWN_BODY_COUNT) <= 1
                 AND c.AVG_CLOSE_LOCATION BETWEEN 0.35 AND 0.65
                 THEN 'NOISY_CHOP'
            ELSE 'NO_CLUSTER'
        END AS RECENT_CLUSTER,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'bar_labels', (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
                    'bar_date', BAR_DATE,
                    'body_direction', BODY_DIRECTION,
                    'body_ratio', BODY_RATIO,
                    'close_location', CLOSE_LOCATION,
                    'upper_wick_ratio', UPPER_WICK_RATIO,
                    'lower_wick_ratio', LOWER_WICK_RATIO,
                    'primary_label', PRIMARY_LABEL,
                    'labels', LABELS
                )) WITHIN GROUP (ORDER BY BAR_DATE DESC)
                FROM candle_psychology_with_primary p
                WHERE p.SYMBOL = c.SYMBOL AND p.MARKET_TYPE = c.MARKET_TYPE
            ),
            'recent_cluster',
                CASE
                    WHEN c.REJECTION_HITS >= 3 THEN 'UPPER_ZONE_REJECTION_CLUSTER'
                    WHEN COALESCE(d.MAX_DOWN_STREAK, 0) >= 3 THEN 'SELLER_PRESSURE_AFTER_ADVANCE'
                    WHEN c.DOWN_BODY_COUNT >= 2 AND c.DIGESTION_HITS >= 2 AND c.REJECTION_HITS = 0 THEN 'ORDERLY_PULLBACK'
                    WHEN c.LOWER_WICK_HITS >= 3 THEN 'LOWER_WICK_ACCUMULATION'
                    WHEN c.BULLISH_PUSH_HITS >= 2 AND c.REJECTION_HITS = 0 THEN 'BREAKOUT_FOLLOW_THROUGH'
                    WHEN ABS(c.UP_BODY_COUNT - c.DOWN_BODY_COUNT) <= 1
                         AND c.AVG_CLOSE_LOCATION BETWEEN 0.35 AND 0.65 THEN 'NOISY_CHOP'
                    ELSE 'NO_CLUSTER'
                END,
            'cluster_confidence',
                LEAST(1.0, GREATEST(c.REJECTION_HITS, COALESCE(d.MAX_DOWN_STREAK, 0),
                    c.BULLISH_PUSH_HITS, c.DIGESTION_HITS, c.LOWER_WICK_HITS) / 5.0),
            'cluster_bars_assessed', c.BARS_ASSESSED,
            'bars_assessed_total', 20
        ) AS CANDLE_PSYCHOLOGY_JSON
    FROM candle_psychology_clusters c
    LEFT JOIN candle_psychology_consec_down d
      ON d.SYMBOL = c.SYMBOL AND d.MARKET_TYPE = c.MARKET_TYPE
),
-- ----------------------------------------------------------------
-- Phase 4 evidence-hardening v1: actionability_context
-- Synthesized read of resistance/support proximity and recent cluster
-- behavior. Refined: CONFIRMED requires no HIGH overhead AND a
-- BREAKOUT_FOLLOW_THROUGH or ORDERLY_PULLBACK cluster. A bullish bar
-- or TREND_UP state alone is NOT sufficient.
-- ----------------------------------------------------------------
actionability_context AS (
    SELECT
        u.SYMBOL,
        u.MARKET_TYPE,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'resistance_overhead_risk',
                CASE
                    WHEN nr.DISTANCE_TO_RESISTANCE_PCT IS NULL THEN 'UNKNOWN'
                    WHEN nr.DISTANCE_TO_RESISTANCE_PCT < 3.0 THEN 'HIGH'
                    WHEN nr.DISTANCE_TO_RESISTANCE_PCT < 6.0 THEN 'MODERATE'
                    WHEN nr.DISTANCE_TO_RESISTANCE_PCT < 12.0 THEN 'LOW'
                    ELSE 'CLEAR'
                END,
            'overhead_resistance_distance_pct', nr.DISTANCE_TO_RESISTANCE_PCT,
            'support_protection_quality',
                CASE
                    WHEN br.BROKEN_RESISTANCE_AS_SUPPORT_JSON:confidence::FLOAT >= 0.7 THEN 'STRONG'
                    WHEN ns.DISTANCE_TO_SUPPORT_PCT IS NOT NULL
                         AND ns.DISTANCE_TO_SUPPORT_PCT <= 3.0 THEN 'MODERATE'
                    WHEN ns.DISTANCE_TO_SUPPORT_PCT IS NOT NULL THEN 'WEAK'
                    ELSE 'ABSENT'
                END,
            'nearest_support_distance_pct', ns.DISTANCE_TO_SUPPORT_PCT,
            'broken_resistance_support_confidence',
                br.BROKEN_RESISTANCE_AS_SUPPORT_JSON:confidence::FLOAT,
            'continuation_quality',
                CASE
                    -- Active rejection cluster always degrades continuation.
                    WHEN cp.RECENT_CLUSTER IN ('UPPER_ZONE_REJECTION_CLUSTER','SELLER_PRESSURE_AFTER_ADVANCE')
                         AND st.STRUCTURAL_STATE IN ('TREND_UP','PULLBACK_IN_TREND')
                        THEN 'CONTESTED'
                    WHEN cp.RECENT_CLUSTER IN ('UPPER_ZONE_REJECTION_CLUSTER','SELLER_PRESSURE_AFTER_ADVANCE')
                        THEN 'REJECTED'
                    -- Confirmation requires follow-through or orderly pullback AND no HIGH overhead.
                    WHEN cp.RECENT_CLUSTER = 'BREAKOUT_FOLLOW_THROUGH'
                         AND COALESCE(nr.DISTANCE_TO_RESISTANCE_PCT, 99) >= 3.0
                        THEN 'CONFIRMED'
                    WHEN cp.RECENT_CLUSTER = 'ORDERLY_PULLBACK'
                         AND COALESCE(nr.DISTANCE_TO_RESISTANCE_PCT, 99) >= 3.0
                         AND st.STRUCTURAL_STATE IN ('TREND_UP','PULLBACK_IN_TREND')
                        THEN 'CONFIRMED'
                    ELSE 'UNCONFIRMED'
                END,
            'active_candle_cluster', cp.RECENT_CLUSTER,
            'entry_location_quality',
                CASE
                    WHEN nr.DISTANCE_TO_RESISTANCE_PCT IS NULL THEN 'UNKNOWN'
                    WHEN nr.DISTANCE_TO_RESISTANCE_PCT < 2.0 THEN 'AT_RESISTANCE'
                    WHEN COALESCE(br.BROKEN_RESISTANCE_AS_SUPPORT_JSON:distance_below_current_pct::FLOAT, 99) <= 2.0
                        THEN 'AT_BROKEN_RESISTANCE_SUPPORT'
                    WHEN COALESCE(ns.DISTANCE_TO_SUPPORT_PCT, 99) <= 2.0
                        THEN 'AT_SUPPORT'
                    WHEN nr.DISTANCE_TO_RESISTANCE_PCT < 6.0 THEN 'BELOW_RESISTANCE_OVERHEAD'
                    ELSE 'MID_RANGE'
                END,
            'target_path_clear',
                COALESCE(nr.DISTANCE_TO_RESISTANCE_PCT, 99) >= 6.0,
            'confirmation_needed',
                (nr.DISTANCE_TO_RESISTANCE_PCT IS NOT NULL AND nr.DISTANCE_TO_RESISTANCE_PCT < 3.0)
                OR cp.RECENT_CLUSTER IN ('UPPER_ZONE_REJECTION_CLUSTER','SELLER_PRESSURE_AFTER_ADVANCE'),
            'invalidation_reference_available',
                (br.BROKEN_RESISTANCE_AS_SUPPORT_JSON IS NOT NULL
                 OR ns.NEAREST_SUPPORT_JSON IS NOT NULL)
        ) AS ACTIONABILITY_CONTEXT_JSON
    FROM symbol_universe u
    LEFT JOIN nearest_resistance nr
      ON nr.SYMBOL = u.SYMBOL AND nr.MARKET_TYPE = u.MARKET_TYPE
    LEFT JOIN nearest_support ns
      ON ns.SYMBOL = u.SYMBOL AND ns.MARKET_TYPE = u.MARKET_TYPE
    LEFT JOIN broken_resistance br
      ON br.SYMBOL = u.SYMBOL AND br.MARKET_TYPE = u.MARKET_TYPE
    LEFT JOIN latest_state st
      ON st.SYMBOL = u.SYMBOL AND st.MARKET_TYPE = u.MARKET_TYPE
    LEFT JOIN candle_psychology cp
      ON cp.SYMBOL = u.SYMBOL AND cp.MARKET_TYPE = u.MARKET_TYPE
),
-- Phase 8 coherence enrichment: compute per-setup level<->entry coherence
-- metrics inline so the dossier can (a) expose them to the agent panel and
-- (b) prefer coherent setups when picking primary evidence.
-- Level-anchored families: LEVEL_PRICE must be near entry midpoint. Other
-- families (e.g. pattern-only) always coherent.
setup_events_enriched AS (
    SELECT
        s.*,
        (s.ENTRY_ZONE_LOW + s.ENTRY_ZONE_HIGH) / 2.0 AS ENTRY_MID,
        CASE
            WHEN s.LEVEL_PRICE IS NULL
              OR s.ENTRY_ZONE_LOW IS NULL
              OR s.ENTRY_ZONE_HIGH IS NULL
              OR (s.ENTRY_ZONE_LOW + s.ENTRY_ZONE_HIGH) <= 0
            THEN NULL
            ELSE ABS(s.LEVEL_PRICE - (s.ENTRY_ZONE_LOW + s.ENTRY_ZONE_HIGH) / 2.0)
                 / NULLIF((s.ENTRY_ZONE_LOW + s.ENTRY_ZONE_HIGH) / 2.0, 0) * 100.0
        END AS LEVEL_TO_ENTRY_MID_PCT,
        CASE
            WHEN s.LEVEL_PRICE IS NULL
              OR s.ENTRY_ZONE_LOW IS NULL
              OR s.ENTRY_ZONE_HIGH IS NULL
              OR s.VOLATILITY_CONTEXT IS NULL
              OR s.VOLATILITY_CONTEXT <= 0
            THEN NULL
            ELSE ABS(s.LEVEL_PRICE - (s.ENTRY_ZONE_LOW + s.ENTRY_ZONE_HIGH) / 2.0)
                 / s.VOLATILITY_CONTEXT
        END AS LEVEL_TO_ENTRY_MID_ATR,
        CASE
            WHEN s.SETUP_FAMILY NOT IN (
                'BREAKOUT_RETEST_LONG','SUPPORT_WICK_LONG','TREND_PULLBACK_LONG',
                'THREE_BAR_REVERSAL_LONG','BREAKDOWN_RETEST_SHORT',
                'RESISTANCE_WICK_SHORT','THREE_BAR_REVERSAL_SHORT',
                'FAILED_BREAKOUT_SHORT'
            ) THEN TRUE
            WHEN s.LEVEL_PRICE IS NULL OR s.ENTRY_ZONE_LOW IS NULL
              OR s.ENTRY_ZONE_HIGH IS NULL
              OR (s.ENTRY_ZONE_LOW + s.ENTRY_ZONE_HIGH) <= 0
            THEN NULL
            ELSE (
                ABS(s.LEVEL_PRICE - (s.ENTRY_ZONE_LOW + s.ENTRY_ZONE_HIGH) / 2.0)
                / NULLIF((s.ENTRY_ZONE_LOW + s.ENTRY_ZONE_HIGH) / 2.0, 0) * 100.0
                <= 5.0
                OR (
                    s.VOLATILITY_CONTEXT IS NOT NULL AND s.VOLATILITY_CONTEXT > 0
                    AND ABS(s.LEVEL_PRICE - (s.ENTRY_ZONE_LOW + s.ENTRY_ZONE_HIGH) / 2.0)
                        / s.VOLATILITY_CONTEXT
                        <= 3.0
                )
            )
        END AS LEVEL_ENTRY_COHERENT
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS s
    WHERE s.SETUP_DATE >= DATEADD('day', -30, CURRENT_DATE())
),
setup_events AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        ARRAY_AGG(
            OBJECT_CONSTRUCT_KEEP_NULL(
                'setup_event_id', SETUP_EVENT_ID,
                'setup_date', SETUP_DATE,
                'setup_family', SETUP_FAMILY,
                'event_direction', DIRECTION,
                'setup_status', SETUP_STATUS,
                'structural_state', STRUCTURAL_STATE,
                'prior_state', PRIOR_STATE,
                'level_type', LEVEL_TYPE,
                'level_price', LEVEL_PRICE,
                'level_significance', LEVEL_SIGNIFICANCE,
                'entry_zone_low_evidence', ENTRY_ZONE_LOW,
                'entry_zone_high_evidence', ENTRY_ZONE_HIGH,
                'price_invalidation_level_evidence', PRICE_INVALIDATION_LEVEL,
                'structure_confidence', STRUCTURE_CONFIDENCE,
                'wick_confirmation_score', WICK_CONFIRMATION_SCORE,
                'three_bar_confirmation_score', THREE_BAR_CONFIRMATION_SCORE,
                'trend_context_score', TREND_CONTEXT_SCORE,
                'regime_compat', REGIME_COMPAT,
                'risk_class_evidence', RISK_CLASS,
                'features_json', FEATURES_JSON,
                'level_to_entry_mid_pct', ROUND(LEVEL_TO_ENTRY_MID_PCT, 2),
                'level_to_entry_mid_atr', ROUND(LEVEL_TO_ENTRY_MID_ATR, 2),
                'level_entry_coherent', LEVEL_ENTRY_COHERENT,
                'coherence_warning',
                    CASE
                        WHEN LEVEL_ENTRY_COHERENT = FALSE
                        THEN 'STALE_STRUCTURAL_ANCHOR: cited '
                             || COALESCE(LEVEL_TYPE, 'level')
                             || ' at $' || ROUND(LEVEL_PRICE, 2)
                             || ' is '
                             || ROUND(LEVEL_TO_ENTRY_MID_PCT, 1)
                             || '% from entry midpoint $' || ROUND(ENTRY_MID, 2)
                        ELSE NULL
                    END
            )
        ) WITHIN GROUP (ORDER BY SETUP_DATE DESC, SETUP_EVENT_ID DESC) AS SETUP_EVENTS_JSON,
        ARRAY_AGG(
            IFF(DIRECTION = 'LONG',
                OBJECT_CONSTRUCT_KEEP_NULL(
                    'setup_event_id', SETUP_EVENT_ID,
                    'setup_date', SETUP_DATE,
                    'setup_family', SETUP_FAMILY,
                    'setup_status', SETUP_STATUS,
                    'structure_confidence', STRUCTURE_CONFIDENCE,
                    'regime_compat', REGIME_COMPAT,
                    'level_entry_coherent', LEVEL_ENTRY_COHERENT
                ),
                NULL)
        ) WITHIN GROUP (ORDER BY SETUP_DATE DESC, SETUP_EVENT_ID DESC) AS LONG_PATTERN_SIGNS_RAW,
        ARRAY_AGG(
            IFF(DIRECTION = 'SHORT',
                OBJECT_CONSTRUCT_KEEP_NULL(
                    'setup_event_id', SETUP_EVENT_ID,
                    'setup_date', SETUP_DATE,
                    'setup_family', SETUP_FAMILY,
                    'setup_status', SETUP_STATUS,
                    'structure_confidence', STRUCTURE_CONFIDENCE,
                    'regime_compat', REGIME_COMPAT,
                    'level_entry_coherent', LEVEL_ENTRY_COHERENT
                ),
                NULL)
        ) WITHIN GROUP (ORDER BY SETUP_DATE DESC, SETUP_EVENT_ID DESC) AS SHORT_PATTERN_SIGNS_RAW,
        ARRAY_AGG(
            IFF(SETUP_STATUS IN ('INVALIDATED', 'EXPIRED'),
                OBJECT_CONSTRUCT_KEEP_NULL(
                    'setup_event_id', SETUP_EVENT_ID,
                    'setup_date', SETUP_DATE,
                    'setup_family', SETUP_FAMILY,
                    'event_direction', DIRECTION,
                    'setup_status', SETUP_STATUS,
                    'level_price', LEVEL_PRICE,
                    'price_invalidation_level', PRICE_INVALIDATION_LEVEL
                ),
                NULL)
        ) WITHIN GROUP (ORDER BY SETUP_DATE DESC, SETUP_EVENT_ID DESC) AS RECENT_INVALIDATED_SETUP_EVENTS_RAW,
        -- Phase 8: PRIMARY_EVIDENCE_SETUP_EVENT_ID prefers COHERENT active
        -- setups. If none coherent, falls back to max SETUP_EVENT_ID among
        -- active statuses (previous behaviour) so the agent still sees a
        -- primary pointer for reasoning, but the dossier board warning
        -- BOTH_NO_COHERENT_PRIMARY_EVIDENCE tells the chair to treat it as
        -- suspect.
        MAX(IFF(
                SETUP_STATUS IN ('DETECTED', 'ELIGIBLE', 'WAITING', 'STALE')
                AND COALESCE(LEVEL_ENTRY_COHERENT, TRUE) = TRUE,
                SETUP_EVENT_ID, NULL
        )) AS PRIMARY_EVIDENCE_COHERENT_ID,
        MAX(IFF(SETUP_STATUS IN ('DETECTED', 'ELIGIBLE', 'WAITING', 'STALE'), SETUP_EVENT_ID, NULL))
            AS PRIMARY_EVIDENCE_ANY_ID,
        COALESCE(
            MAX(IFF(
                SETUP_STATUS IN ('DETECTED', 'ELIGIBLE', 'WAITING', 'STALE')
                AND COALESCE(LEVEL_ENTRY_COHERENT, TRUE) = TRUE,
                SETUP_EVENT_ID, NULL
            )),
            MAX(IFF(SETUP_STATUS IN ('DETECTED', 'ELIGIBLE', 'WAITING', 'STALE'), SETUP_EVENT_ID, NULL))
        ) AS PRIMARY_EVIDENCE_SETUP_EVENT_ID
    FROM setup_events_enriched
    GROUP BY SYMBOL, MARKET_TYPE
),
history_by_direction AS (
    SELECT
        IFF(SETUP_FAMILY ILIKE '%SHORT%', 'SHORT', 'LONG') AS EVIDENCE_DIRECTION,
        MARKET_TYPE,
        ARRAY_AGG(
            OBJECT_CONSTRUCT_KEEP_NULL(
                'setup_family', SETUP_FAMILY,
                'eval_window', EVAL_WINDOW,
                'n_setups', N_SETUPS,
                'meaningful_hit_rate', MEANINGFUL_HIT_RATE,
                'directional_hit_rate', DIRECTIONAL_HIT_RATE,
                'path_survival_hit_rate', PATH_SURVIVAL_HIT_RATE,
                'avg_mfe', AVG_MFE,
                'avg_mae', AVG_MAE,
                'mfe_mae_ratio', MFE_MAE_RATIO,
                'trust_label', TRUST_LABEL,
                'failure_mode_distribution', FAILURE_MODE_DISTRIBUTION,
                'best_window', BEST_WINDOW
            )
        ) WITHIN GROUP (ORDER BY SETUP_FAMILY, EVAL_WINDOW) AS HISTORY_ARRAY
    FROM MIP.APP.STRUCTURAL_SETUP_TRUST
    WHERE EVAL_WINDOW = 20
    GROUP BY 1, 2
),
path_by_direction AS (
    SELECT
        IFF(SETUP_FAMILY ILIKE '%SHORT%', 'SHORT', 'LONG') AS EVIDENCE_DIRECTION,
        MARKET_TYPE,
        ARRAY_AGG(
            OBJECT_CONSTRUCT_KEEP_NULL(
                'setup_family', SETUP_FAMILY,
                'eval_window', EVAL_WINDOW,
                'median_mfe', MEDIAN_MFE,
                'median_mae', MEDIAN_MAE,
                'pct_adverse_before_favorable', PCT_ADVERSE_BEFORE_FAVORABLE,
                'avg_bars_to_mfe', AVG_BARS_TO_MFE,
                'avg_bars_to_mae', AVG_BARS_TO_MAE,
                'gap_risk_contribution', GAP_RISK_CONTRIBUTION
            )
        ) WITHIN GROUP (ORDER BY SETUP_FAMILY, EVAL_WINDOW) AS PATH_ARRAY
    FROM MIP.APP.STRUCTURAL_PATH_STATS
    WHERE EVAL_WINDOW = 20
    GROUP BY 1, 2
),
recent_trade_memory AS (
    SELECT
        SYMBOL,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'recent_closeout_ts', MAX(EXIT_TS),
            'recent_trades_30d', COUNT_IF(EXIT_TS >= DATEADD('day', -30, CURRENT_TIMESTAMP())),
            'recent_failed_trade_30d', MAX(IFF(EXIT_TS >= DATEADD('day', -30, CURRENT_TIMESTAMP()) AND COALESCE(REALIZED_RETURN, 0) < 0, 1, 0)) = 1,
            'recent_outcome_class', MAX_BY(OUTCOME_CLASS, EXIT_TS),
            'recent_realized_return', MAX_BY(REALIZED_RETURN, EXIT_TS)
        ) AS RECENT_TRADE_MEMORY_JSON
    FROM MIP.MART.V_TRADE_INTELLIGENCE
    GROUP BY SYMBOL
),
-- Phase 4 taxonomy v2: most recent agentic proposal per symbol so the Chair
-- can detect contested-prior-thesis cases (WATCH_LONG_FAILURE / WATCH_SHORT_FAILURE)
-- without an extra slice call. Active statuses mirror live_action_context.
last_agentic_proposal_row AS (
    SELECT
        SYMBOL,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'proposal_id', PROPOSAL_ID,
            'created_at', CREATED_AT,
            'age_days', DATEDIFF('day', CREATED_AT, CURRENT_TIMESTAMP()),
            'direction', DIRECTION,
            'final_action', BOARD_FINAL_VERDICT,
            'status', STATUS,
            'setup_family', SETUP_FAMILY,
            'entry_zone_low', ENTRY_ZONE_LOW,
            'entry_zone_high', ENTRY_ZONE_HIGH,
            'invalidation_level', PRICE_INVALIDATION_LEVEL,
            'is_active', STATUS IN (
                'PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION',
                'OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED'
            )
        ) AS LAST_AGENTIC_PROPOSAL_JSON
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    WHERE SETUP_FAMILY ILIKE 'AGENTIC_%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY CREATED_AT DESC) = 1
),
recent_proposal_memory AS (
    SELECT
        agg.SYMBOL,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'proposals_14d', agg.PROPOSALS_14D,
            'last_proposed_at', agg.LAST_PROPOSED_AT,
            'active_proposals', agg.ACTIVE_PROPOSALS,
            'recent_agentic_proposals_14d', agg.RECENT_AGENTIC_PROPOSALS_14D,
            'last_agentic_proposal', lap.LAST_AGENTIC_PROPOSAL_JSON
        ) AS RECENT_PROPOSAL_MEMORY_JSON
    FROM (
        SELECT
            SYMBOL,
            COUNT_IF(CREATED_AT >= DATEADD('day', -14, CURRENT_TIMESTAMP())) AS PROPOSALS_14D,
            MAX(CREATED_AT) AS LAST_PROPOSED_AT,
            COUNT_IF(STATUS = 'PROPOSED') AS ACTIVE_PROPOSALS,
            COUNT_IF(CREATED_AT >= DATEADD('day', -14, CURRENT_TIMESTAMP())
                     AND SETUP_FAMILY ILIKE 'AGENTIC_%') AS RECENT_AGENTIC_PROPOSALS_14D
        FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
        GROUP BY SYMBOL
    ) agg
    LEFT JOIN last_agentic_proposal_row lap
      ON lap.SYMBOL = agg.SYMBOL
),
live_action_context AS (
    SELECT
        SYMBOL,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'open_structural_actions', COUNT_IF(STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION','OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')),
            'pre_broker_actions', COUNT_IF(STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION','OPEN_BLOCKED')),
            'execution_requested_actions', COUNT_IF(STATUS = 'EXECUTION_REQUESTED'),
            'latest_action_status', MAX_BY(STATUS, UPDATED_AT),
            'latest_action_updated_at', MAX(UPDATED_AT)
        ) AS LIVE_ACTION_CONTEXT_JSON
    FROM MIP.LIVE.LIVE_ACTIONS
    WHERE LIVE_INTENT_KIND = 'STRUCTURAL'
    GROUP BY SYMBOL
),
-- Live broker truth only. Do NOT use PORTFOLIO_POSITIONS here: that table
-- carries simulation/horizon book rows that are invisible in LPA and must
-- not drive proposal-board eligibility.
open_position_context AS (
    SELECT
        SYMBOL,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'open_position_count', COUNT(*),
            'total_quantity', SUM(QUANTITY),
            'portfolio_ids', ARRAY_UNIQUE_AGG(PORTFOLIO_ID)
        ) AS OPEN_POSITION_CONTEXT_JSON
    FROM MIP.MART.V_LIVE_OPEN_POSITIONS
    WHERE COALESCE(QUANTITY, 0) <> 0
    GROUP BY SYMBOL
)
SELECT
    u.SYMBOL,
    u.MARKET_TYPE,
    CURRENT_DATE() AS AS_OF_DATE,
    NULL::NUMBER AS PORTFOLIO_ID,
    u.SYMBOL || '|' || u.MARKET_TYPE || '|' || TO_VARCHAR(CURRENT_DATE()) || '|ALL' AS DOSSIER_KEY,
    'phase4_symbol_dossier_v1' AS DOSSIER_VERSION,
    ld.CLOSE AS CURRENT_PRICE,
    'DAILY_CLOSE' AS CURRENT_PRICE_SOURCE,
    ld.LATEST_DAILY_TS::DATE AS CURRENT_PRICE_DATE,
    ld.LATEST_DAILY_TS,
    rb.RECENT_DAILY_BARS_JSON,
    rb.RECENT_PRICE_ACTION_SUMMARY_JSON,
    rb.CANDLE_SEQUENCE_JSON,
    ns.NEAREST_SUPPORT_JSON,
    nr.NEAREST_RESISTANCE_JSON,
    ns.DISTANCE_TO_SUPPORT_PCT,
    nr.DISTANCE_TO_RESISTANCE_PCT,
    br.BROKEN_RESISTANCE_AS_SUPPORT_JSON,
    ARRAY_COMPACT(COALESCE(se.LONG_PATTERN_SIGNS_RAW, ARRAY_CONSTRUCT())) AS LONG_PATTERN_SIGNS_JSON,
    ARRAY_COMPACT(COALESCE(se.SHORT_PATTERN_SIGNS_RAW, ARRAY_CONSTRUCT())) AS SHORT_PATTERN_SIGNS_JSON,
    COALESCE(se.SETUP_EVENTS_JSON, ARRAY_CONSTRUCT()) AS SETUP_EVENTS_JSON,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'structural_state', st.STRUCTURAL_STATE,
        'prior_state', st.PRIOR_STATE,
        'state_entered_date', st.STATE_ENTERED_DATE,
        'bars_in_state', st.BARS_IN_STATE,
        'transition_trigger', st.TRANSITION_TRIGGER,
        'key_level_context', st.KEY_LEVEL_CONTEXT,
        'state_confidence', st.STATE_CONFIDENCE
    ) AS STRUCTURAL_STATE_JSON,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'vol_regime', rg.VOL_REGIME,
        'trend_regime', rg.TREND_REGIME,
        'range_regime', rg.RANGE_REGIME,
        'atr_20', rg.ATR_20,
        'sma_20', rg.SMA_20,
        'sma_10_slope', rg.SMA_10_SLOPE,
        'range_10_high', rg.RANGE_10_HIGH,
        'range_10_low', rg.RANGE_10_LOW
    ) AS REGIME_TAGS_JSON,
    rg.REGIME_SETUP_COMPAT AS REGIME_COMPAT_JSON,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'nearest_support', ns.NEAREST_SUPPORT_JSON,
        'broken_resistance_as_support', br.BROKEN_RESISTANCE_AS_SUPPORT_JSON,
        'recent_invalidated_setup_events', ARRAY_COMPACT(COALESCE(se.RECENT_INVALIDATED_SETUP_EVENTS_RAW, ARRAY_CONSTRUCT()))
    ) AS LONG_INVALIDATION_EVIDENCE_JSON,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'nearest_resistance', nr.NEAREST_RESISTANCE_JSON,
        'recent_invalidated_setup_events', ARRAY_COMPACT(COALESCE(se.RECENT_INVALIDATED_SETUP_EVENTS_RAW, ARRAY_CONSTRUCT()))
    ) AS SHORT_INVALIDATION_EVIDENCE_JSON,
    ARRAY_COMPACT(COALESCE(se.RECENT_INVALIDATED_SETUP_EVENTS_RAW, ARRAY_CONSTRUCT())) AS RECENT_INVALIDATED_SETUP_EVENTS_JSON,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'long', hl.HISTORY_ARRAY,
        'short', hs.HISTORY_ARRAY
    ) AS HISTORICAL_OUTCOME_EVIDENCE_JSON,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'long', pl.PATH_ARRAY,
        'short', ps.PATH_ARRAY
    ) AS PATH_TRUST_EVIDENCE_JSON,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'long_history', hl.HISTORY_ARRAY,
        'short_history', hs.HISTORY_ARRAY,
        'long_path', pl.PATH_ARRAY,
        'short_path', ps.PATH_ARRAY
    ) AS FAMILY_STATS_BY_DIRECTION_JSON,
    COALESCE(rtm.RECENT_TRADE_MEMORY_JSON, OBJECT_CONSTRUCT()) AS RECENT_TRADE_MEMORY_JSON,
    COALESCE(rpm.RECENT_PROPOSAL_MEMORY_JSON, OBJECT_CONSTRUCT()) AS RECENT_PROPOSAL_MEMORY_JSON,
    COALESCE(op.OPEN_POSITION_CONTEXT_JSON, OBJECT_CONSTRUCT()) AS OPEN_POSITION_CONTEXT_JSON,
    COALESCE(la.LIVE_ACTION_CONTEXT_JSON, OBJECT_CONSTRUCT()) AS LIVE_ACTION_CONTEXT_JSON,
    cfg.SHORT_RESEARCH_VISIBLE,
    cfg.SHORT_LIVE_ENABLED,
    cfg.FX_LIVE_ENABLED,
    ARRAY_COMPACT(ARRAY_CONSTRUCT(
        IFF(ld.CLOSE IS NULL, 'CURRENT_PRICE_MISSING', NULL),
        IFF(rb.RECENT_DAILY_BARS_JSON IS NULL, 'RECENT_BARS_MISSING', NULL),
        IFF(st.STRUCTURAL_STATE IS NULL, 'STRUCTURAL_STATE_MISSING', NULL),
        IFF(rg.VOL_REGIME IS NULL, 'REGIME_TAGS_MISSING', NULL)
    )) AS DATA_QUALITY_FLAGS,
    ARRAY_COMPACT(ARRAY_CONSTRUCT(
        IFF(COALESCE(la.LIVE_ACTION_CONTEXT_JSON:open_structural_actions::NUMBER, 0) > 0, 'EXISTING_LIVE_ACTION_CONTEXT', NULL),
        IFF(COALESCE(rpm.RECENT_PROPOSAL_MEMORY_JSON:active_proposals::NUMBER, 0) > 0, 'EXISTING_ACTIVE_PROPOSAL_CONTEXT', NULL),
        IFF(ARRAY_SIZE(ARRAY_COMPACT(COALESCE(se.LONG_PATTERN_SIGNS_RAW, ARRAY_CONSTRUCT()))) > 0
            AND ARRAY_SIZE(ARRAY_COMPACT(COALESCE(se.SHORT_PATTERN_SIGNS_RAW, ARRAY_CONSTRUCT()))) > 0, 'BOTH_LONG_AND_SHORT_EVIDENCE_VISIBLE', NULL),
        IFF(NOT cfg.SHORT_LIVE_ENABLED, 'SHORT_LIVE_DISABLED_BUT_RESEARCH_VISIBLE', NULL),
        -- Phase 8 coherence signal: any active setup exists but none pass the
        -- level<->entry coherence gate. Agents should treat direction inference
        -- from setup_events_evidence_only with strong skepticism.
        IFF(se.PRIMARY_EVIDENCE_ANY_ID IS NOT NULL
            AND se.PRIMARY_EVIDENCE_COHERENT_ID IS NULL,
            'NO_COHERENT_PRIMARY_EVIDENCE', NULL)
    )) AS BOARD_WARNING_FLAGS,
    se.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
    SHA2(TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
        'symbol', u.SYMBOL,
        'market_type', u.MARKET_TYPE,
        'as_of_date', CURRENT_DATE(),
        'current_price', ld.CLOSE,
        'recent_bars', rb.RECENT_DAILY_BARS_JSON,
        'structure', st.STRUCTURAL_STATE,
        'regime', rg.REGIME_SETUP_COMPAT,
        'setup_events', se.SETUP_EVENTS_JSON
    )), 256) AS PAYLOAD_HASH,
    OBJECT_CONSTRUCT_KEEP_NULL(
        -- Phase 4 evidence-hardening v2: market_structure_map + structural timeline v1.
        'evidence_contract_version', 'phase4_structural_v2',
        'identity', OBJECT_CONSTRUCT_KEEP_NULL(
            'symbol', u.SYMBOL,
            'market_type', u.MARKET_TYPE,
            'as_of_date', CURRENT_DATE(),
            'portfolio_id', NULL,
            'dossier_version', 'phase4_symbol_dossier_v2'
        ),
        'price', OBJECT_CONSTRUCT_KEEP_NULL(
            'current_price', ld.CLOSE,
            'current_price_source', 'DAILY_CLOSE',
            'current_price_date', ld.LATEST_DAILY_TS::DATE,
            'latest_daily_ts', ld.LATEST_DAILY_TS
        ),
        'recent_bars', rb.RECENT_DAILY_BARS_JSON,
        'recent_price_action_summary', rb.RECENT_PRICE_ACTION_SUMMARY_JSON,
        'candle_sequence', rb.CANDLE_SEQUENCE_JSON,
        'structural_timeline_summary', t90.STRUCTURAL_TIMELINE_SUMMARY_JSON,
        'structural_timeline_bars', t90.STRUCTURAL_TIMELINE_BARS_JSON,
        'candle_psychology', cp.CANDLE_PSYCHOLOGY_JSON,
        'actionability_context', ac.ACTIONABILITY_CONTEXT_JSON,
        'market_structure_map', msm.MARKET_STRUCTURE_MAP,
        'levels', OBJECT_CONSTRUCT_KEEP_NULL(
            'nearest_support', ns.NEAREST_SUPPORT_JSON,
            'nearest_resistance', nr.NEAREST_RESISTANCE_JSON,
            'distance_to_support_pct', ns.DISTANCE_TO_SUPPORT_PCT,
            'distance_to_resistance_pct', nr.DISTANCE_TO_RESISTANCE_PCT,
            'broken_resistance_as_support', br.BROKEN_RESISTANCE_AS_SUPPORT_JSON
        ),
        'long_pattern_signs', ARRAY_COMPACT(COALESCE(se.LONG_PATTERN_SIGNS_RAW, ARRAY_CONSTRUCT())),
        'short_pattern_signs', ARRAY_COMPACT(COALESCE(se.SHORT_PATTERN_SIGNS_RAW, ARRAY_CONSTRUCT())),
        'setup_events_evidence_only', COALESCE(se.SETUP_EVENTS_JSON, ARRAY_CONSTRUCT()),
        'primary_evidence_setup_event_id', se.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
        'primary_evidence_setup_event_id_role', 'EVIDENCE_ONLY_NOT_DIRECTION_SOURCE',
        'structure', OBJECT_CONSTRUCT_KEEP_NULL(
            'structural_state', st.STRUCTURAL_STATE,
            'prior_state', st.PRIOR_STATE,
            'state_entered_date', st.STATE_ENTERED_DATE,
            'bars_in_state', st.BARS_IN_STATE,
            'transition_trigger', st.TRANSITION_TRIGGER,
            'key_level_context', st.KEY_LEVEL_CONTEXT,
            'state_confidence', st.STATE_CONFIDENCE
        ),
        'regime', OBJECT_CONSTRUCT_KEEP_NULL(
            'tags', OBJECT_CONSTRUCT_KEEP_NULL(
                'vol_regime', rg.VOL_REGIME,
                'trend_regime', rg.TREND_REGIME,
                'range_regime', rg.RANGE_REGIME,
                'atr_20', rg.ATR_20,
                'sma_20', rg.SMA_20,
                'sma_10_slope', rg.SMA_10_SLOPE,
                'range_10_high', rg.RANGE_10_HIGH,
                'range_10_low', rg.RANGE_10_LOW
            ),
            'compatibility_by_setup_evidence', rg.REGIME_SETUP_COMPAT
        ),
        'invalidation_evidence', OBJECT_CONSTRUCT_KEEP_NULL(
            'long', OBJECT_CONSTRUCT_KEEP_NULL(
                'nearest_support', ns.NEAREST_SUPPORT_JSON,
                'broken_resistance_as_support', br.BROKEN_RESISTANCE_AS_SUPPORT_JSON
            ),
            'short', OBJECT_CONSTRUCT_KEEP_NULL(
                'nearest_resistance', nr.NEAREST_RESISTANCE_JSON
            ),
            'recent_invalidated_setup_events', ARRAY_COMPACT(COALESCE(se.RECENT_INVALIDATED_SETUP_EVENTS_RAW, ARRAY_CONSTRUCT()))
        ),
        'history', OBJECT_CONSTRUCT_KEEP_NULL(
            'long_history', hl.HISTORY_ARRAY,
            'short_history', hs.HISTORY_ARRAY,
            'long_path', pl.PATH_ARRAY,
            'short_path', ps.PATH_ARRAY
        ),
        'memory', OBJECT_CONSTRUCT_KEEP_NULL(
            'recent_trade_memory', COALESCE(rtm.RECENT_TRADE_MEMORY_JSON, OBJECT_CONSTRUCT()),
            'recent_proposal_memory', COALESCE(rpm.RECENT_PROPOSAL_MEMORY_JSON, OBJECT_CONSTRUCT()),
            'open_position_context', COALESCE(op.OPEN_POSITION_CONTEXT_JSON, OBJECT_CONSTRUCT()),
            'live_action_context', COALESCE(la.LIVE_ACTION_CONTEXT_JSON, OBJECT_CONSTRUCT())
        ),
        'policy', OBJECT_CONSTRUCT_KEEP_NULL(
            'short_research_visible', cfg.SHORT_RESEARCH_VISIBLE,
            'short_live_enabled', cfg.SHORT_LIVE_ENABLED,
            'fx_live_enabled', cfg.FX_LIVE_ENABLED,
            'short_evidence_visibility_rule', 'SHORT_EVIDENCE_ALWAYS_VISIBLE_WHEN_AVAILABLE',
            'short_publication_rule', 'EXECUTABLE_SHORTS_REQUIRE_SHORT_LIVE_ENABLED_TRUE'
        ),
        'warnings', ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(ld.CLOSE IS NULL, 'CURRENT_PRICE_MISSING', NULL),
            IFF(rb.RECENT_DAILY_BARS_JSON IS NULL, 'RECENT_BARS_MISSING', NULL),
            IFF(st.STRUCTURAL_STATE IS NULL, 'STRUCTURAL_STATE_MISSING', NULL),
            IFF(rg.VOL_REGIME IS NULL, 'REGIME_TAGS_MISSING', NULL),
            IFF(NOT cfg.SHORT_LIVE_ENABLED, 'SHORT_LIVE_DISABLED_BUT_RESEARCH_VISIBLE', NULL)
        ))
    ) AS DOSSIER_PAYLOAD_JSON
FROM symbol_universe u
CROSS JOIN cfg
LEFT JOIN latest_daily ld
  ON ld.SYMBOL = u.SYMBOL
 AND ld.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN recent_bars rb
  ON rb.SYMBOL = u.SYMBOL
 AND rb.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN latest_state st
  ON st.SYMBOL = u.SYMBOL
 AND st.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN latest_regime rg
  ON rg.SYMBOL = u.SYMBOL
 AND rg.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN nearest_support ns
  ON ns.SYMBOL = u.SYMBOL
 AND ns.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN nearest_resistance nr
  ON nr.SYMBOL = u.SYMBOL
 AND nr.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN broken_resistance br
  ON br.SYMBOL = u.SYMBOL
 AND br.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN structural_timeline_90d t90
  ON t90.SYMBOL = u.SYMBOL
 AND t90.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN MIP.MART.V_SYMBOL_MARKET_STRUCTURE_MAP msm
  ON msm.SYMBOL = u.SYMBOL
 AND msm.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN candle_psychology cp
  ON cp.SYMBOL = u.SYMBOL
 AND cp.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN actionability_context ac
  ON ac.SYMBOL = u.SYMBOL
 AND ac.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN setup_events se
  ON se.SYMBOL = u.SYMBOL
 AND se.MARKET_TYPE = u.MARKET_TYPE
LEFT JOIN history_by_direction hl
  ON hl.MARKET_TYPE = u.MARKET_TYPE
 AND hl.EVIDENCE_DIRECTION = 'LONG'
LEFT JOIN history_by_direction hs
  ON hs.MARKET_TYPE = u.MARKET_TYPE
 AND hs.EVIDENCE_DIRECTION = 'SHORT'
LEFT JOIN path_by_direction pl
  ON pl.MARKET_TYPE = u.MARKET_TYPE
 AND pl.EVIDENCE_DIRECTION = 'LONG'
LEFT JOIN path_by_direction ps
  ON ps.MARKET_TYPE = u.MARKET_TYPE
 AND ps.EVIDENCE_DIRECTION = 'SHORT'
LEFT JOIN recent_trade_memory rtm
  ON rtm.SYMBOL = u.SYMBOL
LEFT JOIN recent_proposal_memory rpm
  ON rpm.SYMBOL = u.SYMBOL
LEFT JOIN open_position_context op
  ON op.SYMBOL = u.SYMBOL
LEFT JOIN live_action_context la
  ON la.SYMBOL = u.SYMBOL
WHERE ld.CLOSE IS NOT NULL;

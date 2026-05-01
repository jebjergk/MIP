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
                'body_direction', IFF(CLOSE >= OPEN, 'UP', 'DOWN'),
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
        ) <= 10
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
            'distance_below_current_pct', 100 * (CURRENT_PRICE - LEVEL_PRICE) / NULLIF(CURRENT_PRICE, 0)
        ) AS BROKEN_RESISTANCE_AS_SUPPORT_JSON
    FROM level_base
    WHERE LEVEL_TYPE IN ('RESISTANCE_ZONE', 'SWING_HIGH')
      AND LEVEL_PRICE < CURRENT_PRICE
      AND 100 * (CURRENT_PRICE - LEVEL_PRICE) / NULLIF(CURRENT_PRICE, 0) <= 3.0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SYMBOL, MARKET_TYPE
        ORDER BY CURRENT_PRICE - LEVEL_PRICE
    ) = 1
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
                'features_json', FEATURES_JSON
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
                    'regime_compat', REGIME_COMPAT
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
                    'regime_compat', REGIME_COMPAT
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
        MAX(IFF(SETUP_STATUS IN ('DETECTED', 'ELIGIBLE', 'WAITING', 'STALE'), SETUP_EVENT_ID, NULL)) AS PRIMARY_EVIDENCE_SETUP_EVENT_ID
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS
    WHERE SETUP_DATE >= DATEADD('day', -30, CURRENT_DATE())
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
recent_proposal_memory AS (
    SELECT
        SYMBOL,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'proposals_14d', COUNT_IF(CREATED_AT >= DATEADD('day', -14, CURRENT_TIMESTAMP())),
            'last_proposed_at', MAX(CREATED_AT),
            'active_proposals', COUNT_IF(STATUS = 'PROPOSED'),
            'recent_agentic_proposals_14d', COUNT_IF(CREATED_AT >= DATEADD('day', -14, CURRENT_TIMESTAMP()) AND SETUP_FAMILY ILIKE 'AGENTIC_%')
        ) AS RECENT_PROPOSAL_MEMORY_JSON
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    GROUP BY SYMBOL
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
open_position_context AS (
    SELECT
        SYMBOL,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'open_position_count', COUNT(*),
            'total_quantity', SUM(QUANTITY),
            'portfolio_ids', ARRAY_UNIQUE_AGG(PORTFOLIO_ID)
        ) AS OPEN_POSITION_CONTEXT_JSON
    FROM MIP.APP.PORTFOLIO_POSITIONS
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
        IFF(NOT cfg.SHORT_LIVE_ENABLED, 'SHORT_LIVE_DISABLED_BUT_RESEARCH_VISIBLE', NULL)
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
        'identity', OBJECT_CONSTRUCT_KEEP_NULL(
            'symbol', u.SYMBOL,
            'market_type', u.MARKET_TYPE,
            'as_of_date', CURRENT_DATE(),
            'portfolio_id', NULL,
            'dossier_version', 'phase4_symbol_dossier_v1'
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

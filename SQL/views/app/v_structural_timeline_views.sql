/*  ================================================================
    v_structural_timeline_views.sql
    Snowflake views powering the Structural Market Timeline UI.
    Symbol-first, chart-centric views for structural context over time.
    ETF excluded (not tradeable).
    Levels are historically faithful (shown from detection date only).
    Narratives and proposal-skip reasons are backend-driven.
    ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE WAREHOUSE MIP_WH_XS;

-- ================================================================
-- 1. V_STRUCTURAL_TIMELINE_PRICE
--    Daily price bars pre-joined with structural state and regime.
--    One query gives chart everything it needs per bar.
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TIMELINE_PRICE AS
SELECT
    mb.TS::DATE                         AS BAR_DATE,
    mb.SYMBOL,
    mb.MARKET_TYPE,
    mb.OPEN,
    mb.HIGH,
    mb.LOW,
    mb.CLOSE,
    mb.VOLUME,
    sl.STRUCTURAL_STATE,
    sl.STATE_CONFIDENCE,
    sl.PRIOR_STATE,
    sl.BARS_IN_STATE,
    rt.VOL_REGIME,
    rt.TREND_REGIME,
    rt.RANGE_REGIME,
    rt.ATR_20
FROM MIP.MART.MARKET_BARS mb
LEFT JOIN MIP.APP.STRUCTURAL_STATE_LOG sl
  ON sl.SYMBOL = mb.SYMBOL
  AND sl.MARKET_TYPE = mb.MARKET_TYPE
  AND sl.AS_OF_DATE = mb.TS::DATE
LEFT JOIN MIP.APP.STRUCTURAL_REGIME_TAG rt
  ON rt.SYMBOL = mb.SYMBOL
  AND rt.MARKET_TYPE = mb.MARKET_TYPE
  AND rt.AS_OF_DATE = mb.TS::DATE
WHERE mb.INTERVAL_MINUTES = 1440
  AND mb.MARKET_TYPE != 'ETF';

-- ================================================================
-- 2. V_STRUCTURAL_TIMELINE_LEVELS
--    Structural levels/zones with full history for faithful rendering.
--    Frontend draws each level starting from FIRST_TOUCH_DATE only.
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TIMELINE_LEVELS AS
SELECT
    lc.LEVEL_ID,
    lc.SYMBOL,
    lc.MARKET_TYPE,
    lc.AS_OF_DATE,
    lc.LEVEL_TYPE,
    lc.LEVEL_PRICE,
    lc.LEVEL_LOW,
    lc.LEVEL_HIGH,
    lc.TOUCH_COUNT,
    lc.FIRST_TOUCH_DATE,
    lc.LAST_TOUCH_DATE,
    lc.LEVEL_SIGNIFICANCE,
    lc.TOUCH_SCORE,
    lc.RECENCY_SCORE,
    lc.REACTION_SCORE,
    lc.HISTORY_SCORE,
    lc.CLUSTER_SCORE
FROM MIP.APP.STRUCTURAL_LEVEL_CACHE lc
WHERE lc.MARKET_TYPE != 'ETF';

-- ================================================================
-- 3. V_STRUCTURAL_TIMELINE_SETUPS
--    Setup events enriched with lifecycle, proposal readiness,
--    skip reason, outcome, proposal linkage, and backend narrative.
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TIMELINE_SETUPS AS
WITH policy_active AS (
    SELECT DISTINCT SETUP_FAMILY, DIRECTION
    FROM MIP.APP.STRUCTURAL_RISK_POLICY
    WHERE IS_ACTIVE = TRUE
),
trust_best AS (
    SELECT SETUP_FAMILY, MARKET_TYPE, TRUST_LABEL,
           MEANINGFUL_HIT_RATE AS FAMILY_MHR,
           PATH_SURVIVAL_HIT_RATE AS FAMILY_PATH_SURV,
           MFE_MAE_RATIO AS FAMILY_MFE_MAE,
           BEST_WINDOW
    FROM MIP.APP.STRUCTURAL_SETUP_TRUST
    WHERE EVAL_WINDOW = BEST_WINDOW
)
SELECT
    se.SETUP_EVENT_ID,
    se.SETUP_FAMILY,
    se.DIRECTION,
    se.SYMBOL,
    se.MARKET_TYPE,
    se.SETUP_DATE,
    se.STRUCTURAL_STATE,
    se.PRIOR_STATE,
    se.LEVEL_TYPE,
    se.LEVEL_PRICE,
    se.LEVEL_SIGNIFICANCE,
    se.ENTRY_ZONE_LOW,
    se.ENTRY_ZONE_HIGH,
    se.PRICE_INVALIDATION_LEVEL,
    se.INVALIDATION_RULE,
    se.INVALIDATION_BUFFER_ATR,
    se.TRAIL_ACTIVATION_CONDITION,
    se.TRAIL_ACTIVATION_THRESHOLD,
    se.TRAIL_STYLE,
    se.TRAIL_PARAMS,
    se.STRUCTURE_CONFIDENCE,
    se.WICK_CONFIRMATION_SCORE,
    se.THREE_BAR_CONFIRMATION_SCORE,
    se.TREND_CONTEXT_SCORE,
    se.REGIME_COMPAT,
    se.RISK_CLASS,
    se.VOLATILITY_CONTEXT,
    se.CONFLUENCE_FLAGS,

    -- Lifecycle fields
    se.SETUP_STATUS,
    se.STATUS_UPDATED_AT,
    se.BARS_SINCE_DETECTION,
    se.DISTANCE_FROM_ENTRY_ZONE,
    se.ELIGIBLE_SINCE,
    se.EXPIRY_DATE,

    -- Trust / readiness context
    COALESCE(tb.TRUST_LABEL, 'UNKNOWN')             AS TRUST_LABEL,
    tb.FAMILY_MHR,
    tb.FAMILY_PATH_SURV,
    tb.FAMILY_MFE_MAE,
    COALESCE(tb.BEST_WINDOW, 20)                    AS BEST_WINDOW,
    CASE WHEN pa.SETUP_FAMILY IS NOT NULL THEN TRUE ELSE FALSE END AS IS_POLICY_ACTIVE,
    CASE
        WHEN COALESCE(tb.TRUST_LABEL, '') IN ('TRUSTED', 'PROVISIONAL')
             AND pa.SETUP_FAMILY IS NOT NULL
        THEN TRUE ELSE FALSE
    END                                              AS IS_PROPOSAL_READY,
    CASE
        WHEN pa.SETUP_FAMILY IS NULL                          THEN 'NO_ACTIVE_POLICY'
        WHEN COALESCE(tb.TRUST_LABEL, '') = 'REJECTED'        THEN 'REJECTED_TRUST'
        WHEN COALESCE(tb.TRUST_LABEL, '') = 'RESEARCH'         THEN 'RESEARCH_ONLY'
        WHEN COALESCE(tb.TRUST_LABEL, '') NOT IN ('TRUSTED', 'PROVISIONAL')
                                                               THEN 'INSUFFICIENT_TRUST'
        WHEN se.REGIME_COMPAT = 'POOR'                         THEN 'WEAK_REGIME'
        ELSE NULL
    END                                              AS PROPOSAL_SKIP_REASON,

    -- Outcome (20-bar window)
    so.EVAL_STATUS,
    so.MAX_FAVORABLE_EXCURSION                       AS MFE_PCT,
    so.MAX_ADVERSE_EXCURSION                         AS MAE_PCT,
    so.MFE_MAE_RATIO,
    so.MEANINGFUL_MOVE_SUCCESS,
    so.DIRECTIONAL_SUCCESS,
    so.INVALIDATION_HIT,
    so.FAILURE_MODE,
    so.BARS_TO_CONFIRMATION,
    so.BARS_TO_INVALIDATION,

    -- Proposal linkage — direction-guarded: only link when proposal direction
    -- matches setup direction (SETUP_EVENT_ID is NULL for cross-direction cases).
    sp.PROPOSAL_ID,
    sp.STATUS                                        AS PROPOSAL_STATUS,
    sp.RATIONALE_TEXT,
    sp.EXECUTION_POLICY_STATUS,
    sp.EXECUTION_POLICY_REASON,
    sp.IS_RESEARCH_ONLY,
    CASE WHEN sp.PROPOSAL_ID IS NOT NULL THEN TRUE ELSE FALSE END AS BECAME_PROPOSAL,

    -- Backend-driven plain-language narrative
    CASE se.SETUP_FAMILY
        WHEN 'BREAKOUT_RETEST_LONG' THEN
            'Price broke above ' || COALESCE(se.LEVEL_TYPE, 'resistance') || ' near $' || ROUND(se.LEVEL_PRICE, 2)
            || ', pulled back into the breakout zone ($' || ROUND(se.ENTRY_ZONE_LOW, 2) || ' – $' || ROUND(se.ENTRY_ZONE_HIGH, 2)
            || '), and held. Structural state: ' || COALESCE(se.STRUCTURAL_STATE, '—')
            || '. Wick confirmation: ' || COALESCE(ROUND(se.WICK_CONFIRMATION_SCORE, 2)::VARCHAR, '—')
            || ', 3-bar score: ' || COALESCE(ROUND(se.THREE_BAR_CONFIRMATION_SCORE, 2)::VARCHAR, '—')
            || '. Regime: ' || COALESCE(se.REGIME_COMPAT, '—') || '.'
        WHEN 'BREAKDOWN_RETEST_SHORT' THEN
            'Price broke below ' || COALESCE(se.LEVEL_TYPE, 'support') || ' near $' || ROUND(se.LEVEL_PRICE, 2)
            || ', rallied back into the breakdown zone ($' || ROUND(se.ENTRY_ZONE_LOW, 2) || ' – $' || ROUND(se.ENTRY_ZONE_HIGH, 2)
            || '), and failed to reclaim. Structural state: ' || COALESCE(se.STRUCTURAL_STATE, '—')
            || '. Wick confirmation: ' || COALESCE(ROUND(se.WICK_CONFIRMATION_SCORE, 2)::VARCHAR, '—')
            || ', 3-bar score: ' || COALESCE(ROUND(se.THREE_BAR_CONFIRMATION_SCORE, 2)::VARCHAR, '—')
            || '. Regime: ' || COALESCE(se.REGIME_COMPAT, '—') || '.'
        WHEN 'SUPPORT_WICK_LONG' THEN
            'Price tested ' || COALESCE(se.LEVEL_TYPE, 'support') || ' near $' || ROUND(se.LEVEL_PRICE, 2)
            || ' and produced a strong lower-wick rejection within the entry zone ($' || ROUND(se.ENTRY_ZONE_LOW, 2) || ' – $' || ROUND(se.ENTRY_ZONE_HIGH, 2)
            || '). Wick score: ' || COALESCE(ROUND(se.WICK_CONFIRMATION_SCORE, 2)::VARCHAR, '—')
            || '. State: ' || COALESCE(se.STRUCTURAL_STATE, '—')
            || '. Regime: ' || COALESCE(se.REGIME_COMPAT, '—') || '.'
        WHEN 'RESISTANCE_WICK_SHORT' THEN
            'Price tested ' || COALESCE(se.LEVEL_TYPE, 'resistance') || ' near $' || ROUND(se.LEVEL_PRICE, 2)
            || ' and produced a strong upper-wick rejection within the entry zone ($' || ROUND(se.ENTRY_ZONE_LOW, 2) || ' – $' || ROUND(se.ENTRY_ZONE_HIGH, 2)
            || '). Wick score: ' || COALESCE(ROUND(se.WICK_CONFIRMATION_SCORE, 2)::VARCHAR, '—')
            || '. State: ' || COALESCE(se.STRUCTURAL_STATE, '—')
            || '. Regime: ' || COALESCE(se.REGIME_COMPAT, '—') || '.'
        WHEN 'THREE_BAR_REVERSAL_LONG' THEN
            'A 3-bar bullish reversal formed at ' || COALESCE(se.LEVEL_TYPE, 'support') || ' near $' || ROUND(se.LEVEL_PRICE, 2)
            || '. Entry zone: $' || ROUND(se.ENTRY_ZONE_LOW, 2) || ' – $' || ROUND(se.ENTRY_ZONE_HIGH, 2)
            || '. 3-bar score: ' || COALESCE(ROUND(se.THREE_BAR_CONFIRMATION_SCORE, 2)::VARCHAR, '—')
            || '. State: ' || COALESCE(se.STRUCTURAL_STATE, '—')
            || '. Regime: ' || COALESCE(se.REGIME_COMPAT, '—') || '.'
        WHEN 'THREE_BAR_REVERSAL_SHORT' THEN
            'A 3-bar bearish reversal formed at ' || COALESCE(se.LEVEL_TYPE, 'resistance') || ' near $' || ROUND(se.LEVEL_PRICE, 2)
            || '. Entry zone: $' || ROUND(se.ENTRY_ZONE_LOW, 2) || ' – $' || ROUND(se.ENTRY_ZONE_HIGH, 2)
            || '. 3-bar score: ' || COALESCE(ROUND(se.THREE_BAR_CONFIRMATION_SCORE, 2)::VARCHAR, '—')
            || '. State: ' || COALESCE(se.STRUCTURAL_STATE, '—')
            || '. Regime: ' || COALESCE(se.REGIME_COMPAT, '—') || '.'
        WHEN 'TREND_PULLBACK_LONG' THEN
            'Trend pullback to ' || COALESCE(se.LEVEL_TYPE, 'support') || ' near $' || ROUND(se.LEVEL_PRICE, 2)
            || ' within an uptrend. Entry zone: $' || ROUND(se.ENTRY_ZONE_LOW, 2) || ' – $' || ROUND(se.ENTRY_ZONE_HIGH, 2)
            || '. Trend context: ' || COALESCE(ROUND(se.TREND_CONTEXT_SCORE, 2)::VARCHAR, '—')
            || '. State: ' || COALESCE(se.STRUCTURAL_STATE, '—')
            || '. Regime: ' || COALESCE(se.REGIME_COMPAT, '—') || '.'
        WHEN 'FAILED_BREAKOUT_SHORT' THEN
            'A breakout above ' || COALESCE(se.LEVEL_TYPE, 'resistance') || ' near $' || ROUND(se.LEVEL_PRICE, 2)
            || ' failed and price reversed back below the level. Entry zone: $' || ROUND(se.ENTRY_ZONE_LOW, 2) || ' – $' || ROUND(se.ENTRY_ZONE_HIGH, 2)
            || '. State: ' || COALESCE(se.STRUCTURAL_STATE, '—')
            || '. Regime: ' || COALESCE(se.REGIME_COMPAT, '—') || '.'
        ELSE
            se.SETUP_FAMILY || ' (' || se.DIRECTION || ') detected at '
            || COALESCE(se.LEVEL_TYPE, 'level') || ' $' || ROUND(se.LEVEL_PRICE, 2)
            || '. Entry zone: $' || ROUND(se.ENTRY_ZONE_LOW, 2) || ' – $' || ROUND(se.ENTRY_ZONE_HIGH, 2)
            || '. State: ' || COALESCE(se.STRUCTURAL_STATE, '—')
            || '. Regime: ' || COALESCE(se.REGIME_COMPAT, '—') || '.'
    END                                              AS SETUP_NARRATIVE

FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_OUTCOMES so
  ON so.SETUP_EVENT_ID = se.SETUP_EVENT_ID AND so.EVAL_WINDOW = 20 AND so.EVAL_STATUS = 'SUCCESS'
-- Direction-guarded join: SETUP_EVENT_ID is NULL for cross-direction proposals,
-- so this join naturally excludes contaminated cross-direction links.
LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
  ON sp.SETUP_EVENT_ID = se.SETUP_EVENT_ID
  AND sp.DIRECTION = se.DIRECTION
  AND sp.BOARD_RUN_ID IS NOT NULL
LEFT JOIN trust_best tb
  ON tb.SETUP_FAMILY = se.SETUP_FAMILY AND tb.MARKET_TYPE = se.MARKET_TYPE
LEFT JOIN policy_active pa
  ON pa.SETUP_FAMILY = se.SETUP_FAMILY AND pa.DIRECTION = se.DIRECTION
WHERE se.MARKET_TYPE != 'ETF';

-- ================================================================
-- 4. V_STRUCTURAL_TIMELINE_PROPOSALS
--    Proposals with full setup-to-trade lineage.
--    Agentic-only: BOARD_RUN_ID IS NOT NULL (legacy deterministic
--    selector retired post-Phase-4 cutover; only Phase 4 Cortex
--    agentic-board rows surface here). The IS_AGENTIC flag is
--    always TRUE in this view; it is exposed so the UI can label
--    the lane unambiguously.
-- ================================================================
-- ================================================================
-- 4. V_STRUCTURAL_TIMELINE_PROPOSALS
--    Proposals with full setup-to-trade lineage.
--    Strict column separation: proposal fields come from sp,
--    evidence fields from the evidence setup event.
--    IS_CROSS_DIRECTION_EVIDENCE = TRUE when SETUP_EVENT_ID is NULL
--    (proposal direction differs from primary evidence setup direction).
--    Agentic-only: BOARD_RUN_ID IS NOT NULL.
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TIMELINE_PROPOSALS AS
SELECT
    -- ── Proposal identity ──────────────────────────────────────────
    sp.PROPOSAL_ID,
    sp.SYMBOL,
    sp.STATUS                                AS PROPOSAL_STATUS,
    sp.CREATED_AT                            AS PROPOSAL_CREATED_AT,
    sp.BOARD_RUN_ID,
    TRUE                                     AS IS_AGENTIC,

    -- ── Proposal direction + geometry (from sp — always authoritative) ──
    sp.DIRECTION                             AS PROPOSAL_DIRECTION,
    sp.SETUP_FAMILY                          AS PROPOSAL_SETUP_FAMILY,
    sp.ENTRY_ZONE_LOW,
    sp.ENTRY_ZONE_HIGH,
    sp.PRICE_INVALIDATION_LEVEL,
    sp.INVALIDATION_RULE,
    sp.TRAIL_STYLE,
    sp.EXIT_STYLE,
    sp.EXIT_PROFILE,
    sp.STRUCTURE_CONFIDENCE,
    sp.LEVEL_SIGNIFICANCE,
    sp.REGIME_COMPAT,
    sp.MEANINGFUL_HIT_RATE,
    sp.PATH_SURVIVAL_HIT_RATE,
    sp.MFE_MAE_RATIO,
    sp.RISK_CLASS,
    sp.RATIONALE_TEXT,

    -- ── Execution policy (hard gate) ──────────────────────────────
    sp.EXECUTION_POLICY_STATUS,
    sp.EXECUTION_POLICY_REASON,
    sp.IS_RESEARCH_ONLY,

    -- ── Evidence setup event fields (may differ in direction) ─────
    sp.SETUP_EVENT_ID,
    sp.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
    -- Evidence direction from the linked same-direction event (NULL if cross-direction)
    se_same.DIRECTION                        AS EVIDENCE_SETUP_DIRECTION,
    se_same.SETUP_FAMILY                     AS EVIDENCE_SETUP_FAMILY,
    -- For cross-direction: pull from the actual evidence event for context only
    COALESCE(se_same.SETUP_DATE, se_ev.SETUP_DATE)    AS EVIDENCE_SETUP_DATE,
    COALESCE(se_same.MARKET_TYPE, se_ev.MARKET_TYPE)  AS MARKET_TYPE,
    COALESCE(se_same.STRUCTURAL_STATE, se_ev.STRUCTURAL_STATE) AS EVIDENCE_STRUCTURAL_STATE,
    COALESCE(se_same.REGIME_COMPAT, se_ev.REGIME_COMPAT) AS EVIDENCE_REGIME_COMPAT,

    -- ── Cross-direction evidence flag ─────────────────────────────
    -- TRUE when the primary evidence setup direction differs from proposal direction.
    -- These proposals are POLICY_BLOCKED (research-only) at LPA/API.
    IFF(sp.SETUP_EVENT_ID IS NULL AND sp.PRIMARY_EVIDENCE_SETUP_EVENT_ID IS NOT NULL,
        TRUE, FALSE)                         AS IS_CROSS_DIRECTION_EVIDENCE,
    -- Evidence-only context: setup family of cross-direction evidence (for diagnostics)
    IFF(sp.SETUP_EVENT_ID IS NULL, se_ev.SETUP_FAMILY, NULL) AS CROSS_DIRECTION_EVIDENCE_SETUP_FAMILY,
    IFF(sp.SETUP_EVENT_ID IS NULL, se_ev.DIRECTION,    NULL) AS CROSS_DIRECTION_EVIDENCE_DIRECTION,

    -- ── Trade linkage ─────────────────────────────────────────────
    pt.TRADE_ID,
    pt.TRADE_TS,
    pt.SIDE                                  AS TRADE_SIDE,
    pt.PRICE                                 AS TRADE_PRICE,
    pt.QUANTITY                              AS TRADE_QTY,
    pt.REALIZED_PNL

FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
-- Same-direction evidence event (non-null SETUP_EVENT_ID means directions match)
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se_same
  ON se_same.SETUP_EVENT_ID = sp.SETUP_EVENT_ID
-- Primary evidence event regardless of direction (for cross-direction context)
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se_ev
  ON se_ev.SETUP_EVENT_ID = sp.PRIMARY_EVIDENCE_SETUP_EVENT_ID
LEFT JOIN MIP.APP.PORTFOLIO_TRADES pt
  ON pt.PROPOSAL_ID = sp.PROPOSAL_ID
WHERE COALESCE(se_same.MARKET_TYPE, se_ev.MARKET_TYPE) != 'ETF'
  AND sp.BOARD_RUN_ID IS NOT NULL;

-- ================================================================
-- 5. V_STRUCTURAL_TIMELINE_EVENTS
--    Unified chronological event stream for the timeline rail.
--    UNION of state changes, setup lifecycle, proposals, and levels.
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TIMELINE_EVENTS AS

-- State transitions
SELECT
    sl.AS_OF_DATE                            AS EVENT_DATE,
    sl.SYMBOL,
    sl.MARKET_TYPE,
    'STATE_CHANGED'                          AS EVENT_TYPE,
    COALESCE(sl.PRIOR_STATE, '—') || ' → ' || sl.STRUCTURAL_STATE AS EVENT_DESCRIPTION,
    NULL                                     AS SETUP_FAMILY,
    NULL                                     AS DIRECTION,
    NULL                                     AS SETUP_EVENT_ID,
    NULL                                     AS PROPOSAL_ID
FROM MIP.APP.STRUCTURAL_STATE_LOG sl
WHERE sl.PRIOR_STATE IS NOT NULL
  AND sl.PRIOR_STATE != sl.STRUCTURAL_STATE
  AND sl.MARKET_TYPE != 'ETF'

UNION ALL

-- Setup detected
SELECT
    se.SETUP_DATE                            AS EVENT_DATE,
    se.SYMBOL,
    se.MARKET_TYPE,
    'SETUP_DETECTED'                         AS EVENT_TYPE,
    se.SETUP_FAMILY || ' ' || se.DIRECTION || ' detected'
    || CASE WHEN se.LEVEL_PRICE IS NOT NULL THEN ' at $' || ROUND(se.LEVEL_PRICE, 2)::VARCHAR ELSE '' END
                                             AS EVENT_DESCRIPTION,
    se.SETUP_FAMILY,
    se.DIRECTION,
    se.SETUP_EVENT_ID,
    NULL                                     AS PROPOSAL_ID
FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
WHERE se.MARKET_TYPE != 'ETF'

UNION ALL

-- Setup became eligible
SELECT
    se.ELIGIBLE_SINCE                        AS EVENT_DATE,
    se.SYMBOL,
    se.MARKET_TYPE,
    'SETUP_ELIGIBLE'                         AS EVENT_TYPE,
    se.SETUP_FAMILY || ' ' || se.DIRECTION || ' became eligible' AS EVENT_DESCRIPTION,
    se.SETUP_FAMILY,
    se.DIRECTION,
    se.SETUP_EVENT_ID,
    NULL                                     AS PROPOSAL_ID
FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
WHERE se.ELIGIBLE_SINCE IS NOT NULL
  AND se.MARKET_TYPE != 'ETF'

UNION ALL

-- Setup invalidated / expired / stale
SELECT
    COALESCE(se.STATUS_UPDATED_AT::DATE, se.SETUP_DATE) AS EVENT_DATE,
    se.SYMBOL,
    se.MARKET_TYPE,
    CASE se.SETUP_STATUS
        WHEN 'INVALIDATED' THEN 'SETUP_INVALIDATED'
        WHEN 'EXPIRED'     THEN 'SETUP_EXPIRED'
        WHEN 'STALE'       THEN 'SETUP_STALE'
        ELSE 'SETUP_STATUS_CHANGE'
    END                                      AS EVENT_TYPE,
    se.SETUP_FAMILY || ' ' || se.DIRECTION || ' ' || LOWER(se.SETUP_STATUS) AS EVENT_DESCRIPTION,
    se.SETUP_FAMILY,
    se.DIRECTION,
    se.SETUP_EVENT_ID,
    NULL                                     AS PROPOSAL_ID
FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
WHERE se.SETUP_STATUS IN ('INVALIDATED', 'EXPIRED', 'STALE')
  AND se.MARKET_TYPE != 'ETF'

UNION ALL

-- Proposal created (agentic-only: legacy rows with BOARD_RUN_ID NULL are excluded)
SELECT
    sp.CREATED_AT::DATE                      AS EVENT_DATE,
    sp.SYMBOL,
    se.MARKET_TYPE,
    'PROPOSAL_CREATED'                       AS EVENT_TYPE,
    sp.SETUP_FAMILY || ' ' || sp.DIRECTION || ' proposal created' AS EVENT_DESCRIPTION,
    sp.SETUP_FAMILY,
    sp.DIRECTION,
    sp.SETUP_EVENT_ID,
    sp.PROPOSAL_ID
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se
  ON se.SETUP_EVENT_ID = sp.SETUP_EVENT_ID
WHERE se.MARKET_TYPE != 'ETF'
  AND sp.BOARD_RUN_ID IS NOT NULL

UNION ALL

-- New structural level detected
SELECT
    lc.AS_OF_DATE                            AS EVENT_DATE,
    lc.SYMBOL,
    lc.MARKET_TYPE,
    'LEVEL_DETECTED'                         AS EVENT_TYPE,
    lc.LEVEL_TYPE || ' at $' || ROUND(lc.LEVEL_PRICE, 2)::VARCHAR
    || ' (significance: ' || ROUND(lc.LEVEL_SIGNIFICANCE, 2)::VARCHAR || ')' AS EVENT_DESCRIPTION,
    NULL                                     AS SETUP_FAMILY,
    NULL                                     AS DIRECTION,
    NULL                                     AS SETUP_EVENT_ID,
    NULL                                     AS PROPOSAL_ID
FROM MIP.APP.STRUCTURAL_LEVEL_CACHE lc
WHERE lc.AS_OF_DATE = lc.FIRST_TOUCH_DATE
  AND lc.MARKET_TYPE != 'ETF';

-- ================================================================
-- 6. V_STRUCTURAL_TIMELINE_SUMMARY
--    Symbol-level KPIs. "Current" fields use most recent row.
-- ================================================================
CREATE OR REPLACE VIEW MIP.MART.V_STRUCTURAL_TIMELINE_SUMMARY AS
WITH setup_counts AS (
    SELECT
        se.SYMBOL,
        se.MARKET_TYPE,
        COUNT(*)                                               AS TOTAL_SETUPS,
        COUNT(CASE WHEN se.SETUP_STATUS = 'ELIGIBLE' THEN 1 END)  AS ELIGIBLE_SETUPS,
        SUM(CASE WHEN se.DIRECTION = 'LONG' THEN 1 ELSE 0 END)   AS LONG_SETUPS,
        SUM(CASE WHEN se.DIRECTION = 'SHORT' THEN 1 ELSE 0 END)  AS SHORT_SETUPS
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
    WHERE se.MARKET_TYPE != 'ETF'
    GROUP BY se.SYMBOL, se.MARKET_TYPE
),
proposal_counts AS (
    -- Agentic-only: legacy deterministic-selector rows (BOARD_RUN_ID NULL) are excluded.
    SELECT
        sp.SYMBOL,
        se.MARKET_TYPE,
        COUNT(*)                                                                AS PROPOSALS_CREATED,
        COUNT(CASE WHEN sp.STATUS = 'PROPOSED' THEN 1 END)                      AS ACTIVE_PROPOSALS
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
    JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se ON se.SETUP_EVENT_ID = sp.SETUP_EVENT_ID
    WHERE se.MARKET_TYPE != 'ETF'
      AND sp.BOARD_RUN_ID IS NOT NULL
    GROUP BY sp.SYMBOL, se.MARKET_TYPE
),
trade_counts AS (
    -- Agentic-only: only trades whose parent proposal came from the Phase 4 board.
    SELECT
        sp.SYMBOL,
        se.MARKET_TYPE,
        COUNT(DISTINCT pt.TRADE_ID)                            AS TRADES_EXECUTED
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
    JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se ON se.SETUP_EVENT_ID = sp.SETUP_EVENT_ID
    LEFT JOIN MIP.APP.PORTFOLIO_TRADES pt ON pt.PROPOSAL_ID = sp.PROPOSAL_ID
    WHERE se.MARKET_TYPE != 'ETF'
      AND pt.TRADE_ID IS NOT NULL
      AND sp.BOARD_RUN_ID IS NOT NULL
    GROUP BY sp.SYMBOL, se.MARKET_TYPE
),
latest_state AS (
    SELECT SYMBOL, MARKET_TYPE, STRUCTURAL_STATE AS DOMINANT_STATE
    FROM MIP.APP.STRUCTURAL_STATE_LOG
    WHERE MARKET_TYPE != 'ETF'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY AS_OF_DATE DESC) = 1
),
latest_regime AS (
    SELECT SYMBOL, MARKET_TYPE, VOL_REGIME, TREND_REGIME, RANGE_REGIME
    FROM MIP.APP.STRUCTURAL_REGIME_TAG
    WHERE MARKET_TYPE != 'ETF'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY AS_OF_DATE DESC) = 1
),
current_thesis AS (
    -- Agentic-only thesis: latest PROPOSED row from the Phase 4 board within the last 5 days.
    SELECT
        sp.SYMBOL,
        se.MARKET_TYPE,
        sp.SETUP_FAMILY || ' ' || sp.DIRECTION AS CURRENT_THESIS
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
    JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se ON se.SETUP_EVENT_ID = sp.SETUP_EVENT_ID
    WHERE sp.STATUS = 'PROPOSED'
      AND sp.CREATED_AT >= DATEADD('day', -5, CURRENT_TIMESTAMP())
      AND se.MARKET_TYPE != 'ETF'
      AND sp.BOARD_RUN_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sp.SYMBOL, se.MARKET_TYPE ORDER BY sp.CREATED_AT DESC) = 1
),
family_trust AS (
    SELECT
        se.SYMBOL,
        se.MARKET_TYPE,
        COUNT(DISTINCT CASE WHEN t.TRUST_LABEL = 'TRUSTED' THEN se.SETUP_FAMILY END)     AS TRUSTED_FAMILIES,
        COUNT(DISTINCT CASE WHEN t.TRUST_LABEL = 'PROVISIONAL' THEN se.SETUP_FAMILY END)  AS PROVISIONAL_FAMILIES,
        COUNT(DISTINCT CASE WHEN t.TRUST_LABEL = 'RESEARCH' THEN se.SETUP_FAMILY END)     AS RESEARCH_FAMILIES
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
    LEFT JOIN MIP.APP.STRUCTURAL_SETUP_TRUST t
      ON t.SETUP_FAMILY = se.SETUP_FAMILY AND t.MARKET_TYPE = se.MARKET_TYPE AND t.EVAL_WINDOW = t.BEST_WINDOW
    WHERE se.MARKET_TYPE != 'ETF'
    GROUP BY se.SYMBOL, se.MARKET_TYPE
),
strongest_fam AS (
    SELECT
        se.SYMBOL,
        se.MARKET_TYPE,
        se.SETUP_FAMILY AS STRONGEST_FAMILY
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
    JOIN MIP.APP.STRUCTURAL_SETUP_TRUST t
      ON t.SETUP_FAMILY = se.SETUP_FAMILY AND t.MARKET_TYPE = se.MARKET_TYPE AND t.EVAL_WINDOW = t.BEST_WINDOW
    WHERE se.MARKET_TYPE != 'ETF' AND t.TRUST_LABEL = 'TRUSTED'
    GROUP BY se.SYMBOL, se.MARKET_TYPE, se.SETUP_FAMILY, t.MEANINGFUL_HIT_RATE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY se.SYMBOL, se.MARKET_TYPE ORDER BY t.MEANINGFUL_HIT_RATE DESC NULLS LAST) = 1
),
weakest_fam AS (
    SELECT
        se.SYMBOL,
        se.MARKET_TYPE,
        se.SETUP_FAMILY AS WEAKEST_FAMILY
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
    JOIN MIP.APP.STRUCTURAL_SETUP_TRUST t
      ON t.SETUP_FAMILY = se.SETUP_FAMILY AND t.MARKET_TYPE = se.MARKET_TYPE AND t.EVAL_WINDOW = t.BEST_WINDOW
    WHERE se.MARKET_TYPE != 'ETF' AND t.TRUST_LABEL != 'REJECTED'
    GROUP BY se.SYMBOL, se.MARKET_TYPE, se.SETUP_FAMILY, t.MEANINGFUL_HIT_RATE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY se.SYMBOL, se.MARKET_TYPE ORDER BY t.MEANINGFUL_HIT_RATE ASC NULLS LAST) = 1
)
SELECT
    sc.SYMBOL,
    sc.MARKET_TYPE,
    sc.TOTAL_SETUPS,
    sc.ELIGIBLE_SETUPS,
    COALESCE(pc.PROPOSALS_CREATED, 0)          AS PROPOSALS_CREATED,
    COALESCE(pc.ACTIVE_PROPOSALS, 0)           AS ACTIVE_PROPOSALS,
    COALESCE(tc.TRADES_EXECUTED, 0)            AS TRADES_EXECUTED,
    sc.LONG_SETUPS,
    sc.SHORT_SETUPS,
    ls.DOMINANT_STATE,
    lr.VOL_REGIME,
    lr.TREND_REGIME,
    lr.RANGE_REGIME,
    ct.CURRENT_THESIS,
    ft.TRUSTED_FAMILIES,
    ft.PROVISIONAL_FAMILIES,
    ft.RESEARCH_FAMILIES,
    sf.STRONGEST_FAMILY,
    wf.WEAKEST_FAMILY
FROM setup_counts sc
LEFT JOIN proposal_counts pc ON pc.SYMBOL = sc.SYMBOL AND pc.MARKET_TYPE = sc.MARKET_TYPE
LEFT JOIN trade_counts tc ON tc.SYMBOL = sc.SYMBOL AND tc.MARKET_TYPE = sc.MARKET_TYPE
LEFT JOIN latest_state ls ON ls.SYMBOL = sc.SYMBOL AND ls.MARKET_TYPE = sc.MARKET_TYPE
LEFT JOIN latest_regime lr ON lr.SYMBOL = sc.SYMBOL AND lr.MARKET_TYPE = sc.MARKET_TYPE
LEFT JOIN current_thesis ct ON ct.SYMBOL = sc.SYMBOL AND ct.MARKET_TYPE = sc.MARKET_TYPE
LEFT JOIN family_trust ft ON ft.SYMBOL = sc.SYMBOL AND ft.MARKET_TYPE = sc.MARKET_TYPE
LEFT JOIN strongest_fam sf ON sf.SYMBOL = sc.SYMBOL AND sf.MARKET_TYPE = sc.MARKET_TYPE
LEFT JOIN weakest_fam wf ON wf.SYMBOL = sc.SYMBOL AND wf.MARKET_TYPE = sc.MARKET_TYPE;

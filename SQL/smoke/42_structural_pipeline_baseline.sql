/*  ================================================================
    42_structural_pipeline_baseline.sql
    Structural Pipeline Remediation Program — Phase 0 Baseline
    ================================================================

    PURPOSE
    -------
    Establish a frozen before-state for the structural proposal pipeline.
    Run ALL sections before any Phase 1+ changes. Re-run the same sections
    after each phase to measure deltas.

    HOW TO RUN
    ----------
    Full baseline:
        cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py \
            -f MIP/SQL/smoke/42_structural_pipeline_baseline.sql

    Single section (1-indexed):
        cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py \
            -f MIP/SQL/smoke/42_structural_pipeline_baseline.sql -s N

    SECTION MAP
    -----------
    1  A-DATA     Inverted zone count by family (pass = 0 post-Phase 1)
    2  A-DATA     Inverted zone detail: SUPPORT_WICK_LONG + RESISTANCE_WICK_SHORT samples
    3  B-FUNNEL   Setup lifecycle waterfall last 30 days
    4  B-FUNNEL   Setup DETECTED → ELIGIBLE → PROPOSED conversion by family (30d)
    5  C-RANK     Proposal family × trust distribution last 30 days
    6  C-RANK     Simulated composite scores: proposals vs missed-symbol setups
    7  C-RANK     Missed-symbol funnel trace (AMD, INTC, MU, PANW, NVDA)
    8  D-COMMIT   Committee verdict × zone-position matrix (all-time structural)
    9  E-QUALITY  Daily proposal counts and family share last 14 days

    BASELINE RECORDED: 2026-04-28
    ----------------------------------------------------------------

    PHASE 0 FINDINGS (recorded at baseline, do not modify):
    --------------------------------------------------------
    A. Data integrity:
       SUPPORT_WICK_LONG:    52 total, 36 inverted (69.2% inverted) — confirmed bug
       RESISTANCE_WICK_SHORT: 51 total, 39 inverted (76.5% inverted) — confirmed bug
       All other families: 0 inverted

    B. Funnel:
       ~50 proposals in last 30 trading days (~2.5/day; pipeline runs ~5/day × 1 portfolio)
       Only 3 families appear in proposals over 30 days out of 8 families detected
       BREAKOUT_RETEST_LONG: 251 detections (largest family), 0 proposals ever

    C. Ranking:
       TREND_PULLBACK_LONG: 74% of proposals (37 of 50)
       THREE_BAR_REVERSAL_SHORT: 16% (8 of 50)
       THREE_BAR_REVERSAL_LONG: 10% (5 of 50)
       Trust weighting: PROVISIONAL families (TREND_PULLBACK_LONG, THREE_BAR_REVERSAL_LONG)
         already dominate because PATH_SURVIVAL_HIT_RATE is 0.704 vs 0.355 for BREAKOUT_RETEST_LONG
       NVDA ELIGIBLE 2026-04-24: scored 0.8301, lost 5th slot to UBER (0.8448) by 0.0147 pts
       PANW ELIGIBLE 2026-04-24: scored 0.8334, lost to UBER by 0.0114 pts
       MU BREAKOUT_RETEST_LONG 2026-04-15: SC=0.958/LS=0.917, scored 0.8641
         — lost 6th slot to CSCO (0.8705) due to PSHR 0.355 vs 0.706 for TPL

    D. Committee:
       INSIDE zone: 6 BLOCK / 8 PROCEED_REDUCED  (43% block rate)
       ABOVE zone:  5 BLOCK / 4 PROCEED_REDUCED  (56% block rate)
       BELOW zone:  1 BLOCK / 0 PROCEED_REDUCED
       Inconsistency: being inside zone is NOT safer than being above zone

    E. Additional confirmed:
       SETUP_DATE >= DATEADD('day',-5) lookback window (line 112 of 520_sp_propose_structural_trades.sql)
         — a DETECTED setup more than 5 days old is excluded even if ELIGIBLE
       TRUST_LABEL filter excludes REJECTED only; TRUSTED/PROVISIONAL/RESEARCH all pass
       No TRUSTED setups exist for any family in live data (all are PROVISIONAL or RESEARCH)

    ================================================================ */


-- =================================================================
-- SECTION 1 (A-DATA): Inverted zone count by family — last 30 days
-- Gate: INVERTED = 0 for all families after Phase 1
-- Baseline: SUPPORT_WICK_LONG=36, RESISTANCE_WICK_SHORT=39, others=0
-- =================================================================
SELECT
    SETUP_FAMILY,
    COUNT(*)                                                               AS TOTAL,
    COUNT_IF(ENTRY_ZONE_HIGH < ENTRY_ZONE_LOW)                            AS INVERTED,
    ROUND(COUNT_IF(ENTRY_ZONE_HIGH < ENTRY_ZONE_LOW)
          / NULLIF(COUNT(*), 0) * 100, 2)                                 AS INVERTED_RATE_PCT
FROM MIP.APP.STRUCTURAL_SETUP_EVENTS
WHERE SETUP_DATE >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY 1
ORDER BY 3 DESC;


-- =================================================================
-- SECTION 2 (A-DATA): Inverted zone sample rows (10 per family)
-- Used to validate the fix in Phase 1: should return 0 rows after
-- =================================================================
SELECT
    SETUP_FAMILY, SYMBOL, SETUP_DATE, SETUP_STATUS,
    ROUND(ENTRY_ZONE_LOW, 2)  AS ZONE_LOW,
    ROUND(ENTRY_ZONE_HIGH, 2) AS ZONE_HIGH,
    ROUND(ENTRY_ZONE_HIGH - ENTRY_ZONE_LOW, 2) AS ZONE_WIDTH
FROM MIP.APP.STRUCTURAL_SETUP_EVENTS
WHERE SETUP_DATE >= DATEADD(day, -30, CURRENT_DATE)
  AND ENTRY_ZONE_HIGH < ENTRY_ZONE_LOW
  AND SETUP_FAMILY IN ('SUPPORT_WICK_LONG', 'RESISTANCE_WICK_SHORT')
ORDER BY SETUP_FAMILY, SETUP_DATE DESC
LIMIT 20;


-- =================================================================
-- SECTION 3 (B-FUNNEL): Setup lifecycle waterfall — last 30 days
-- Used to detect silent stage collapses after each phase
-- Expected: DETECTED > ELIGIBLE > PROPOSED (no sudden drop)
-- =================================================================
SELECT
    SETUP_DATE,
    SETUP_STATUS,
    COUNT(*) AS CNT
FROM MIP.APP.STRUCTURAL_SETUP_EVENTS
WHERE SETUP_DATE >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


-- =================================================================
-- SECTION 4 (B-FUNNEL): Family detection→proposal conversion — last 30 days
-- Baseline: BREAKOUT_RETEST_LONG = 251 detected, 0 proposed
-- Gate post-Phase 2: BREAKOUT_RETEST_LONG should begin converting
-- =================================================================
SELECT
    se.SETUP_FAMILY,
    COUNT(DISTINCT se.SETUP_EVENT_ID)                                      AS DETECTED,
    COUNT(DISTINCT p.PROPOSAL_ID)                                          AS PROPOSED,
    ROUND(COUNT(DISTINCT p.PROPOSAL_ID)
          / NULLIF(COUNT(DISTINCT se.SETUP_EVENT_ID), 0) * 100, 1)        AS CONV_PCT,
    ROUND(AVG(se.STRUCTURE_CONFIDENCE), 3)                                 AS AVG_CONF,
    ROUND(AVG(se.LEVEL_SIGNIFICANCE), 3)                                   AS AVG_LEVEL
FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
       ON p.SETUP_EVENT_ID = se.SETUP_EVENT_ID
WHERE se.SETUP_DATE >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY 1
ORDER BY 2 DESC;


-- =================================================================
-- SECTION 5 (C-RANK): Proposal family × trust distribution — last 30 days
-- Baseline: TREND_PULLBACK_LONG=74%, THREE_BAR_REVERSAL_SHORT=16%,
--           THREE_BAR_REVERSAL_LONG=10%; 0 TRUSTED proposals
-- Gate post-Phase 2: TPL share < 55%, at least 4 families represented
-- =================================================================
SELECT
    p.SETUP_FAMILY,
    COALESCE(t.TRUST_LABEL, 'UNKNOWN')                                     AS TRUST_LABEL,
    COUNT(*)                                                               AS PROPOSALS,
    COUNT(DISTINCT p.SYMBOL)                                               AS SYMBOLS,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 1)                      AS SHARE_PCT
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_TRUST t
       ON t.SETUP_FAMILY = p.SETUP_FAMILY
      AND t.MARKET_TYPE  = 'STOCK'
      AND t.EVAL_WINDOW  = 20
WHERE p.CREATED_AT >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 3 DESC;


-- =================================================================
-- SECTION 6 (C-RANK): Composite score comparison — proposed vs missed
-- Shows what scores were accepted (rank 1-5) vs what was rejected
-- Baseline: NVDA missed by 0.0147; MU BREAKOUT missed by PATH_SURVIVAL
-- =================================================================
WITH scored AS (
    SELECT
        se.SYMBOL,
        se.SETUP_DATE,
        se.SETUP_FAMILY,
        se.SETUP_STATUS,
        COALESCE(t.TRUST_LABEL, 'RESEARCH')                                AS TRUST_LABEL,
        ROUND(
            COALESCE(se.STRUCTURE_CONFIDENCE, 0.5) * 0.30
            + COALESCE(se.LEVEL_SIGNIFICANCE, 0.3) * 0.20
            + CASE WHEN se.REGIME_COMPAT = 'GOOD'    THEN 0.20
                   WHEN se.REGIME_COMPAT = 'NEUTRAL' THEN 0.10
                   ELSE 0.0 END
            + COALESCE(t.MEANINGFUL_HIT_RATE, 0.3)    * 0.20
            + COALESCE(t.PATH_SURVIVAL_HIT_RATE, 0.2) * 0.10
        , 4)                                                               AS OLD_SCORE,
        CASE WHEN p.PROPOSAL_ID IS NOT NULL THEN 'PROPOSED' ELSE 'NOT_PROPOSED' END AS OUTCOME
    FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
    LEFT JOIN MIP.APP.STRUCTURAL_SETUP_TRUST t
           ON t.SETUP_FAMILY = se.SETUP_FAMILY
          AND t.MARKET_TYPE  = se.MARKET_TYPE
          AND t.EVAL_WINDOW  = 20
    LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
           ON p.SETUP_EVENT_ID = se.SETUP_EVENT_ID
    WHERE se.SETUP_DATE >= DATEADD(day, -14, CURRENT_DATE)
      AND se.SYMBOL IN ('AMD','INTC','MU','PANW','NVDA',
                        'ORCL','V','COST','SOFI','SBUX','UBER',
                        'AAPL','PLTR','NUE','JPM','PFE','CAT','RIVN','CLF')
)
SELECT SYMBOL, SETUP_DATE, SETUP_FAMILY, TRUST_LABEL,
       OLD_SCORE, OUTCOME
FROM scored
ORDER BY SETUP_DATE DESC, OLD_SCORE DESC;


-- =================================================================
-- SECTION 7 (C-RANK): Missed-symbol funnel trace — last 30 days
-- Baseline: ALL 5 symbols have PROPOSED_ID = NULL despite strong setups
-- Gate post-Phase 2: at least 2-3 begin appearing as PROPOSED
-- =================================================================
SELECT
    se.SYMBOL,
    se.SETUP_DATE,
    se.SETUP_FAMILY,
    se.SETUP_STATUS,
    ROUND(se.STRUCTURE_CONFIDENCE, 3)   AS STRUCT_CONF,
    ROUND(se.LEVEL_SIGNIFICANCE, 3)     AS LEVEL_SIG,
    se.REGIME_COMPAT,
    COALESCE(t.TRUST_LABEL, 'UNKNOWN')  AS TRUST_LABEL,
    ROUND(t.PATH_SURVIVAL_HIT_RATE, 3)  AS PSHR,
    CASE WHEN p.PROPOSAL_ID IS NULL THEN 'NOT_PROPOSED' ELSE 'PROPOSED' END AS PROPOSED
FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_TRUST t
       ON t.SETUP_FAMILY = se.SETUP_FAMILY
      AND t.MARKET_TYPE  = se.MARKET_TYPE
      AND t.EVAL_WINDOW  = 20
LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
       ON p.SETUP_EVENT_ID = se.SETUP_EVENT_ID
WHERE se.SYMBOL IN ('AMD','INTC','MU','PANW','NVDA')
  AND se.SETUP_DATE >= DATEADD(day, -30, CURRENT_DATE)
ORDER BY se.SYMBOL, se.SETUP_DATE DESC;


-- =================================================================
-- SECTION 8 (D-COMMIT): Committee verdict × zone-position matrix
-- Baseline: INSIDE BLOCK=6, INSIDE PROCEED=8; ABOVE BLOCK=5, ABOVE PROCEED=4
-- Gate post-Phase 4: INSIDE block rate must drop; reason codes required
-- =================================================================
SELECT
    CASE
        WHEN CURRENT_PRICE BETWEEN ENTRY_ZONE_LOW AND ENTRY_ZONE_HIGH THEN 'INSIDE'
        WHEN CURRENT_PRICE > ENTRY_ZONE_HIGH                          THEN 'ABOVE'
        ELSE                                                               'BELOW'
    END                                                                    AS ZONE_POSITION,
    COMMITTEE_VERDICT,
    COUNT(*)                                                               AS CNT,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER (
              PARTITION BY CASE
                  WHEN CURRENT_PRICE BETWEEN ENTRY_ZONE_LOW AND ENTRY_ZONE_HIGH THEN 'INSIDE'
                  WHEN CURRENT_PRICE > ENTRY_ZONE_HIGH                          THEN 'ABOVE'
                  ELSE                                                               'BELOW'
              END) * 100, 1)                                               AS ZONE_BLOCK_RATE_PCT
FROM MIP.LIVE.LIVE_ACTIONS
WHERE LIVE_INTENT_KIND = 'STRUCTURAL'
  AND COMMITTEE_VERDICT IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;


-- =================================================================
-- SECTION 9 (E-QUALITY): Daily proposal counts and family share — last 14 days
-- Baseline: max 5/day, TREND_PULLBACK_LONG dominant most days
-- Gate post-Phase 2: max 8/day, TREND_PULLBACK_LONG share < 55%
-- =================================================================
SELECT
    p.CREATED_AT::DATE                                                     AS PROPOSAL_DATE,
    COUNT(*)                                                               AS TOTAL_PROPOSALS,
    COUNT_IF(p.SETUP_FAMILY = 'TREND_PULLBACK_LONG')                      AS TPL_COUNT,
    ROUND(COUNT_IF(p.SETUP_FAMILY = 'TREND_PULLBACK_LONG')
          / NULLIF(COUNT(*), 0) * 100, 1)                                 AS TPL_PCT,
    COUNT(DISTINCT p.SETUP_FAMILY)                                        AS DISTINCT_FAMILIES,
    LISTAGG(DISTINCT p.SETUP_FAMILY, ' | ')
        WITHIN GROUP (ORDER BY p.SETUP_FAMILY)                            AS FAMILIES_SEEN
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
WHERE p.CREATED_AT >= DATEADD(day, -14, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

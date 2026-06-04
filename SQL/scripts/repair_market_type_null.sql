/* ================================================================
   repair_market_type_null.sql
   Legacy MARKET_TYPE NULL cleanup for execution/proposal-critical rows.

   GOAL
   Execution/proposal-critical rows should no longer rely on
   MARKET_TYPE = NULL being treated as STOCK. NULL market types are
   repaired by inferring the correct value from canonical instrument
   reference data. Only confidently classified rows are auto-updated;
   anything ambiguous is left NULL and surfaced for manual review.

   SCOPE (execution/proposal-critical base tables)
   - MIP.LIVE.LIVE_ACTIONS                -> has MARKET_TYPE, can be NULL (repaired here)
   - MIP.APP.STRUCTURAL_TRADE_PROPOSALS   -> NO MARKET_TYPE column (derives via setup-event join) - nothing to repair
   - MIP.LIVE.LIVE_ORDERS                 -> NO MARKET_TYPE column - nothing to repair
   - MIP.AGENT_OUT.ORDER_PROPOSALS        -> NO MARKET_TYPE column - nothing to repair
   - MIP.APP.STRUCTURAL_SETUP_EVENTS      -> MARKET_TYPE NOT NULL (used as a reference source)
   - PROPOSAL_BOARD_* snapshot tables     -> nullable but per-run/ephemeral; the orchestrator and
                                             SP now always write STOCK/FX/ETF (no NULL observed)

   CANONICAL REFERENCE
   A symbol -> market_type map built from authoritative NOT-NULL sources:
     1. MIP.APP.INGEST_UNIVERSE        (primary ingest universe)
     2. MIP.APP.STRUCTURAL_SETUP_EVENTS (covers FX/ETF symbols not in ingest, e.g. AUD/USD)
     3. MIP.APP.PORTFOLIO_TRADES        (executed-trade history)
   A row is CONFIDENTLY classified only when all sources that know the
   symbol agree on exactly one market type (NMT = 1). Disagreement
   (NMT > 1) or no source (unresolved) is left NULL = REVIEW_REQUIRED.

   This script is idempotent: it only touches rows where MARKET_TYPE IS NULL.
   It does NOT use COALESCE(MARKET_TYPE,'STOCK') as a correctness mechanism.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

-- ── Step 1: Pre-cleanup snapshot — NULL rows and how they resolve ──────────
WITH ref AS (
    SELECT SYM, MAX(MT) AS MT, COUNT(DISTINCT MT) AS NMT
    FROM (
        SELECT UPPER(SYMBOL) AS SYM, MARKET_TYPE AS MT FROM MIP.APP.INGEST_UNIVERSE        WHERE MARKET_TYPE IS NOT NULL
        UNION
        SELECT UPPER(SYMBOL),        MARKET_TYPE        FROM MIP.APP.STRUCTURAL_SETUP_EVENTS WHERE MARKET_TYPE IS NOT NULL
        UNION
        SELECT UPPER(SYMBOL),        MARKET_TYPE        FROM MIP.APP.PORTFOLIO_TRADES        WHERE MARKET_TYPE IS NOT NULL
    )
    GROUP BY SYM
)
SELECT
    'LIVE_ACTIONS' AS TABLE_NAME,
    CASE
        WHEN r.SYM IS NULL THEN 'REVIEW_REQUIRED_UNRESOLVED'
        WHEN r.NMT > 1     THEN 'REVIEW_REQUIRED_CONFLICT'
        ELSE 'RESOLVED_' || r.MT
    END AS RESOLUTION,
    COUNT(*) AS NULL_ROWS,
    COUNT(DISTINCT la.SYMBOL) AS SYMBOLS
FROM MIP.LIVE.LIVE_ACTIONS la
LEFT JOIN ref r ON r.SYM = UPPER(la.SYMBOL)
WHERE la.MARKET_TYPE IS NULL
GROUP BY 1, 2
ORDER BY 2;

-- ── Step 2: Repair LIVE_ACTIONS — confident rows only ─────────────────────
UPDATE MIP.LIVE.LIVE_ACTIONS la
   SET MARKET_TYPE = r.MT
FROM (
    SELECT SYM, MAX(MT) AS MT, COUNT(DISTINCT MT) AS NMT
    FROM (
        SELECT UPPER(SYMBOL) AS SYM, MARKET_TYPE AS MT FROM MIP.APP.INGEST_UNIVERSE        WHERE MARKET_TYPE IS NOT NULL
        UNION
        SELECT UPPER(SYMBOL),        MARKET_TYPE        FROM MIP.APP.STRUCTURAL_SETUP_EVENTS WHERE MARKET_TYPE IS NOT NULL
        UNION
        SELECT UPPER(SYMBOL),        MARKET_TYPE        FROM MIP.APP.PORTFOLIO_TRADES        WHERE MARKET_TYPE IS NOT NULL
    )
    GROUP BY SYM
    HAVING COUNT(DISTINCT MT) = 1
) r
WHERE la.MARKET_TYPE IS NULL
  AND UPPER(la.SYMBOL) = r.SYM;

-- ── Step 3: Post-cleanup — remaining NULLs (must be REVIEW_REQUIRED only) ──
SELECT
    'LIVE_ACTIONS_remaining_null' AS METRIC,
    COUNT(*) AS N
FROM MIP.LIVE.LIVE_ACTIONS
WHERE MARKET_TYPE IS NULL;

-- ── Step 4: Review query — unresolved / conflicting NULL rows ─────────────
WITH ref AS (
    SELECT SYM, MAX(MT) AS MT, COUNT(DISTINCT MT) AS NMT
    FROM (
        SELECT UPPER(SYMBOL) AS SYM, MARKET_TYPE AS MT FROM MIP.APP.INGEST_UNIVERSE        WHERE MARKET_TYPE IS NOT NULL
        UNION
        SELECT UPPER(SYMBOL),        MARKET_TYPE        FROM MIP.APP.STRUCTURAL_SETUP_EVENTS WHERE MARKET_TYPE IS NOT NULL
        UNION
        SELECT UPPER(SYMBOL),        MARKET_TYPE        FROM MIP.APP.PORTFOLIO_TRADES        WHERE MARKET_TYPE IS NOT NULL
    )
    GROUP BY SYM
)
SELECT
    la.ACTION_ID, la.SYMBOL, la.STATUS, la.LIVE_INTENT_KIND, la.PORTFOLIO_ID,
    CASE WHEN r.SYM IS NULL THEN 'UNRESOLVED_NOT_IN_REFERENCE'
         WHEN r.NMT > 1     THEN 'CONFLICT_MULTIPLE_MARKET_TYPES'
         ELSE 'UNEXPECTED' END AS REVIEW_REASON,
    la.CREATED_AT
FROM MIP.LIVE.LIVE_ACTIONS la
LEFT JOIN ref r ON r.SYM = UPPER(la.SYMBOL)
WHERE la.MARKET_TYPE IS NULL
ORDER BY REVIEW_REASON, la.SYMBOL, la.CREATED_AT;

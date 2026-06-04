/* ================================================================
   47_phase4_stock_only_regression.sql
   Phase 4 STOCK-only regression gate.

   The Phase 4 agentic candidate universe is STOCK only. FX/ETF and any
   other explicit non-STOCK market type must be excluded BEFORE the Cortex
   agent panel and must never become live-tradeable. Read-only gate with
   four hard zero-count checks:

     R1  non-STOCK candidates sent to agents (latest run)  -> must be 0
     R2  non-STOCK agent outcomes (latest run)             -> must be 0
     R3  non-STOCK executable structural proposals         -> must be 0
     R4  non-STOCK tradeable LIVE_ACTIONS rows             -> must be 0

   Scoping notes:
   - Canonical board market types are exactly STOCK / FX / ETF (never NULL).
     The board filters on MARKET_TYPE = 'STOCK'.
   - R1/R2 target the LATEST board run, so they verify the first board
     executed AFTER this fix is deployed. (Historical pre-fix runs already
     sent FX/ETF to agents; those rows are immutable history.)
   - R4 treats NULL MARKET_TYPE as STOCK: legacy/operator LIVE_ACTIONS rows
     carry NULL for real US stocks. Only an EXPLICIT non-STOCK value
     (FX, ETF, ...) is a violation. This mirrors the import guard, which
     defaults NULL -> STOCK.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

-- R1: non-STOCK candidates sent to agents in the LATEST board run.
WITH latest AS (
    SELECT RUN_ID
    FROM MIP.APP.PROPOSAL_BOARD_RUN
    ORDER BY STARTED_AT DESC
    LIMIT 1
)
SELECT
    'R1_NON_STOCK_SENT_TO_AGENTS_LATEST_RUN' AS CHECK_NAME,
    COUNT(DISTINCT ao.DOSSIER_ID) AS OBSERVED_COUNT,
    IFF(COUNT(DISTINCT ao.DOSSIER_ID) = 0, 'PASS', 'FAIL') AS RESULT
FROM latest l
JOIN MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 ao
  ON ao.RUN_ID = l.RUN_ID
JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
  ON s.RUN_ID = ao.RUN_ID AND s.DOSSIER_ID = ao.DOSSIER_ID
WHERE COALESCE(s.MARKET_TYPE, 'STOCK') <> 'STOCK';

-- R2: non-STOCK agent outcome rows in the LATEST board run.
WITH latest AS (
    SELECT RUN_ID
    FROM MIP.APP.PROPOSAL_BOARD_RUN
    ORDER BY STARTED_AT DESC
    LIMIT 1
)
SELECT
    'R2_NON_STOCK_AGENT_OUTCOMES_LATEST_RUN' AS CHECK_NAME,
    COUNT(*) AS OBSERVED_COUNT,
    IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS RESULT
FROM latest l
JOIN MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 ao
  ON ao.RUN_ID = l.RUN_ID
JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
  ON s.RUN_ID = ao.RUN_ID AND s.DOSSIER_ID = ao.DOSSIER_ID
WHERE COALESCE(s.MARKET_TYPE, 'STOCK') <> 'STOCK';

-- R3: non-STOCK structural proposals still EXECUTABLE / tradeable (all-time).
-- Market type resolved from board dossier snapshot first, then evidence event.
SELECT
    'R3_NON_STOCK_EXECUTABLE_PROPOSALS' AS CHECK_NAME,
    COUNT(*) AS OBSERVED_COUNT,
    IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS RESULT
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
LEFT JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
  ON s.RUN_ID = p.BOARD_RUN_ID AND s.DOSSIER_ID = p.BOARD_DOSSIER_ID
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se_ev
  ON se_ev.SETUP_EVENT_ID = p.PRIMARY_EVIDENCE_SETUP_EVENT_ID
WHERE p.STATUS = 'PROPOSED'
  AND COALESCE(p.EXECUTION_POLICY_STATUS, 'EXECUTABLE') = 'EXECUTABLE'
  AND COALESCE(p.IS_RESEARCH_ONLY, FALSE) = FALSE
  AND COALESCE(s.MARKET_TYPE, se_ev.MARKET_TYPE, 'STOCK') <> 'STOCK';

-- R4: LIVE_ACTIONS rows with an EXPLICIT non-STOCK market type in a tradeable
-- (non-terminal) state. NULL MARKET_TYPE = legacy stock and is NOT a violation.
SELECT
    'R4_NON_STOCK_TRADEABLE_LIVE_ACTIONS' AS CHECK_NAME,
    COUNT(*) AS OBSERVED_COUNT,
    IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS RESULT
FROM MIP.LIVE.LIVE_ACTIONS la
WHERE la.MARKET_TYPE IS NOT NULL
  AND la.MARKET_TYPE <> 'STOCK'
  AND la.STATUS IN (
      'PROPOSED', 'INTENT_APPROVED', 'PENDING_OPEN_VALIDATION',
      'OPEN_BLOCKED', 'REVALIDATED_PASS', 'EXECUTION_REQUESTED',
      'PENDING_SUBMIT', 'OPEN', 'FILLED'
  );

-- Context: non-STOCK candidates correctly excluded pre-agent in the latest run.
WITH latest AS (
    SELECT RUN_ID
    FROM MIP.APP.PROPOSAL_BOARD_RUN
    ORDER BY STARTED_AT DESC
    LIMIT 1
)
SELECT
    'CTX_NON_STOCK_EXCLUDED_PRE_AGENT_LATEST_RUN' AS CHECK_NAME,
    COUNT(*) AS OBSERVED_COUNT,
    'INFO' AS RESULT
FROM latest l
JOIN MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY e
  ON e.RUN_ID = l.RUN_ID
WHERE e.PRIMARY_REASON_CODE = 'NON_STOCK_EXCLUDED_PRE_AGENT';

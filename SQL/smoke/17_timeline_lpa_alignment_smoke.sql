/* ================================================================
   17_timeline_lpa_alignment_smoke.sql
   Structural Timeline vs LPA-import alignment — smoke checks.

   Validates that the Structural Market Timeline's per-symbol
   "actionable proposals" count matches the set LPA's structural
   importer will actually accept. The mismatch reported on
   2026-05-27 (CRM/ABBV/COIN — 3 in timeline, 1 in LPA) was caused
   by timeline counting all STATUS='PROPOSED' rows including
   research-only ones (EXECUTION_POLICY_STATUS != 'EXECUTABLE' or
   IS_RESEARCH_ONLY = TRUE). The view now splits ACTIVE_PROPOSALS
   into ACTIONABLE_PROPOSALS + RESEARCH_PROPOSALS so the UI can
   show operator-actionable counts that line up with LPA.

   Checks (read-only):
     1) V_STRUCTURAL_TIMELINE_SUMMARY exposes the new columns.
     2) ACTIVE = ACTIONABLE + RESEARCH for every row (algebraic
        invariant — the split must not lose or double-count).
     3) For every symbol, ACTIONABLE_PROPOSALS matches the exact
        predicate the LPA structural importer applies in
        live.py / _STRUCTURAL_PROPOSAL_QUERY (STATUS='PROPOSED'
        AND EXECUTION_POLICY_STATUS='EXECUTABLE' AND NOT
        IS_RESEARCH_ONLY, MARKET_TYPE != 'ETF', agentic BOARD_RUN_ID,
        AND BOARD_RUN_ID in V_LATEST_AUTHORITATIVE_BOARD_RUN).
     4) Symbols currently surfaced with RESEARCH_PROPOSALS > 0 —
        diagnostic listing so operators can see which symbols
        the board produced research-only output for.

   Idempotent. Safe to re-run after each daily pipeline.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE WAREHOUSE MIP_WH_XS;
USE DATABASE MIP;
USE SCHEMA MART;

-- 1. View columns are present.
SELECT 'VIEW_COLUMNS_PRESENT' AS CHECK_NAME, COUNT(*) AS N
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_SCHEMA = 'MART'
   AND TABLE_NAME = 'V_STRUCTURAL_TIMELINE_SUMMARY'
   AND COLUMN_NAME IN ('ACTIONABLE_PROPOSALS', 'RESEARCH_PROPOSALS');
-- Expect 2.

-- 2. Algebraic invariant: ACTIVE = ACTIONABLE + RESEARCH per row.
SELECT 'ACTIVE_SPLIT_BALANCED' AS CHECK_NAME, COUNT(*) AS MISMATCH_ROWS
  FROM MIP.MART.V_STRUCTURAL_TIMELINE_SUMMARY
 WHERE ACTIVE_PROPOSALS <> (COALESCE(ACTIONABLE_PROPOSALS, 0)
                            + COALESCE(RESEARCH_PROPOSALS, 0));
-- Expect 0.

-- 3. Per-symbol ACTIONABLE count must equal the LPA-importer-eligible set
--    (EXECUTABLE + not research-only + authoritative board run).
WITH expected AS (
    SELECT sp.SYMBOL,
           se.MARKET_TYPE,
           COUNT(*) AS EXPECTED_ACTIONABLE
      FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
      JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se
        ON se.SETUP_EVENT_ID = COALESCE(sp.SETUP_EVENT_ID, sp.PRIMARY_EVIDENCE_SETUP_EVENT_ID)
      JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN auth
        ON auth.RUN_ID = sp.BOARD_RUN_ID
     WHERE sp.STATUS = 'PROPOSED'
       AND sp.BOARD_RUN_ID IS NOT NULL
       AND se.MARKET_TYPE != 'ETF'
       AND COALESCE(sp.EXECUTION_POLICY_STATUS, 'EXECUTABLE') = 'EXECUTABLE'
       AND NOT COALESCE(sp.IS_RESEARCH_ONLY, FALSE)
     GROUP BY sp.SYMBOL, se.MARKET_TYPE
)
SELECT 'TIMELINE_LPA_ACTIONABLE_ALIGNED' AS CHECK_NAME,
       COUNT(*) AS MISMATCH_COUNT,
       ARRAY_AGG(OBJECT_CONSTRUCT(
           'symbol',           v.SYMBOL,
           'market_type',      v.MARKET_TYPE,
           'view_actionable',  COALESCE(v.ACTIONABLE_PROPOSALS, 0),
           'expected_actionable', COALESCE(e.EXPECTED_ACTIONABLE, 0)
       )) WITHIN GROUP (ORDER BY v.SYMBOL) AS MISMATCHES
  FROM MIP.MART.V_STRUCTURAL_TIMELINE_SUMMARY v
  LEFT JOIN expected e
    ON e.SYMBOL = v.SYMBOL AND e.MARKET_TYPE = v.MARKET_TYPE
 WHERE COALESCE(v.ACTIONABLE_PROPOSALS, 0) <> COALESCE(e.EXPECTED_ACTIONABLE, 0);
-- Expect MISMATCH_COUNT = 0.

-- 4. Diagnostic: proposals LPA cannot import (policy-blocked or non-authoritative run).
SELECT
    'RESEARCH_ONLY_PROPOSALS_NOW' AS CHECK_NAME,
    sp.SYMBOL,
    sp.PROPOSAL_ID,
    sp.DIRECTION,
    sp.EXECUTION_POLICY_STATUS,
    sp.EXECUTION_POLICY_REASON,
    sp.IS_RESEARCH_ONLY,
    sp.CREATED_AT
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
  LEFT JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN auth
    ON auth.RUN_ID = sp.BOARD_RUN_ID
 WHERE sp.STATUS = 'PROPOSED'
   AND sp.BOARD_RUN_ID IS NOT NULL
   AND (
        COALESCE(sp.EXECUTION_POLICY_STATUS, 'EXECUTABLE') != 'EXECUTABLE'
        OR COALESCE(sp.IS_RESEARCH_ONLY, FALSE)
        OR auth.RUN_ID IS NULL
   )
 ORDER BY sp.CREATED_AT DESC, sp.SYMBOL
 LIMIT 50;

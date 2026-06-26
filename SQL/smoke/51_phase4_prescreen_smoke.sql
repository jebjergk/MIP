-- 51_phase4_prescreen_smoke.sql
-- Read-only checks for Phase 4 structural pre-screen (eligibility + cap).
-- Expect tighter eligibility than "pass all 63 universe symbols".

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

-- S51-A: Dossier open-position context must use live broker book, not sim orphans.
SELECT
    'S51-A live open positions in dossier' AS CHECK_NAME,
    COUNT(*) AS SYMBOLS_WITH_OPEN_POS
FROM MIP.MART.V_PROPOSAL_BOARD_SYMBOL_DOSSIER d
WHERE UPPER(d.MARKET_TYPE) = 'STOCK'
  AND COALESCE(
        d.DOSSIER_PAYLOAD_JSON:memory:open_position_context:open_position_count::INT,
        0
      ) > 0;

-- S51-B: Sim book should not be the only open-position source (14-symbol orphan set).
SELECT
    'S51-B sim PORTFOLIO_POSITIONS STOCK symbols' AS CHECK_NAME,
    COUNT(DISTINCT SYMBOL) AS SIM_OPEN_SYMBOLS
FROM MIP.APP.PORTFOLIO_POSITIONS
WHERE UPPER(MARKET_TYPE) = 'STOCK'
  AND ABS(QUANTITY) > 0;

-- S51-C: Latest board run should show cost-cap skips separate from genuine blocks.
SELECT
    'S51-C latest run eligibility mix' AS CHECK_NAME,
    r.RUN_ID,
    SUM(IFF(e.ELIGIBLE, 1, 0)) AS ELIGIBLE_ROWS,
    SUM(IFF(e.PRIMARY_REASON_CODE = 'NOT_SENT_TO_AGENT_PANEL_COST_CAP', 1, 0)) AS COST_CAPPED,
    SUM(IFF(NOT e.ELIGIBLE AND e.PRIMARY_REASON_CODE != 'NOT_SENT_TO_AGENT_PANEL_COST_CAP', 1, 0)) AS GENUINE_BLOCKED,
    TRY_TO_NUMBER(r.MODEL_CONFIG_JSON:max_candidates::STRING) AS MAX_CANDIDATES
FROM MIP.APP.PROPOSAL_BOARD_RUN r
JOIN MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY e ON e.RUN_ID = r.RUN_ID
WHERE r.STARTED_AT >= DATEADD('day', -3, CURRENT_TIMESTAMP())
GROUP BY r.RUN_ID, r.MODEL_CONFIG_JSON:max_candidates
ORDER BY r.RUN_ID DESC
LIMIT 3;

-- ============================================================
-- Smoke: 42_phase2b_real_readonly_smoke.sql
-- Phase 2B — Real IBKR Account Read-Only Linkage
-- ============================================================

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

-- 1. Both portfolios exist with correct modes
SELECT PORTFOLIO_ID, IBKR_ACCOUNT_ID, IBKR_ACCOUNT_MODE, ADAPTER_MODE,
       IS_ACTIVE, IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED,
       IB_GATEWAY_HOST, IB_GATEWAY_PORT, IB_CLIENT_ID
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
ORDER BY PORTFOLIO_ID;
-- Expected: 2 rows. Paper (DUQ101771, PAPER, exec=true, real_money=false).
--           Real (U24621464, REAL, exec=false, real_money=false).

-- 2. Uniqueness: at most one active config per IBKR_ACCOUNT_ID
SELECT IBKR_ACCOUNT_ID, COUNT(*) AS ACTIVE_CONFIGS
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
WHERE IS_ACTIVE = TRUE
GROUP BY IBKR_ACCOUNT_ID
HAVING COUNT(*) > 1;
-- Expected: 0 rows.

-- 3. Real account safety flags are locked correctly
SELECT PORTFOLIO_ID, IBKR_ACCOUNT_ID,
       IFF(IS_EXECUTION_ENABLED = FALSE, 'OK', 'FAIL: IS_EXECUTION_ENABLED must be FALSE') AS EXEC_GATE,
       IFF(REAL_MONEY_ENABLED   = FALSE, 'OK', 'FAIL: REAL_MONEY_ENABLED must be FALSE')  AS REAL_MONEY_GATE
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
WHERE IBKR_ACCOUNT_MODE = 'REAL';
-- Expected: EXEC_GATE=OK, REAL_MONEY_GATE=OK for all REAL rows.

-- 4. Paper account execution still enabled
SELECT PORTFOLIO_ID, IBKR_ACCOUNT_ID,
       IFF(IS_EXECUTION_ENABLED = TRUE, 'OK', 'WARN: paper execution disabled') AS PAPER_EXEC
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
WHERE IBKR_ACCOUNT_MODE = 'PAPER';
-- Expected: PAPER_EXEC=OK.

-- 5. Per-portfolio connection params on real account
SELECT PORTFOLIO_ID, IBKR_ACCOUNT_ID,
       IFF(IB_GATEWAY_PORT = 7496, 'OK', 'FAIL: expected port 7496') AS PORT_CHECK,
       IFF(IB_CLIENT_ID    = 9403, 'OK', 'FAIL: expected client_id 9403') AS CLIENT_ID_CHECK,
       IFF(IB_CLIENT_ID   <> (SELECT IB_CLIENT_ID
                              FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                              WHERE IBKR_ACCOUNT_MODE = 'PAPER'
                                AND IB_CLIENT_ID IS NOT NULL
                              LIMIT 1)
           OR (SELECT IB_CLIENT_ID
               FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
               WHERE IBKR_ACCOUNT_MODE = 'PAPER'
                 AND IB_CLIENT_ID IS NOT NULL
               LIMIT 1) IS NULL,
           'OK', 'FAIL: client_id collision with paper') AS CLIENT_ID_COLLISION_CHECK
FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
WHERE IBKR_ACCOUNT_MODE = 'REAL';
-- Expected: PORT_CHECK=OK, CLIENT_ID_CHECK=OK, CLIENT_ID_COLLISION_CHECK=OK.

-- 6. No LIVE_ACTIONS rows exist under the real portfolio (none created yet)
SELECT COUNT(*) AS REAL_ACCOUNT_ACTIONS
FROM MIP.LIVE.LIVE_ACTIONS la
JOIN MIP.LIVE.LIVE_PORTFOLIO_CONFIG lpc ON lpc.PORTFOLIO_ID = la.PORTFOLIO_ID
WHERE lpc.IBKR_ACCOUNT_MODE = 'REAL';
-- Expected: 0 (no actions for real account in Phase 2B).

-- 7. Cross-account snapshot isolation: all BROKER_SNAPSHOTS rows have non-null IBKR_ACCOUNT_ID
SELECT COUNT(*) AS NULL_ACCOUNT_SNAPSHOTS
FROM MIP.LIVE.BROKER_SNAPSHOTS
WHERE IBKR_ACCOUNT_ID IS NULL;
-- Expected: 0.

-- 8. Paper actions still scoped to paper portfolio (regression check)
SELECT COUNT(*) AS PAPER_ACTIONS
FROM MIP.LIVE.LIVE_ACTIONS la
JOIN MIP.LIVE.LIVE_PORTFOLIO_CONFIG lpc ON lpc.PORTFOLIO_ID = la.PORTFOLIO_ID
WHERE lpc.IBKR_ACCOUNT_MODE = 'PAPER';
-- Expected: > 0 (existing paper actions remain).

-- 9. Re-run Phase 1 safety summary (no regression)
SELECT 'PHASE1_SAFETY' AS TEST_SUITE,
       (SELECT COUNT(*) FROM MIP.LIVE.LIVE_ACTIONS
        WHERE BROKER_NAME IS NULL OR IBKR_ACCOUNT_ID IS NULL OR BROKER_UNIVERSE_TYPE IS NULL
       ) AS INCOMPLETE_FROZEN_CTX,
       (SELECT COUNT(*) FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
        WHERE BROKER_NAME IS NULL
       ) AS NULL_BROKER_NAME_CONFIGS,
       (SELECT COUNT(*) FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
        WHERE IBKR_ACCOUNT_MODE NOT IN ('PAPER', 'REAL')
          AND IS_ACTIVE = TRUE
       ) AS UNKNOWN_ACCOUNT_MODE_CONFIGS;
-- Expected: all 0.

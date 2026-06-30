-- Portfolio 2 real-money: align BUST_PCT with paper portfolio 1.
-- BUST_PCT=0 was capping stop_loss_pct to zero via min(sl, bust), which
-- blocked bracket seeding after agentic APPROVE (LIVE_SL_REQUIRED_MISSING).

UPDATE MIP.LIVE.LIVE_PORTFOLIO_CONFIG
   SET BUST_PCT = 0.2
 WHERE PORTFOLIO_ID = 2
   AND COALESCE(BUST_PCT, 0) <= 0;

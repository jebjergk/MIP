use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();



-- ============================================================
-- CLEANUP: Remove bad data from today's failed pipeline run
-- Run this BEFORE redeploying the fixed procedures
-- ============================================================

-- Step 1: Delete simulation-created DUPLICATE positions (wrong entry/hold_until)
delete from MIP.APP.PORTFOLIO_POSITIONS
where PORTFOLIO_ID = 1
  and RUN_ID = '1d36cefb-617b-4f75-ba9d-b525c4da7c28';
-- Expected: 3 rows deleted (KO, JNJ, PG with entry_index 119, hold_until 120)

-- Step 2: Delete ALL trades from today's simulation run
delete from MIP.APP.PORTFOLIO_TRADES
where PORTFOLIO_ID = 1
  and RUN_ID = '1d36cefb-617b-4f75-ba9d-b525c4da7c28';
-- Expected: deletes simulation-created BUY/SELL/re-BUY trades

-- Step 3: Delete the AUD/USD trade from the failed pipeline
delete from MIP.APP.PORTFOLIO_TRADES
where TRADE_ID = 4519;
-- Expected: 1 row (AUD/USD BUY from failed pipeline d87a4e70)

-- Step 4: Delete any AUD/USD position from the failed pipeline
delete from MIP.APP.PORTFOLIO_POSITIONS
where PORTFOLIO_ID = 1
  and SYMBOL = 'AUD/USD'
  and CREATED_AT >= '2026-02-07';

-- Step 5: Delete PORTFOLIO_DAILY from today's simulation
delete from MIP.APP.PORTFOLIO_DAILY
where PORTFOLIO_ID = 1
  and RUN_ID = '1d36cefb-617b-4f75-ba9d-b525c4da7c28';

-- Step 6: Reset PORTFOLIO to point to no simulation run
-- (next pipeline will recalculate from episode start)
update MIP.APP.PORTFOLIO
set LAST_SIMULATION_RUN_ID = null,
    LAST_SIMULATED_AT = null,
    UPDATED_AT = current_timestamp()
where PORTFOLIO_ID = 1;

-- Step 7: Verify - should only show 3 agent positions (JNJ, PG, KO with hold_until 126)
select * from MIP.APP.PORTFOLIO_POSITIONS where PORTFOLIO_ID = 1;

-- Step 8: Verify - should show yesterday's BUY trades only
select TRADE_ID, SYMBOL, SIDE, TRADE_TS, PRICE, QUANTITY, RUN_ID, PROPOSAL_ID
from MIP.APP.PORTFOLIO_TRADES
where PORTFOLIO_ID = 1
order by TRADE_TS desc;
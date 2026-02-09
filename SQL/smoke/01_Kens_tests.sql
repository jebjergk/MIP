use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


select * from mip.app.pattern_definition;



-- 1. Redeploy the SP (run 180_sp_run_portfolio_simulation.sql)

-- 2. Clean + test
delete from MIP.APP.PORTFOLIO_DAILY where PORTFOLIO_ID = 2;
update MIP.APP.PORTFOLIO set LAST_SIMULATION_RUN_ID = null where PORTFOLIO_ID = 2;
call MIP.APP.SP_RUN_PORTFOLIO_SIMULATION(2, '2026-02-03'::timestamp_ntz, '2026-02-06'::timestamp_ntz);

-- 3. Check — EQUITY_VALUE should now be > 0
select TS, CASH, EQUITY_VALUE, TOTAL_EQUITY, OPEN_POSITIONS
from MIP.APP.PORTFOLIO_DAILY where PORTFOLIO_ID = 2 order by TS;
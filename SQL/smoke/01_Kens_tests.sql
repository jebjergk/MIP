use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


-- Delete the 3 orphaned trades from run 1
delete from MIP.APP.PORTFOLIO_TRADES
 where PORTFOLIO_ID = 2
   and TRADE_ID in (4504, 4505, 4506);

-- Fix CASH_AFTER for the remaining 3 trades
MERGE INTO MIP.APP.PORTFOLIO_TRADES t
USING (
    with ranked as (
        select TRADE_ID, SIDE, NOTIONAL,
               greatest(coalesce(0.50, 0), abs(NOTIONAL) * 2 / 10000) as FEE,
               row_number() over (order by TRADE_TS, TRADE_ID) as rn
        from MIP.APP.PORTFOLIO_TRADES
        where PORTFOLIO_ID = 2
    ),
    cumulative as (
        select TRADE_ID,
               2000 - sum(
                   case when SIDE = 'BUY' then NOTIONAL + FEE
                        when SIDE = 'SELL' then -(NOTIONAL - FEE)
                        else 0
                   end
               ) over (order by rn rows between unbounded preceding and current row) as NEW_CASH_AFTER
        from ranked
    )
    select TRADE_ID, NEW_CASH_AFTER from cumulative
) c
ON t.TRADE_ID = c.TRADE_ID
WHEN MATCHED THEN UPDATE SET t.CASH_AFTER = c.NEW_CASH_AFTER;

-- Verify: should show 3 trades with correct cumulative cash
select TRADE_ID, SYMBOL, SIDE, NOTIONAL, CASH_AFTER
from MIP.APP.PORTFOLIO_TRADES
where PORTFOLIO_ID = 2
order by TRADE_TS, TRADE_ID;
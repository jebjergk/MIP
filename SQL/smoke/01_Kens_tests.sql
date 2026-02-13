use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


select * from mip.app.pattern_definition;

select
    PORTFOLIO_ID,
    EPISODE_ID,
    START_TS,
    END_TS,
    STATUS,
    END_REASON,
    START_EQUITY
from MIP.APP.PORTFOLIO_EPISODE
where PORTFOLIO_ID in (1, 2)
order by PORTFOLIO_ID, START_TS;

select
    TRADE_ID,
    PORTFOLIO_ID,
    EPISODE_ID,
    SYMBOL,
    SIDE,
    TRADE_TS,
    PRICE,
    QUANTITY,
    CASH_AFTER,
    PROPOSAL_ID
from MIP.APP.PORTFOLIO_TRADES
where EPISODE_ID is null
  and PORTFOLIO_ID in (1, 2)
order by PORTFOLIO_ID, TRADE_TS;
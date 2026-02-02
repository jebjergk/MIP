use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();

select count(*) as positions
from MIP.APP.PORTFOLIO_POSITIONS
where portfolio_id = 1;

select max(trade_ts) 
from MIP.APP.PORTFOLIO_TRADES
where portfolio_id = 1 limit 1;

select count(*) as trades
from MIP.APP.PORTFOLIO_TRADES
where portfolio_id = 1;

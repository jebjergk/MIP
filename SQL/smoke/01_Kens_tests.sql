use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


select * from MIP.MART.V_BAR_INDEX where bar_index = 57 order by ts desc;

select * from mip.app.portfolio_episode;

select * from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL;


select * from mip.app.portfolio_trades order by trade_ts desc;
  select * from MIP.APP.PORTFOLIO_POSITIONS order by hold_until_index;

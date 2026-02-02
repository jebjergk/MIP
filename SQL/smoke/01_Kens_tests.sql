use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


select
  portfolio_id,
  run_id,
  symbol,
  market_type,
  entry_ts,
  quantity,
  case
    when quantity > 0 then 'BUY'
    when quantity < 0 then 'SELL'
    else 'FLAT'
  end as side,
  entry_price,
  cost_basis
from MIP.APP.PORTFOLIO_POSITIONS
where portfolio_id = 1
order by entry_ts desc;







  desc table MIP.APP.PORTFOLIO_POSITIONS;

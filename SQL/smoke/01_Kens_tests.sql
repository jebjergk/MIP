use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();

select *
from MIP.APP.PORTFOLIO_POSITIONS
where portfolio_id = 1;

desc table MIP.APP.recommendation_outcomes;
 limit 1;

select *
from MIP.APP.PORTFOLIO
where portfolio_id = 1;

select *
from MIP.APP.PORTFOLIO_PROFILE
where portfolio_id = 1;

select
  side,
  count(*) as n
from MIP.<schema>.<table>
group by 1
order by n desc;

SELECT PORTFOLIO_ID, PROFILE_ID, NAME
FROM MIP.APP.PORTFOLIO
WHERE PORTFOLIO_ID = 1;

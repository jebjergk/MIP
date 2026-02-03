-- portfolio_2_smoke.sql
-- Smoke checks after running scripts/bootstrap_portfolio_2.sql.
-- Expect: two portfolios; Portfolio #2 has one ACTIVE episode with PROFILE_ID = 2 (LOW_RISK).

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) All portfolios (expect at least 2; second named PORTFOLIO_2_LOW_RISK)
select * from MIP.APP.PORTFOLIO order by PORTFOLIO_ID;

-- 2) ACTIVE episodes for both portfolios (expect one row per active portfolio)
select * from MIP.APP.PORTFOLIO_EPISODE
where STATUS = 'ACTIVE'
order by PORTFOLIO_ID;

-- 3) Portfolio #2 active episode must have PROFILE_ID = 2 (LOW_RISK)
select EPISODE_ID, PORTFOLIO_ID, PROFILE_ID, START_TS, STATUS
from MIP.APP.PORTFOLIO_EPISODE e
join MIP.APP.PORTFOLIO p on p.PORTFOLIO_ID = e.PORTFOLIO_ID
where p.NAME = 'PORTFOLIO_2_LOW_RISK' and e.STATUS = 'ACTIVE';
-- Expect: one row, PROFILE_ID = 2

-- 4) Pipeline scoping: both active portfolios appear in run scope (no filter excludes #2)
select PORTFOLIO_ID, NAME, STATUS from MIP.APP.PORTFOLIO where STATUS = 'ACTIVE' order by PORTFOLIO_ID;

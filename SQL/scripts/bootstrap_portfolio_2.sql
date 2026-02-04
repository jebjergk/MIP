-- bootstrap_portfolio_2.sql
-- Purpose: Idempotent activation of a second portfolio (Portfolio #2) with PROFILE 2 (LOW_RISK)
-- and its initial ACTIVE episode. Safe to rerun: does not duplicate portfolio or episode.
-- Prereqs: 160_app_portfolio_tables.sql, 168_portfolio_episode.sql (and 169 if START_EQUITY used).
-- Profiles: 1 PRIVATE_SAVINGS (unchanged), 2 LOW_RISK, 3 HIGH_RISK.
-- After run: smoke/portfolio_2_smoke.sql verifies two portfolios and active episode for #2.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Step 1: Insert Portfolio #2 if missing (match by name to avoid duplicates).
-- Explicit PORTFOLIO_ID = 2 so the second portfolio gets id 2; Snowflake autoincrement
-- can otherwise produce large gaps (e.g. 201). If you already ran this and got 201,
-- delete that portfolio (and its episode) then re-run to get id 2:
--   delete from MIP.APP.PORTFOLIO_EPISODE where PORTFOLIO_ID = 201;
--   delete from MIP.APP.PORTFOLIOaber  where PORTFOLIO_ID = 201;
insert into MIP.APP.PORTFOLIO (
    PORTFOLIO_ID,
    PROFILE_ID,
    NAME,
    BASE_CURRENCY,
    STARTING_CASH,
    STATUS,
    NOTES
)
select
    2,                              -- PORTFOLIO_ID = 2 (second portfolio)
    2,                              -- PROFILE_ID = LOW_RISK
    'PORTFOLIO_2_LOW_RISK',
    'USD',
    100000,
    'ACTIVE',
    'Second operational portfolio; demo / compare path. Bootstrapped by bootstrap_portfolio_2.sql.'
from (select 1 as dummy) t
where not exists (
    select 1 from MIP.APP.PORTFOLIO
    where NAME = 'PORTFOLIO_2_LOW_RISK'
);

-- Step 2: Start initial ACTIVE episode for Portfolio #2 if it has none.
-- Uses canonical SP_START_PORTFOLIO_EPISODE; PROFILE_ID 2 (LOW_RISK) is set on portfolio and episode.
execute immediate $$
declare
    v_portfolio_id number;
    v_has_active number;
begin
    select PORTFOLIO_ID into :v_portfolio_id
      from MIP.APP.PORTFOLIO
     where NAME = 'PORTFOLIO_2_LOW_RISK'
     limit 1;

    if (v_portfolio_id is null) then
        return;
    end if;

    select count(*) into :v_has_active
      from MIP.APP.PORTFOLIO_EPISODE
     where PORTFOLIO_ID = :v_portfolio_id
       and STATUS = 'ACTIVE';

    if (v_has_active > 0) then
        return;
    end if;

    call MIP.APP.SP_START_PORTFOLIO_EPISODE(:v_portfolio_id, 2, 'INITIALIZE');
end;
$$;

-- Verification (optional): uncomment to print.
-- select * from MIP.APP.PORTFOLIO where NAME = 'PORTFOLIO_2_LOW_RISK';
-- select * from MIP.APP.PORTFOLIO_EPISODE where PORTFOLIO_ID = (select PORTFOLIO_ID from MIP.APP.PORTFOLIO where NAME = 'PORTFOLIO_2_LOW_RISK' limit 1) and STATUS = 'ACTIVE';

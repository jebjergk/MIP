-- morning_brief_persist_smoke.sql
-- Smoke test for persisted morning brief merge

set portfolio_id = (
    select PORTFOLIO_ID
    from MIP.APP.PORTFOLIO
    where STATUS = 'ACTIVE'
    order by PORTFOLIO_ID
    limit 1
);

call MIP.APP.SP_WRITE_MORNING_BRIEF($portfolio_id, 'SMOKE_TEST');

with latest as (
    select BRIEF:attribution:latest_run_id::string as run_id
    from MIP.MART.V_MORNING_BRIEF_JSON
    where PORTFOLIO_ID = $portfolio_id
)
select count(*) as ROW_COUNT
from MIP.AGENT_OUT.MORNING_BRIEF mb
join latest
  on mb.PORTFOLIO_ID = $portfolio_id
 and mb.RUN_ID = latest.run_id;

-- morning_brief_persist_smoke.sql
-- Smoke test for persisted morning brief merge

call MIP.APP.SP_WRITE_MORNING_BRIEF(1, 'SMOKE_TEST');

with latest as (
    select BRIEF:attribution:latest_run_id::string as run_id
    from MIP.MART.V_MORNING_BRIEF_JSON
)
select count(*) as ROW_COUNT
from MIP.AGENT_OUT.MORNING_BRIEF mb
join latest
  on mb.PORTFOLIO_ID = 1
 and mb.RUN_ID = latest.run_id;

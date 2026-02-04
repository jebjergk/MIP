-- morning_brief_persist_smoke.sql
-- Smoke test for persisted morning brief merge (deterministic key: portfolio_id, as_of_ts, run_id, agent_name)

set portfolio_id = (
    select PORTFOLIO_ID
    from MIP.APP.PORTFOLIO
    where STATUS = 'ACTIVE'
    order by PORTFOLIO_ID
    limit 1
);
set as_of_ts = (select max(ts)::timestamp_ntz from MIP.MART.MARKET_BARS where interval_minutes = 1440);
set run_id = 'SMOKE_TEST_PERSIST';

call MIP.APP.SP_WRITE_MORNING_BRIEF($portfolio_id, $as_of_ts, $run_id);

select count(*) as ROW_COUNT
from MIP.AGENT_OUT.MORNING_BRIEF mb
where mb.PORTFOLIO_ID = $portfolio_id
  and mb.AS_OF_TS = $as_of_ts
  and mb.RUN_ID = $run_id
  and coalesce(mb.AGENT_NAME, '') = 'MORNING_BRIEF';

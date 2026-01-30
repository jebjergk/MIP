-- smoke_pipeline_idempotency.sql
-- Smoke test: run SP_RUN_DAILY_PIPELINE twice (same day, same portfolios), then assert no duplicate rows
-- for the same run_id in MORNING_BRIEF and ORDER_PROPOSALS. Expect 0 rows from duplicate-check queries.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Run pipeline first time
call MIP.APP.SP_RUN_DAILY_PIPELINE();

-- 2) Run pipeline second time (idempotent: rowcounts should not increase for same natural keys)
call MIP.APP.SP_RUN_DAILY_PIPELINE();

-- 3) Capture the two most recent pipeline run IDs (run_id_2 = latest, run_id_1 = previous)
set run_id_2 = (
    select RUN_ID
    from MIP.APP.MIP_AUDIT_LOG
    where EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
      and EVENT_TYPE = 'PIPELINE'
    order by EVENT_TS desc
    limit 1
);
set run_id_1 = (
    select RUN_ID
    from MIP.APP.MIP_AUDIT_LOG
    where EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
      and EVENT_TYPE = 'PIPELINE'
    order by EVENT_TS desc
    limit 1 offset 1
);

-- 4) Duplicate check: MORNING_BRIEF — expect 0 rows for each run_id
-- Duplicates in brief (by portfolio_id, as_of_ts, run_id, agent_name)
select 'RUN_1_BRIEF' as check_label, portfolio_id, as_of_ts, run_id, agent_name, count(*) as n
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id = $run_id_1
group by portfolio_id, as_of_ts, run_id, agent_name
having count(*) > 1;
-- Expect 0 rows.

select 'RUN_2_BRIEF' as check_label, portfolio_id, as_of_ts, run_id, agent_name, count(*) as n
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id = $run_id_2
group by portfolio_id, as_of_ts, run_id, agent_name
having count(*) > 1;
-- Expect 0 rows.

-- 5) Duplicate check: ORDER_PROPOSALS — expect 0 rows for each run_id (canonical key: RUN_ID_VARCHAR)
-- Duplicates in proposals (by run_id, portfolio_id, symbol, side)
select 'RUN_1_PROPOSALS' as check_label, run_id_varchar, portfolio_id, symbol, side, count(*) as n
from MIP.AGENT_OUT.ORDER_PROPOSALS
where run_id_varchar = $run_id_1
group by run_id_varchar, portfolio_id, symbol, side
having count(*) > 1;
-- Expect 0 rows.

select 'RUN_2_PROPOSALS' as check_label, run_id_varchar, portfolio_id, symbol, side, count(*) as n
from MIP.AGENT_OUT.ORDER_PROPOSALS
where run_id_varchar = $run_id_2
group by run_id_varchar, portfolio_id, symbol, side
having count(*) > 1;
-- Expect 0 rows.

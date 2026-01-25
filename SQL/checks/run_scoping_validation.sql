-- run_scoping_validation.sql
-- Purpose: Validate run scoping integrity across proposals, executions, and pipeline runs

use role MIP_ADMIN_ROLE;
use database MIP;

-- Set variables for filtering (null = check all)
set v_run_id = null;
set v_portfolio_id = null;

-- Check 1: Validate ORDER_PROPOSALS.RUN_ID matches pipeline run IDs from audit log
-- Expect 0: proposals with run IDs that don't exist in pipeline audit entries
select
    count(*) as orphaned_proposal_run_ids
from MIP.AGENT_OUT.ORDER_PROPOSALS p
left join MIP.APP.MIP_AUDIT_LOG a
  on a.RUN_ID = to_varchar(p.RUN_ID)
  and a.EVENT_TYPE = 'PIPELINE'
  and a.EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
where p.RUN_ID is not null
  and (:v_run_id is null or p.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
  and a.RUN_ID is null;

-- Check 2: Validate ORDER_PROPOSALS.SIGNAL_RUN_ID matches signal generation runs
-- Expect 0: proposals with signal run IDs that don't match recommendation log run IDs
select
    count(*) as invalid_signal_run_ids
from MIP.AGENT_OUT.ORDER_PROPOSALS p
where p.SIGNAL_RUN_ID is not null
  and (:v_run_id is null or p.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
  and not exists (
      select 1
      from MIP.APP.RECOMMENDATION_LOG r
      where (
          r.RUN_ID = p.SIGNAL_RUN_ID
          or try_to_number(replace(r.RUN_ID, 'T', '')) = try_to_number(replace(p.SIGNAL_RUN_ID, 'T', ''))
      )
      and r.RECOMMENDATION_ID = p.RECOMMENDATION_ID
  );

-- Check 3: Validate PORTFOLIO_TRADES.RUN_ID matches proposal run IDs
-- Expect 0: trades with run IDs that don't match their proposal's run ID
select
    count(*) as trade_proposal_run_id_mismatch
from MIP.APP.PORTFOLIO_TRADES t
join MIP.AGENT_OUT.ORDER_PROPOSALS p
  on p.PROPOSAL_ID = t.PROPOSAL_ID
where (:v_run_id is null or t.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or t.PORTFOLIO_ID = :v_portfolio_id)
  and (
      to_varchar(t.RUN_ID) != to_varchar(p.RUN_ID)
      or t.RUN_ID is null
      or p.RUN_ID is null
  );

-- Check 4: Verify proposal-to-execution linkage consistency
-- Expect 0: executed proposals without corresponding trades
select
    count(*) as executed_proposals_without_trades
from MIP.AGENT_OUT.ORDER_PROPOSALS p
left join MIP.APP.PORTFOLIO_TRADES t
  on t.PROPOSAL_ID = p.PROPOSAL_ID
where p.STATUS = 'EXECUTED'
  and (:v_run_id is null or p.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
  and t.PROPOSAL_ID is null;

-- Check 5: Verify trades without corresponding proposals
-- Expect 0: trades without valid proposal linkage
select
    count(*) as trades_without_proposals
from MIP.APP.PORTFOLIO_TRADES t
left join MIP.AGENT_OUT.ORDER_PROPOSALS p
  on p.PROPOSAL_ID = t.PROPOSAL_ID
where (:v_run_id is null or t.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or t.PORTFOLIO_ID = :v_portfolio_id)
  and p.PROPOSAL_ID is null;

-- Check 6: Validate run ID consistency across pipeline steps
-- Expect consistent run IDs: all proposals/executions for a pipeline run should share the same run ID
select
    p.RUN_ID,
    count(distinct p.RUN_ID) as distinct_run_ids,
    count(*) as proposal_count
from MIP.AGENT_OUT.ORDER_PROPOSALS p
where (:v_run_id is null or p.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
group by p.RUN_ID
having count(distinct p.RUN_ID) > 1;

-- Summary: Run scoping integrity summary
select
    'RUN_SCOPING_VALIDATION' as check_name,
    count(distinct p.RUN_ID) as distinct_pipeline_run_ids,
    count(distinct p.SIGNAL_RUN_ID) as distinct_signal_run_ids,
    count(*) as total_proposals,
    count(distinct t.RUN_ID) as distinct_trade_run_ids,
    count(distinct t.PROPOSAL_ID) as total_executed_trades
from MIP.AGENT_OUT.ORDER_PROPOSALS p
left join MIP.APP.PORTFOLIO_TRADES t
  on t.PROPOSAL_ID = p.PROPOSAL_ID
where (:v_run_id is null or p.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id);

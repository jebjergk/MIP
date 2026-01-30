-- run_scoping_validation.sql
-- Purpose: Validate run scoping integrity across proposals, executions, and pipeline runs

use role MIP_ADMIN_ROLE;
use database MIP;

-- Set variables for filtering (null = check all)
set v_run_id = null;
set v_portfolio_id = null;

-- Check 1: Validate ORDER_PROPOSALS.RUN_ID_VARCHAR matches pipeline run IDs from audit log (canonical RUN_ID only)
-- Expect 0: proposals with run IDs that don't exist in pipeline audit entries
select
    count(*) as orphaned_proposal_run_ids
from MIP.AGENT_OUT.ORDER_PROPOSALS p
left join MIP.APP.MIP_AUDIT_LOG a
  on a.RUN_ID = p.RUN_ID_VARCHAR
  and a.EVENT_TYPE = 'PIPELINE'
  and a.EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
where p.RUN_ID_VARCHAR is not null
  and (:v_run_id is null or p.RUN_ID_VARCHAR = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
  and a.RUN_ID is null;

-- Check 2: SIGNAL_RUN_ID is optional/legacy; no production scoping on SIGNAL_RUN_ID (removed)

-- Check 3: Validate PORTFOLIO_TRADES.RUN_ID matches proposal RUN_ID_VARCHAR
-- Expect 0: trades with run IDs that don't match their proposal's run ID
select
    count(*) as trade_proposal_run_id_mismatch
from MIP.APP.PORTFOLIO_TRADES t
join MIP.AGENT_OUT.ORDER_PROPOSALS p
  on p.PROPOSAL_ID = t.PROPOSAL_ID
where (:v_run_id is null or to_varchar(t.RUN_ID) = :v_run_id)
  and (:v_portfolio_id is null or t.PORTFOLIO_ID = :v_portfolio_id)
  and (
      to_varchar(t.RUN_ID) != p.RUN_ID_VARCHAR
      or t.RUN_ID is null
      or p.RUN_ID_VARCHAR is null
  );

-- Check 4: Verify proposal-to-execution linkage consistency
-- Expect 0: executed proposals without corresponding trades
select
    count(*) as executed_proposals_without_trades
from MIP.AGENT_OUT.ORDER_PROPOSALS p
left join MIP.APP.PORTFOLIO_TRADES t
  on t.PROPOSAL_ID = p.PROPOSAL_ID
where p.STATUS = 'EXECUTED'
  and (:v_run_id is null or p.RUN_ID_VARCHAR = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
  and t.PROPOSAL_ID is null;

-- Check 5: Verify trades without corresponding proposals
-- Expect 0: trades without valid proposal linkage
select
    count(*) as trades_without_proposals
from MIP.APP.PORTFOLIO_TRADES t
left join MIP.AGENT_OUT.ORDER_PROPOSALS p
  on p.PROPOSAL_ID = t.PROPOSAL_ID
where (:v_run_id is null or to_varchar(t.RUN_ID) = :v_run_id)
  and (:v_portfolio_id is null or t.PORTFOLIO_ID = :v_portfolio_id)
  and p.PROPOSAL_ID is null;

-- Check 6: Validate run ID consistency (canonical RUN_ID_VARCHAR)
select
    p.RUN_ID_VARCHAR,
    count(distinct p.RUN_ID_VARCHAR) as distinct_run_ids,
    count(*) as proposal_count
from MIP.AGENT_OUT.ORDER_PROPOSALS p
where (:v_run_id is null or p.RUN_ID_VARCHAR = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
group by p.RUN_ID_VARCHAR
having count(distinct p.RUN_ID_VARCHAR) > 1;

-- Summary: Run scoping integrity (RUN_ID only; SIGNAL_RUN_ID not used for scoping)
select
    'RUN_SCOPING_VALIDATION' as check_name,
    count(distinct p.RUN_ID_VARCHAR) as distinct_pipeline_run_ids,
    count(*) as total_proposals,
    count(distinct to_varchar(t.RUN_ID)) as distinct_trade_run_ids,
    count(distinct t.PROPOSAL_ID) as total_executed_trades
from MIP.AGENT_OUT.ORDER_PROPOSALS p
left join MIP.APP.PORTFOLIO_TRADES t
  on t.PROPOSAL_ID = p.PROPOSAL_ID
where (:v_run_id is null or p.RUN_ID_VARCHAR = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id);

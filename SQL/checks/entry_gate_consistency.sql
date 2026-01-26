-- entry_gate_consistency.sql
-- Purpose: Validate entry gate enforcement consistency across proposals, executions, and portfolio state

use role MIP_ADMIN_ROLE;
use database MIP;

-- Set variables for filtering (null = check all)
set v_run_id = null;
set v_portfolio_id = null;

-- Check 1: When ENTRIES_BLOCKED=true, verify no new BUY proposals were created
-- Expect 0: BUY proposals created when entry gate was active
select
    count(*) as buy_proposals_during_block
from MIP.AGENT_OUT.ORDER_PROPOSALS p
join MIP.MART.V_PORTFOLIO_RISK_STATE g
  on g.PORTFOLIO_ID = p.PORTFOLIO_ID
where p.SIDE = 'BUY'
  and (:v_run_id is null or p.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
  and g.ENTRIES_BLOCKED = true
  and p.PROPOSED_AT >= coalesce(g.DRAWDOWN_STOP_TS, current_timestamp() - interval '7 days');

-- Check 2: When ENTRIES_BLOCKED=true, verify no BUY executions occurred
-- Expect 0: BUY trades executed when entry gate was active
select
    count(*) as buy_executions_during_block
from MIP.APP.PORTFOLIO_TRADES t
join MIP.MART.V_PORTFOLIO_RISK_STATE g
  on g.PORTFOLIO_ID = t.PORTFOLIO_ID
join MIP.AGENT_OUT.ORDER_PROPOSALS p
  on p.PROPOSAL_ID = t.PROPOSAL_ID
where t.SIDE = 'BUY'
  and (:v_run_id is null or t.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or t.PORTFOLIO_ID = :v_portfolio_id)
  and g.ENTRIES_BLOCKED = true
  and t.TRADE_TS >= coalesce(g.DRAWDOWN_STOP_TS, current_timestamp() - interval '7 days');

-- Check 3: Verify SELL/EXIT trades are allowed when entry gate is active
-- This is informational - SELL trades should be allowed during drawdown stops
select
    count(*) as sell_executions_during_block,
    sum(t.NOTIONAL) as total_sell_notional
from MIP.APP.PORTFOLIO_TRADES t
join MIP.MART.V_PORTFOLIO_RISK_STATE g
  on g.PORTFOLIO_ID = t.PORTFOLIO_ID
where t.SIDE = 'SELL'
  and (:v_run_id is null or t.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or t.PORTFOLIO_ID = :v_portfolio_id)
  and g.ENTRIES_BLOCKED = true
  and t.TRADE_TS >= coalesce(g.DRAWDOWN_STOP_TS, current_timestamp() - interval '7 days');

-- Check 4: Cross-reference V_PORTFOLIO_RISK_STATE with actual proposal counts
-- Verify that when entry gate is active, proposal counts reflect the block
select
    g.PORTFOLIO_ID,
    g.ENTRIES_BLOCKED,
    g.BLOCK_REASON,
    g.DRAWDOWN_STOP_TS,
    count(distinct p.PROPOSAL_ID) as proposals_after_stop,
    count(distinct case when p.SIDE = 'BUY' then p.PROPOSAL_ID end) as buy_proposals_after_stop,
    count(distinct case when p.SIDE = 'SELL' then p.PROPOSAL_ID end) as sell_proposals_after_stop
from MIP.MART.V_PORTFOLIO_RISK_STATE g
left join MIP.AGENT_OUT.ORDER_PROPOSALS p
  on p.PORTFOLIO_ID = g.PORTFOLIO_ID
  and p.PROPOSED_AT >= coalesce(g.DRAWDOWN_STOP_TS, current_timestamp() - interval '7 days')
where g.ENTRIES_BLOCKED = true
  and (:v_portfolio_id is null or g.PORTFOLIO_ID = :v_portfolio_id)
group by
    g.PORTFOLIO_ID,
    g.ENTRIES_BLOCKED,
    g.BLOCK_REASON,
    g.DRAWDOWN_STOP_TS;

-- Check 5: Validate BLOCK_REASON consistency
-- Ensure BLOCK_REASON matches the actual portfolio state
select
    g.PORTFOLIO_ID,
    g.BLOCK_REASON,
    g.DRAWDOWN_STOP_TS,
    g.MAX_DRAWDOWN,
    g.DRAWDOWN_STOP_PCT,
    case
        when g.DRAWDOWN_STOP_TS is not null
         and g.MAX_DRAWDOWN >= g.DRAWDOWN_STOP_PCT
        then 'DRAWDOWN_STOP_ACTIVE'
        else null
    end as expected_block_reason,
    case
        when g.BLOCK_REASON != expected_block_reason then 'INCONSISTENT'
        else 'CONSISTENT'
    end as consistency_status
from MIP.MART.V_PORTFOLIO_RISK_STATE g
where g.ENTRIES_BLOCKED = true
  and (:v_portfolio_id is null or g.PORTFOLIO_ID = :v_portfolio_id)
  and g.BLOCK_REASON != case
      when g.DRAWDOWN_STOP_TS is not null
       and g.MAX_DRAWDOWN >= g.DRAWDOWN_STOP_PCT
      then 'DRAWDOWN_STOP_ACTIVE'
      else null
  end;

-- Check 6: Verify SP_AGENT_PROPOSE_TRADES audit log entries for SKIP_ENTRIES_BLOCKED
-- Ensure the procedure correctly logs when it skips due to entry gate
select
    count(*) as skip_entries_blocked_audit_entries,
    min(EVENT_TS) as first_skip,
    max(EVENT_TS) as last_skip
from MIP.APP.MIP_AUDIT_LOG
where EVENT_TYPE = 'AGENT'
  and EVENT_NAME = 'SP_AGENT_PROPOSE_TRADES'
  and STATUS = 'SKIP_ENTRIES_BLOCKED'
  and (:v_run_id is null or RUN_ID = :v_run_id);

-- Summary: Entry gate enforcement summary
select
    'ENTRY_GATE_CONSISTENCY' as check_name,
    count(distinct g.PORTFOLIO_ID) as portfolios_with_active_gates,
    count(distinct case when g.ENTRIES_BLOCKED then g.PORTFOLIO_ID end) as portfolios_blocked,
    count(distinct p.PROPOSAL_ID) as total_proposals_during_blocks,
    count(distinct case when p.SIDE = 'BUY' and g.ENTRIES_BLOCKED then p.PROPOSAL_ID end) as buy_proposals_during_blocks,
    count(distinct case when p.SIDE = 'SELL' and g.ENTRIES_BLOCKED then p.PROPOSAL_ID end) as sell_proposals_during_blocks
from MIP.MART.V_PORTFOLIO_RISK_STATE g
left join MIP.AGENT_OUT.ORDER_PROPOSALS p
  on p.PORTFOLIO_ID = g.PORTFOLIO_ID
  and p.PROPOSED_AT >= coalesce(g.DRAWDOWN_STOP_TS, current_timestamp() - interval '7 days')
where (:v_portfolio_id is null or g.PORTFOLIO_ID = :v_portfolio_id);

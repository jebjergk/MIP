-- integrity_checks.sql
-- Purpose: Comprehensive integrity validation for MIP system components

use role MIP_ADMIN_ROLE;
use database MIP;

-- Set variables for filtering (null = check all)
set v_run_id = null;
set v_portfolio_id = null;

-- Check 1: Run scoping validation
-- Verify proposals/executions are scoped to correct pipeline run IDs
-- (Detailed checks in run_scoping_validation.sql)

-- Check 2: Entry gate consistency
-- Ensure entries_blocked=true prevents all new proposals/executions
-- (Detailed checks in entry_gate_consistency.sql)

-- Check 3: Signal linkage integrity
-- Validate all proposals link to eligible signals
-- Extend existing order_proposals_signal_linkage_check.sql
select
    'SIGNAL_LINKAGE' as check_name,
    count(*) as invalid_approved_or_executed
from MIP.AGENT_OUT.ORDER_PROPOSALS p
left join MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
  on s.RECOMMENDATION_ID = p.RECOMMENDATION_ID
where p.STATUS in ('APPROVED', 'EXECUTED')
  and (:v_run_id is null or p.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
  and (s.RECOMMENDATION_ID is null or s.IS_ELIGIBLE = false);

-- Check 4: Drawdown stop enforcement
-- Verify portfolio simulation, proposals, and executions all respect drawdown stops
select
    'DRAWDOWN_STOP_ENFORCEMENT' as check_name,
    count(distinct g.PORTFOLIO_ID) as portfolios_with_drawdown_stops,
    count(distinct case when g.ENTRIES_BLOCKED and g.BLOCK_REASON = 'DRAWDOWN_STOP_ACTIVE' then g.PORTFOLIO_ID end) as portfolios_with_active_stops,
    count(distinct case when g.ENTRIES_BLOCKED then p.PROPOSAL_ID end) as proposals_during_stops,
    count(distinct case when g.ENTRIES_BLOCKED and p.SIDE = 'BUY' then p.PROPOSAL_ID end) as buy_proposals_during_stops
from MIP.MART.V_PORTFOLIO_RISK_STATE g
left join MIP.AGENT_OUT.ORDER_PROPOSALS p
  on p.PORTFOLIO_ID = g.PORTFOLIO_ID
  and p.PROPOSED_AT >= coalesce(g.DRAWDOWN_STOP_TS, current_timestamp() - interval '7 days')
where (:v_portfolio_id is null or g.PORTFOLIO_ID = :v_portfolio_id);

-- Check 5: Idempotency checks
-- Ensure reruns don't create duplicate trades/positions
-- Check for duplicate proposals (same run_id, portfolio_id, recommendation_id)
select
    'IDEMPOTENCY_PROPOSALS' as check_name,
    count(*) as duplicate_proposals
from (
    select
        RUN_ID,
        PORTFOLIO_ID,
        RECOMMENDATION_ID,
        count(*) as proposal_count
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where (:v_run_id is null or RUN_ID = :v_run_id)
      and (:v_portfolio_id is null or PORTFOLIO_ID = :v_portfolio_id)
      and RECOMMENDATION_ID is not null
    group by RUN_ID, PORTFOLIO_ID, RECOMMENDATION_ID
    having count(*) > 1
);

-- Check for duplicate trades (same proposal_id should only have one trade)
select
    'IDEMPOTENCY_TRADES' as check_name,
    count(*) as duplicate_trades
from (
    select
        PROPOSAL_ID,
        count(*) as trade_count
    from MIP.APP.PORTFOLIO_TRADES
    where (:v_run_id is null or RUN_ID = :v_run_id)
      and (:v_portfolio_id is null or PORTFOLIO_ID = :v_portfolio_id)
    group by PROPOSAL_ID
    having count(*) > 1
);

-- Check 6: Data freshness
-- Validate market bars, returns, and recommendations are within expected time windows
select
    'DATA_FRESHNESS' as check_name,
    max(mb.TS) as latest_market_bar_ts,
    max(mr.TS) as latest_return_ts,
    max(rl.GENERATED_AT) as latest_recommendation_ts,
    datediff('hour', max(mb.TS), current_timestamp()) as hours_since_latest_bar,
    datediff('hour', max(mr.TS), current_timestamp()) as hours_since_latest_return,
    datediff('hour', max(rl.GENERATED_AT), current_timestamp()) as hours_since_latest_recommendation
from MIP.MART.MARKET_BARS mb
cross join (
    select max(TS) as TS from MIP.MART.MARKET_RETURNS
) mr
cross join (
    select max(GENERATED_AT) as GENERATED_AT from MIP.APP.RECOMMENDATION_LOG
) rl;

-- Check 7: Portfolio simulation consistency
-- Verify portfolio daily records match portfolio trades
select
    'PORTFOLIO_SIMULATION_CONSISTENCY' as check_name,
    count(distinct pd.PORTFOLIO_ID) as portfolios_with_daily_records,
    count(distinct pt.PORTFOLIO_ID) as portfolios_with_trades,
    count(distinct case when pd.PORTFOLIO_ID is not null and pt.PORTFOLIO_ID is null then pd.PORTFOLIO_ID end) as portfolios_daily_without_trades,
    count(distinct case when pt.PORTFOLIO_ID is not null and pd.PORTFOLIO_ID is null then pt.PORTFOLIO_ID end) as portfolios_trades_without_daily
from MIP.APP.PORTFOLIO_DAILY pd
full outer join MIP.APP.PORTFOLIO_TRADES pt
  on pt.PORTFOLIO_ID = pd.PORTFOLIO_ID
  and pt.RUN_ID = pd.RUN_ID
where (:v_run_id is null or coalesce(pd.RUN_ID, pt.RUN_ID) = :v_run_id)
  and (:v_portfolio_id is null or coalesce(pd.PORTFOLIO_ID, pt.PORTFOLIO_ID) = :v_portfolio_id);

-- Check 8: Audit log completeness
-- Verify all major pipeline steps have audit log entries
select
    'AUDIT_LOG_COMPLETENESS' as check_name,
    count(distinct case when EVENT_NAME = 'SP_RUN_DAILY_PIPELINE' then RUN_ID end) as pipeline_runs_logged,
    count(distinct case when EVENT_NAME = 'SP_AGENT_PROPOSE_TRADES' then RUN_ID end) as proposal_runs_logged,
    count(distinct case when EVENT_NAME = 'SP_VALIDATE_AND_EXECUTE_PROPOSALS' then RUN_ID end) as validation_runs_logged
from MIP.APP.MIP_AUDIT_LOG
where (:v_run_id is null or RUN_ID = :v_run_id);

-- Summary: Overall integrity status
select
    'INTEGRITY_CHECK_SUMMARY' as check_name,
    current_timestamp() as check_timestamp,
    count(distinct p.RUN_ID) as distinct_pipeline_runs,
    count(distinct p.PORTFOLIO_ID) as distinct_portfolios,
    count(distinct p.PROPOSAL_ID) as total_proposals,
    count(distinct t.PROPOSAL_ID) as total_executed_trades,
    count(distinct mb.PIPELINE_RUN_ID) as distinct_brief_runs
from MIP.AGENT_OUT.ORDER_PROPOSALS p
left join MIP.APP.PORTFOLIO_TRADES t
  on t.PROPOSAL_ID = p.PROPOSAL_ID
left join MIP.AGENT_OUT.MORNING_BRIEF mb
  on mb.PIPELINE_RUN_ID = p.RUN_ID
  and mb.PORTFOLIO_ID = p.PORTFOLIO_ID
where (:v_run_id is null or p.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id);

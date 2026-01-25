-- morning_brief_consistency.sql
-- Purpose: Validate morning brief reflects actual executed decisions and matches proposal/execution counts

use role MIP_ADMIN_ROLE;
use database MIP;

-- Set variables for filtering (null = check all)
set v_run_id = null;
set v_portfolio_id = null;

-- Check 1: Verify morning brief proposal counts match ORDER_PROPOSALS table
-- Expect 0: briefs with proposal counts that don't match actual proposals
with brief_proposals as (
    select
        mb.PIPELINE_RUN_ID,
        mb.PORTFOLIO_ID,
        mb.BRIEF_JSON:proposals:proposed_count::number as brief_proposed_count,
        mb.BRIEF_JSON:proposals:approved_count::number as brief_approved_count,
        mb.BRIEF_JSON:proposals:rejected_count::number as brief_rejected_count,
        mb.BRIEF_JSON:proposals:executed_count::number as brief_executed_count
    from MIP.AGENT_OUT.MORNING_BRIEF mb
    where (:v_run_id is null or mb.PIPELINE_RUN_ID = :v_run_id)
      and (:v_portfolio_id is null or mb.PORTFOLIO_ID = :v_portfolio_id)
),
actual_proposals as (
    select
        p.RUN_ID as PIPELINE_RUN_ID,
        p.PORTFOLIO_ID,
        count(*) as actual_proposed_count,
        count_if(p.STATUS = 'APPROVED' or p.STATUS = 'EXECUTED') as actual_approved_count,
        count_if(p.STATUS = 'REJECTED') as actual_rejected_count,
        count_if(p.STATUS = 'EXECUTED') as actual_executed_count
    from MIP.AGENT_OUT.ORDER_PROPOSALS p
    where (:v_run_id is null or p.RUN_ID = :v_run_id)
      and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
    group by p.RUN_ID, p.PORTFOLIO_ID
)
select
    bp.PIPELINE_RUN_ID,
    bp.PORTFOLIO_ID,
    bp.brief_proposed_count,
    ap.actual_proposed_count,
    bp.brief_approved_count,
    ap.actual_approved_count,
    bp.brief_rejected_count,
    ap.actual_rejected_count,
    bp.brief_executed_count,
    ap.actual_executed_count,
    case
        when bp.brief_proposed_count != ap.actual_proposed_count then 'PROPOSED_COUNT_MISMATCH'
        when bp.brief_approved_count != ap.actual_approved_count then 'APPROVED_COUNT_MISMATCH'
        when bp.brief_rejected_count != ap.actual_rejected_count then 'REJECTED_COUNT_MISMATCH'
        when bp.brief_executed_count != ap.actual_executed_count then 'EXECUTED_COUNT_MISMATCH'
        else 'CONSISTENT'
    end as consistency_status
from brief_proposals bp
join actual_proposals ap
  on ap.PIPELINE_RUN_ID = bp.PIPELINE_RUN_ID
  and ap.PORTFOLIO_ID = bp.PORTFOLIO_ID
where bp.brief_proposed_count != ap.actual_proposed_count
   or bp.brief_approved_count != ap.actual_approved_count
   or bp.brief_rejected_count != ap.actual_rejected_count
   or bp.brief_executed_count != ap.actual_executed_count;

-- Check 2: Verify brief risk status matches V_PORTFOLIO_RISK_GATE
-- Expect consistent risk status between brief and risk gate view
select
    mb.PIPELINE_RUN_ID,
    mb.PORTFOLIO_ID,
    mb.BRIEF_JSON:risk:status::string as brief_risk_status,
    g.RISK_STATUS as gate_risk_status,
    g.ENTRIES_BLOCKED,
    g.BLOCK_REASON,
    case
        when mb.BRIEF_JSON:risk:status::string != g.RISK_STATUS then 'RISK_STATUS_MISMATCH'
        when mb.BRIEF_JSON:risk:entries_blocked::boolean != g.ENTRIES_BLOCKED then 'ENTRIES_BLOCKED_MISMATCH'
        else 'CONSISTENT'
    end as consistency_status
from MIP.AGENT_OUT.MORNING_BRIEF mb
join MIP.MART.V_PORTFOLIO_RISK_GATE g
  on g.PORTFOLIO_ID = mb.PORTFOLIO_ID
where (:v_run_id is null or mb.PIPELINE_RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or mb.PORTFOLIO_ID = :v_portfolio_id)
  and (
      mb.BRIEF_JSON:risk:status::string != g.RISK_STATUS
      or mb.BRIEF_JSON:risk:entries_blocked::boolean != g.ENTRIES_BLOCKED
  );

-- Check 3: Verify brief signal summaries match V_SIGNALS_ELIGIBLE_TODAY for the run
-- Expect brief signal counts to match eligible signals for the signal run ID
with brief_signals as (
    select
        mb.PIPELINE_RUN_ID,
        mb.PORTFOLIO_ID,
        mb.BRIEF_JSON:signals:trusted_count::number as brief_trusted_count,
        mb.BRIEF_JSON:signals:watch_count::number as brief_watch_count,
        mb.BRIEF_JSON:signals:eligible_count::number as brief_eligible_count,
        mb.BRIEF_JSON:signals:signal_run_id::string as brief_signal_run_id
    from MIP.AGENT_OUT.MORNING_BRIEF mb
    where (:v_run_id is null or mb.PIPELINE_RUN_ID = :v_run_id)
      and (:v_portfolio_id is null or mb.PORTFOLIO_ID = :v_portfolio_id)
),
actual_signals as (
    select
        s.RUN_ID as SIGNAL_RUN_ID,
        count_if(s.TRUST_LABEL = 'TRUSTED') as actual_trusted_count,
        count_if(s.TRUST_LABEL = 'WATCH') as actual_watch_count,
        count_if(s.IS_ELIGIBLE = true) as actual_eligible_count
    from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
    where (:v_run_id is null or try_to_number(replace(s.RUN_ID, 'T', '')) = try_to_number(replace(:v_run_id, 'T', '')))
    group by s.RUN_ID
)
select
    bs.PIPELINE_RUN_ID,
    bs.PORTFOLIO_ID,
    bs.brief_signal_run_id,
    bs.brief_trusted_count,
    asig.actual_trusted_count,
    bs.brief_watch_count,
    asig.actual_watch_count,
    bs.brief_eligible_count,
    asig.actual_eligible_count,
    case
        when bs.brief_trusted_count != asig.actual_trusted_count then 'TRUSTED_COUNT_MISMATCH'
        when bs.brief_watch_count != asig.actual_watch_count then 'WATCH_COUNT_MISMATCH'
        when bs.brief_eligible_count != asig.actual_eligible_count then 'ELIGIBLE_COUNT_MISMATCH'
        else 'CONSISTENT'
    end as consistency_status
from brief_signals bs
left join actual_signals asig
  on (
      asig.SIGNAL_RUN_ID = bs.brief_signal_run_id
      or try_to_number(replace(asig.SIGNAL_RUN_ID, 'T', '')) = try_to_number(replace(bs.brief_signal_run_id, 'T', ''))
  )
where bs.brief_trusted_count != coalesce(asig.actual_trusted_count, 0)
   or bs.brief_watch_count != coalesce(asig.actual_watch_count, 0)
   or bs.brief_eligible_count != coalesce(asig.actual_eligible_count, 0);

-- Check 4: Verify brief reflects actual executed decisions (not just proposals)
-- Ensure executed trades in brief match PORTFOLIO_TRADES
with brief_executions as (
    select
        mb.PIPELINE_RUN_ID,
        mb.PORTFOLIO_ID,
        mb.BRIEF_JSON:executions:total_count::number as brief_execution_count,
        mb.BRIEF_JSON:executions:buy_count::number as brief_buy_count,
        mb.BRIEF_JSON:executions:sell_count::number as brief_sell_count
    from MIP.AGENT_OUT.MORNING_BRIEF mb
    where (:v_run_id is null or mb.PIPELINE_RUN_ID = :v_run_id)
      and (:v_portfolio_id is null or mb.PORTFOLIO_ID = :v_portfolio_id)
),
actual_executions as (
    select
        t.RUN_ID as PIPELINE_RUN_ID,
        t.PORTFOLIO_ID,
        count(*) as actual_execution_count,
        count_if(t.SIDE = 'BUY') as actual_buy_count,
        count_if(t.SIDE = 'SELL') as actual_sell_count
    from MIP.APP.PORTFOLIO_TRADES t
    where (:v_run_id is null or t.RUN_ID = :v_run_id)
      and (:v_portfolio_id is null or t.PORTFOLIO_ID = :v_portfolio_id)
    group by t.RUN_ID, t.PORTFOLIO_ID
)
select
    be.PIPELINE_RUN_ID,
    be.PORTFOLIO_ID,
    be.brief_execution_count,
    ae.actual_execution_count,
    be.brief_buy_count,
    ae.actual_buy_count,
    be.brief_sell_count,
    ae.actual_sell_count,
    case
        when be.brief_execution_count != ae.actual_execution_count then 'EXECUTION_COUNT_MISMATCH'
        when be.brief_buy_count != ae.actual_buy_count then 'BUY_COUNT_MISMATCH'
        when be.brief_sell_count != ae.actual_sell_count then 'SELL_COUNT_MISMATCH'
        else 'CONSISTENT'
    end as consistency_status
from brief_executions be
join actual_executions ae
  on ae.PIPELINE_RUN_ID = be.PIPELINE_RUN_ID
  and ae.PORTFOLIO_ID = be.PORTFOLIO_ID
where be.brief_execution_count != ae.actual_execution_count
   or be.brief_buy_count != ae.actual_buy_count
   or be.brief_sell_count != ae.actual_sell_count;

-- Check 5: Verify brief includes run ID for traceability
-- Expect all briefs to have pipeline run ID
select
    count(*) as briefs_without_run_id
from MIP.AGENT_OUT.MORNING_BRIEF mb
where (:v_run_id is null or mb.PIPELINE_RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or mb.PORTFOLIO_ID = :v_portfolio_id)
  and mb.PIPELINE_RUN_ID is null;

-- Summary: Morning brief consistency summary
select
    'MORNING_BRIEF_CONSISTENCY' as check_name,
    count(distinct mb.PIPELINE_RUN_ID) as distinct_brief_runs,
    count(distinct mb.PORTFOLIO_ID) as distinct_portfolios,
    min(mb.CREATED_AT) as earliest_brief,
    max(mb.CREATED_AT) as latest_brief
from MIP.AGENT_OUT.MORNING_BRIEF mb
where (:v_run_id is null or mb.PIPELINE_RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or mb.PORTFOLIO_ID = :v_portfolio_id);

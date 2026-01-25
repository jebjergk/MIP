-- 191_sp_monitor_autonomy_safety.sql
-- Purpose: Monitor autonomy safety status and return comprehensive safety assessment

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_MONITOR_AUTONOMY_SAFETY(
    P_RUN_ID string default null,
    P_PORTFOLIO_ID number default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, nullif(current_query_tag(), ''), uuid_string());
    v_safety_checks array := array_construct();
    v_check_result variant;
    v_overall_status string := 'SAFE';
    v_fail_count number := 0;
    v_warn_count number := 0;
    v_pass_count number := 0;
    v_summary variant;
begin
    -- Check 1: Entry gate enforcement
    begin
        select count(*)
          into v_check_result
          from MIP.AGENT_OUT.ORDER_PROPOSALS p
          join MIP.MART.V_PORTFOLIO_RISK_GATE g
            on g.PORTFOLIO_ID = p.PORTFOLIO_ID
          where p.SIDE = 'BUY'
            and (:P_RUN_ID is null or p.RUN_ID = try_to_number(replace(:P_RUN_ID, 'T', '')))
            and (:P_PORTFOLIO_ID is null or p.PORTFOLIO_ID = :P_PORTFOLIO_ID)
            and g.ENTRIES_BLOCKED = true
            and p.PROPOSED_AT >= coalesce(g.DRAWDOWN_STOP_TS, current_timestamp() - interval '7 days')
            and p.STATUS in ('APPROVED', 'EXECUTED');

        if (v_check_result > 0) then
            v_check_result := object_construct(
                'check_name', 'ENTRY_GATE_ENFORCEMENT',
                'status', 'FAIL',
                'details', object_construct('buy_proposals_during_block', :v_check_result)
            );
            v_fail_count := :v_fail_count + 1;
            v_overall_status := 'UNSAFE';
        else
            v_check_result := object_construct(
                'check_name', 'ENTRY_GATE_ENFORCEMENT',
                'status', 'PASS'
            );
            v_pass_count := :v_pass_count + 1;
        end if;
        v_safety_checks := array_append(:v_safety_checks, :v_check_result);
    exception
        when other then
            v_check_result := object_construct(
                'check_name', 'ENTRY_GATE_ENFORCEMENT',
                'status', 'FAIL',
                'error', :sqlerrm
            );
            v_safety_checks := array_append(:v_safety_checks, :v_check_result);
            v_fail_count := :v_fail_count + 1;
            v_overall_status := 'UNSAFE';
    end;

    -- Check 2: Run scoping integrity
    begin
        select count(*)
          into v_check_result
          from MIP.AGENT_OUT.ORDER_PROPOSALS p
          left join MIP.APP.MIP_AUDIT_LOG a
            on a.RUN_ID = to_varchar(p.RUN_ID)
            and a.EVENT_TYPE = 'PIPELINE'
          where p.RUN_ID is not null
            and (:P_RUN_ID is null or p.RUN_ID = try_to_number(replace(:P_RUN_ID, 'T', '')))
            and (:P_PORTFOLIO_ID is null or p.PORTFOLIO_ID = :P_PORTFOLIO_ID)
            and a.RUN_ID is null;

        if (v_check_result > 0) then
            v_check_result := object_construct(
                'check_name', 'RUN_SCOPING_INTEGRITY',
                'status', 'WARN',
                'details', object_construct('orphaned_proposals', :v_check_result)
            );
            v_warn_count := :v_warn_count + 1;
            if (v_overall_status = 'SAFE') then
                v_overall_status := 'WARN';
            end if;
        else
            v_check_result := object_construct(
                'check_name', 'RUN_SCOPING_INTEGRITY',
                'status', 'PASS'
            );
            v_pass_count := :v_pass_count + 1;
        end if;
        v_safety_checks := array_append(:v_safety_checks, :v_check_result);
    exception
        when other then
            v_check_result := object_construct(
                'check_name', 'RUN_SCOPING_INTEGRITY',
                'status', 'WARN',
                'error', :sqlerrm
            );
            v_safety_checks := array_append(:v_safety_checks, :v_check_result);
            v_warn_count := :v_warn_count + 1;
    end;

    -- Check 3: Idempotency (no duplicate proposals)
    begin
        select count(*)
          into v_check_result
          from (
              select RUN_ID, PORTFOLIO_ID, RECOMMENDATION_ID, count(*) as proposal_count
              from MIP.AGENT_OUT.ORDER_PROPOSALS
              where (:P_RUN_ID is null or RUN_ID = try_to_number(replace(:P_RUN_ID, 'T', '')))
                and (:P_PORTFOLIO_ID is null or PORTFOLIO_ID = :P_PORTFOLIO_ID)
                and RECOMMENDATION_ID is not null
              group by RUN_ID, PORTFOLIO_ID, RECOMMENDATION_ID
              having count(*) > 1
          );

        if (v_check_result > 0) then
            v_check_result := object_construct(
                'check_name', 'IDEMPOTENCY',
                'status', 'FAIL',
                'details', object_construct('duplicate_proposal_groups', :v_check_result)
            );
            v_fail_count := :v_fail_count + 1;
            v_overall_status := 'UNSAFE';
        else
            v_check_result := object_construct(
                'check_name', 'IDEMPOTENCY',
                'status', 'PASS'
            );
            v_pass_count := :v_pass_count + 1;
        end if;
        v_safety_checks := array_append(:v_safety_checks, :v_check_result);
    exception
        when other then
            v_check_result := object_construct(
                'check_name', 'IDEMPOTENCY',
                'status', 'FAIL',
                'error', :sqlerrm
            );
            v_safety_checks := array_append(:v_safety_checks, :v_check_result);
            v_fail_count := :v_fail_count + 1;
            v_overall_status := 'UNSAFE';
    end;

    -- Check 4: Signal linkage integrity
    begin
        select count(*)
          into v_check_result
          from MIP.AGENT_OUT.ORDER_PROPOSALS p
          left join MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
            on s.RECOMMENDATION_ID = p.RECOMMENDATION_ID
          where p.STATUS in ('APPROVED', 'EXECUTED')
            and (:P_RUN_ID is null or p.RUN_ID = try_to_number(replace(:P_RUN_ID, 'T', '')))
            and (:P_PORTFOLIO_ID is null or p.PORTFOLIO_ID = :P_PORTFOLIO_ID)
            and (s.RECOMMENDATION_ID is null or s.IS_ELIGIBLE = false);

        if (v_check_result > 0) then
            v_check_result := object_construct(
                'check_name', 'SIGNAL_LINKAGE_INTEGRITY',
                'status', 'FAIL',
                'details', object_construct('invalid_approved_or_executed', :v_check_result)
            );
            v_fail_count := :v_fail_count + 1;
            v_overall_status := 'UNSAFE';
        else
            v_check_result := object_construct(
                'check_name', 'SIGNAL_LINKAGE_INTEGRITY',
                'status', 'PASS'
            );
            v_pass_count := :v_pass_count + 1;
        end if;
        v_safety_checks := array_append(:v_safety_checks, :v_check_result);
    exception
        when other then
            v_check_result := object_construct(
                'check_name', 'SIGNAL_LINKAGE_INTEGRITY',
                'status', 'FAIL',
                'error', :sqlerrm
            );
            v_safety_checks := array_append(:v_safety_checks, :v_check_result);
            v_fail_count := :v_fail_count + 1;
            v_overall_status := 'UNSAFE';
    end;

    v_summary := object_construct(
        'run_id', :v_run_id,
        'check_timestamp', current_timestamp(),
        'overall_status', :v_overall_status,
        'total_checks', array_size(:v_safety_checks),
        'passed', :v_pass_count,
        'failed', :v_fail_count,
        'warnings', :v_warn_count,
        'safety_checks', :v_safety_checks
    );

    -- Log results
    call MIP.APP.SP_LOG_EVENT(
        'VALIDATION',
        'SP_MONITOR_AUTONOMY_SAFETY',
        :v_overall_status,
        array_size(:v_safety_checks),
        :v_summary,
        null,
        :v_run_id,
        null
    );

    return :v_summary;
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'VALIDATION',
            'SP_MONITOR_AUTONOMY_SAFETY',
            'FAIL',
            null,
            object_construct('error', :sqlerrm),
            :sqlerrm,
            :v_run_id,
            null
        );
        raise;
end;
$$;

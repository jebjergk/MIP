-- sp_run_integrity_checks.sql
-- Purpose: Automated test runner that executes all validation checks and returns structured results

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_INTEGRITY_CHECKS(
    P_RUN_ID string default null,
    P_PORTFOLIO_ID number default null,
    P_LOG_RESULTS boolean default true
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, nullif(current_query_tag(), ''), uuid_string());
    v_check_results array := array_construct();
    v_check_result variant;
    v_total_checks number := 0;
    v_passed_checks number := 0;
    v_failed_checks number := 0;
    v_warning_checks number := 0;
    v_failures array := array_construct();
    v_warnings array := array_construct();
    v_summary variant;
begin
    -- Set session variables for check scripts
    execute immediate 'alter session set v_run_id = ''' || coalesce(:v_run_id, 'null') || '''';
    execute immediate 'alter session set v_portfolio_id = ' || coalesce(:P_PORTFOLIO_ID::string, 'null');

    -- Check 1: Run Scoping Validation
    begin
        declare
            v_orphaned_proposals number := 0;
            v_trade_proposal_mismatch number := 0;
        begin
            -- Check for orphaned proposal run IDs (canonical RUN_ID_VARCHAR only; no SIGNAL_RUN_ID scoping)
            select count(*)
              into v_orphaned_proposals
              from MIP.AGENT_OUT.ORDER_PROPOSALS p
              left join MIP.APP.MIP_AUDIT_LOG a
                on a.RUN_ID = p.RUN_ID_VARCHAR
                and a.EVENT_TYPE = 'PIPELINE'
                and a.EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
              where p.RUN_ID_VARCHAR is not null
                and (:P_RUN_ID is null or p.RUN_ID_VARCHAR = :P_RUN_ID)
                and (:P_PORTFOLIO_ID is null or p.PORTFOLIO_ID = :P_PORTFOLIO_ID)
                and a.RUN_ID is null;

            -- SIGNAL_RUN_ID is optional/legacy; no production fail on linkage

            -- Check for trade-proposal run ID mismatches (canonical RUN_ID_VARCHAR)
            select count(*)
              into v_trade_proposal_mismatch
              from MIP.APP.PORTFOLIO_TRADES t
              join MIP.AGENT_OUT.ORDER_PROPOSALS p
                on p.PROPOSAL_ID = t.PROPOSAL_ID
              where (:P_RUN_ID is null or to_varchar(t.RUN_ID) = :P_RUN_ID)
                and (:P_PORTFOLIO_ID is null or t.PORTFOLIO_ID = :P_PORTFOLIO_ID)
                and (
                    to_varchar(t.RUN_ID) != p.RUN_ID_VARCHAR
                    or t.RUN_ID is null
                    or p.RUN_ID_VARCHAR is null
                );

            if (v_orphaned_proposals > 0 or v_trade_proposal_mismatch > 0) then
                v_check_result := object_construct(
                    'check_name', 'RUN_SCOPING_VALIDATION',
                    'status', 'FAIL',
                    'timestamp', current_timestamp(),
                    'details', object_construct(
                        'orphaned_proposals', :v_orphaned_proposals,
                        'trade_proposal_mismatch', :v_trade_proposal_mismatch
                    )
                );
                v_failed_checks := :v_failed_checks + 1;
                v_failures := array_append(:v_failures, 'RUN_SCOPING_VALIDATION: Found ' || (:v_orphaned_proposals + :v_trade_proposal_mismatch) || ' scoping issues');
            else
                v_check_result := object_construct(
                    'check_name', 'RUN_SCOPING_VALIDATION',
                    'status', 'PASS',
                    'timestamp', current_timestamp(),
                    'details', object_construct(
                        'orphaned_proposals', :v_orphaned_proposals,
                        'trade_proposal_mismatch', :v_trade_proposal_mismatch
                    )
                );
                v_passed_checks := :v_passed_checks + 1;
            end if;
            v_check_results := array_append(:v_check_results, :v_check_result);
        end;
    exception
        when other then
            v_check_result := object_construct(
                'check_name', 'RUN_SCOPING_VALIDATION',
                'status', 'FAIL',
                'timestamp', current_timestamp(),
                'error', :sqlerrm
            );
            v_check_results := array_append(:v_check_results, :v_check_result);
            v_failed_checks := :v_failed_checks + 1;
            v_failures := array_append(:v_failures, 'RUN_SCOPING_VALIDATION: ' || :sqlerrm);
    end;

    -- Check 2: Entry Gate Consistency
    begin
        v_check_result := object_construct(
            'check_name', 'ENTRY_GATE_CONSISTENCY',
            'status', 'RUNNING',
            'timestamp', current_timestamp()
        );
        
        -- Check for BUY proposals during entry blocks
        select count(*)
          into v_check_result
          from MIP.AGENT_OUT.ORDER_PROPOSALS p
          join MIP.MART.V_PORTFOLIO_RISK_STATE g
            on g.PORTFOLIO_ID = p.PORTFOLIO_ID
          where p.SIDE = 'BUY'
            and (:P_RUN_ID is null or p.RUN_ID_VARCHAR = :P_RUN_ID)
            and (:P_PORTFOLIO_ID is null or p.PORTFOLIO_ID = :P_PORTFOLIO_ID)
            and g.ENTRIES_BLOCKED = true
            and p.PROPOSED_AT >= coalesce(g.DRAWDOWN_STOP_TS, current_timestamp() - interval '7 days');
        
        if (v_check_result > 0) then
            v_check_result := object_construct(
                'check_name', 'ENTRY_GATE_CONSISTENCY',
                'status', 'FAIL',
                'timestamp', current_timestamp(),
                'details', object_construct('buy_proposals_during_block', :v_check_result)
            );
            v_failed_checks := :v_failed_checks + 1;
            v_failures := array_append(:v_failures, 'ENTRY_GATE_CONSISTENCY: Found ' || :v_check_result || ' BUY proposals during entry block');
        else
            v_check_result := object_construct(
                'check_name', 'ENTRY_GATE_CONSISTENCY',
                'status', 'PASS',
                'timestamp', current_timestamp()
            );
            v_passed_checks := :v_passed_checks + 1;
        end if;
        v_check_results := array_append(:v_check_results, :v_check_result);
    exception
        when other then
            v_check_result := object_construct(
                'check_name', 'ENTRY_GATE_CONSISTENCY',
                'status', 'FAIL',
                'timestamp', current_timestamp(),
                'error', :sqlerrm
            );
            v_check_results := array_append(:v_check_results, :v_check_result);
            v_failed_checks := :v_failed_checks + 1;
            v_failures := array_append(:v_failures, 'ENTRY_GATE_CONSISTENCY: ' || :sqlerrm);
    end;

    -- Check 3: Signal Linkage Integrity
    begin
        select count(*)
          into v_check_result
          from MIP.AGENT_OUT.ORDER_PROPOSALS p
          left join MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
            on s.RECOMMENDATION_ID = p.RECOMMENDATION_ID
          where p.STATUS in ('APPROVED', 'EXECUTED')
            and (:P_RUN_ID is null or p.RUN_ID = :P_RUN_ID)
            and (:P_PORTFOLIO_ID is null or p.PORTFOLIO_ID = :P_PORTFOLIO_ID)
            and (s.RECOMMENDATION_ID is null or s.IS_ELIGIBLE = false);
        
        if (v_check_result > 0) then
            v_check_result := object_construct(
                'check_name', 'SIGNAL_LINKAGE_INTEGRITY',
                'status', 'FAIL',
                'timestamp', current_timestamp(),
                'details', object_construct('invalid_approved_or_executed', :v_check_result)
            );
            v_failed_checks := :v_failed_checks + 1;
            v_failures := array_append(:v_failures, 'SIGNAL_LINKAGE_INTEGRITY: Found ' || :v_check_result || ' invalid approved/executed proposals');
        else
            v_check_result := object_construct(
                'check_name', 'SIGNAL_LINKAGE_INTEGRITY',
                'status', 'PASS',
                'timestamp', current_timestamp()
            );
            v_passed_checks := :v_passed_checks + 1;
        end if;
        v_check_results := array_append(:v_check_results, :v_check_result);
    exception
        when other then
            v_check_result := object_construct(
                'check_name', 'SIGNAL_LINKAGE_INTEGRITY',
                'status', 'FAIL',
                'timestamp', current_timestamp(),
                'error', :sqlerrm
            );
            v_check_results := array_append(:v_check_results, :v_check_result);
            v_failed_checks := :v_failed_checks + 1;
            v_failures := array_append(:v_failures, 'SIGNAL_LINKAGE_INTEGRITY: ' || :sqlerrm);
    end;

    -- Check 4: Idempotency
    begin
        select count(*)
          into v_check_result
          from (
              select RUN_ID_VARCHAR, PORTFOLIO_ID, RECOMMENDATION_ID, count(*) as proposal_count
              from MIP.AGENT_OUT.ORDER_PROPOSALS
              where (:P_RUN_ID is null or RUN_ID_VARCHAR = :P_RUN_ID)
                and (:P_PORTFOLIO_ID is null or PORTFOLIO_ID = :P_PORTFOLIO_ID)
                and RECOMMENDATION_ID is not null
              group by RUN_ID_VARCHAR, PORTFOLIO_ID, RECOMMENDATION_ID
              having count(*) > 1
          );
        
        if (v_check_result > 0) then
            v_check_result := object_construct(
                'check_name', 'IDEMPOTENCY_PROPOSALS',
                'status', 'FAIL',
                'timestamp', current_timestamp(),
                'details', object_construct('duplicate_proposals', :v_check_result)
            );
            v_failed_checks := :v_failed_checks + 1;
            v_failures := array_append(:v_failures, 'IDEMPOTENCY_PROPOSALS: Found ' || :v_check_result || ' duplicate proposal groups');
        else
            v_check_result := object_construct(
                'check_name', 'IDEMPOTENCY_PROPOSALS',
                'status', 'PASS',
                'timestamp', current_timestamp()
            );
            v_passed_checks := :v_passed_checks + 1;
        end if;
        v_check_results := array_append(:v_check_results, :v_check_result);
    exception
        when other then
            v_check_result := object_construct(
                'check_name', 'IDEMPOTENCY_PROPOSALS',
                'status', 'FAIL',
                'timestamp', current_timestamp(),
                'error', :sqlerrm
            );
            v_check_results := array_append(:v_check_results, :v_check_result);
            v_failed_checks := :v_failed_checks + 1;
            v_failures := array_append(:v_failures, 'IDEMPOTENCY_PROPOSALS: ' || :sqlerrm);
    end;

    -- Check 5: Morning Brief Consistency
    begin
        declare
            v_briefs_without_run_id number := 0;
            v_count_mismatches number := 0;
        begin
            -- Check for briefs without run ID
            select count(*)
              into v_briefs_without_run_id
              from MIP.AGENT_OUT.MORNING_BRIEF mb
              where (:P_RUN_ID is null or mb.PIPELINE_RUN_ID = :P_RUN_ID)
                and (:P_PORTFOLIO_ID is null or mb.PORTFOLIO_ID = :P_PORTFOLIO_ID)
                and mb.PIPELINE_RUN_ID is null;

            -- Check for proposal/execution count mismatches
            select count(*)
              into v_count_mismatches
              from (
                  with brief_proposals as (
                      select
                          mb.PIPELINE_RUN_ID,
                          mb.PORTFOLIO_ID,
                          mb.BRIEF:proposals:summary:proposed::number as brief_proposed_count,
                          mb.BRIEF:proposals:summary:executed::number as brief_executed_count
                      from MIP.AGENT_OUT.MORNING_BRIEF mb
                      where (:P_RUN_ID is null or mb.PIPELINE_RUN_ID = :P_RUN_ID)
                        and (:P_PORTFOLIO_ID is null or mb.PORTFOLIO_ID = :P_PORTFOLIO_ID)
                  ),
                  actual_proposals as (
                      select
                          p.RUN_ID_VARCHAR as PIPELINE_RUN_ID,
                          p.PORTFOLIO_ID,
                          count(*) as actual_proposed_count,
                          count_if(p.STATUS = 'EXECUTED') as actual_executed_count
                      from MIP.AGENT_OUT.ORDER_PROPOSALS p
                      where (:P_RUN_ID is null or p.RUN_ID_VARCHAR = :P_RUN_ID)
                        and (:P_PORTFOLIO_ID is null or p.PORTFOLIO_ID = :P_PORTFOLIO_ID)
                      group by p.RUN_ID_VARCHAR, p.PORTFOLIO_ID
                  )
                  select bp.*
                  from brief_proposals bp
                  join actual_proposals ap
                    on ap.PIPELINE_RUN_ID = bp.PIPELINE_RUN_ID
                   and ap.PORTFOLIO_ID = bp.PORTFOLIO_ID
                  where bp.brief_proposed_count != ap.actual_proposed_count
                     or bp.brief_executed_count != ap.actual_executed_count
              );

            if (v_briefs_without_run_id > 0 or v_count_mismatches > 0) then
                v_check_result := object_construct(
                    'check_name', 'MORNING_BRIEF_CONSISTENCY',
                    'status', 'WARN',
                    'timestamp', current_timestamp(),
                    'details', object_construct(
                        'briefs_without_run_id', :v_briefs_without_run_id,
                        'count_mismatches', :v_count_mismatches
                    )
                );
                v_warning_checks := :v_warning_checks + 1;
                v_warnings := array_append(:v_warnings, 'MORNING_BRIEF_CONSISTENCY: Found ' || (:v_briefs_without_run_id + :v_count_mismatches) || ' consistency issues');
            else
                v_check_result := object_construct(
                    'check_name', 'MORNING_BRIEF_CONSISTENCY',
                    'status', 'PASS',
                    'timestamp', current_timestamp(),
                    'details', object_construct(
                        'briefs_without_run_id', :v_briefs_without_run_id,
                        'count_mismatches', :v_count_mismatches
                    )
                );
                v_passed_checks := :v_passed_checks + 1;
            end if;
            v_check_results := array_append(:v_check_results, :v_check_result);
        end;
    exception
        when other then
            v_check_result := object_construct(
                'check_name', 'MORNING_BRIEF_CONSISTENCY',
                'status', 'FAIL',
                'timestamp', current_timestamp(),
                'error', :sqlerrm
            );
            v_check_results := array_append(:v_check_results, :v_check_result);
            v_failed_checks := :v_failed_checks + 1;
            v_failures := array_append(:v_failures, 'MORNING_BRIEF_CONSISTENCY: ' || :sqlerrm);
    end;

    v_total_checks := array_size(:v_check_results);

    v_summary := object_construct(
        'run_id', :v_run_id,
        'check_timestamp', current_timestamp(),
        'total_checks', :v_total_checks,
        'passed', :v_passed_checks,
        'failed', :v_failed_checks,
        'warnings', :v_warning_checks,
        'overall_status', case
            when :v_failed_checks > 0 then 'FAIL'
            when :v_warning_checks > 0 then 'WARN'
            else 'PASS'
        end,
        'check_results', :v_check_results,
        'failures', :v_failures,
        'warnings', :v_warnings
    );

    -- Log results to audit log if requested
    if (:P_LOG_RESULTS) then
        call MIP.APP.SP_LOG_EVENT(
            'VALIDATION',
            'SP_RUN_INTEGRITY_CHECKS',
            case
                when :v_failed_checks > 0 then 'FAIL'
                when :v_warning_checks > 0 then 'WARN'
                else 'SUCCESS'
            end,
            :v_total_checks,
            :v_summary,
            null,
            :v_run_id,
            null
        );
    end if;

    return :v_summary;
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'VALIDATION',
            'SP_RUN_INTEGRITY_CHECKS',
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

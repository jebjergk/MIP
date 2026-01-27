-- 148_sp_pipeline_write_morning_briefs.sql
-- Purpose: Pipeline step to persist morning briefs for active portfolios

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEF(
    P_PORTFOLIO_ID number,
    P_RUN_ID string,
    P_SIGNAL_RUN_ID number,
    P_PARENT_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, nullif(current_query_tag(), ''), uuid_string());
    v_step_start timestamp_ntz;
    v_step_end timestamp_ntz;
    v_rows_before number;
    v_rows_after number;
    v_propose_result variant;
    v_validate_result variant;
    v_signal_run_id number := :P_SIGNAL_RUN_ID;
begin
    v_step_start := current_timestamp();

    begin
        select count(*)
          into :v_rows_before
          from MIP.AGENT_OUT.MORNING_BRIEF
         where PORTFOLIO_ID = :P_PORTFOLIO_ID
           and PIPELINE_RUN_ID = :v_run_id;

        if (v_signal_run_id is not null) then
            v_propose_result := (call MIP.APP.SP_AGENT_PROPOSE_TRADES(
                :P_PORTFOLIO_ID,
                :v_signal_run_id,
                :P_PARENT_RUN_ID
            ));

            v_validate_result := (call MIP.APP.SP_VALIDATE_AND_EXECUTE_PROPOSALS(
                :P_PORTFOLIO_ID,
                :v_signal_run_id,
                :P_PARENT_RUN_ID
            ));
        else
            v_propose_result := object_construct('status', 'SKIPPED', 'reason', 'NO_SIGNAL_RUN_ID');
            v_validate_result := object_construct('status', 'SKIPPED', 'reason', 'NO_SIGNAL_RUN_ID');
        end if;

        call MIP.APP.SP_WRITE_MORNING_BRIEF(:P_PORTFOLIO_ID, :v_run_id);

        select count(*)
          into :v_rows_after
          from MIP.AGENT_OUT.MORNING_BRIEF
         where PORTFOLIO_ID = :P_PORTFOLIO_ID
           and PIPELINE_RUN_ID = :v_run_id;

        v_step_end := current_timestamp();

        call MIP.APP.SP_AUDIT_LOG_STEP(
            :P_PARENT_RUN_ID,
            'MORNING_BRIEF',
            'SUCCESS',
            :v_rows_after,
            object_construct(
                'step_name', 'morning_brief',
                'scope', 'PORTFOLIO',
                'scope_key', to_varchar(:P_PORTFOLIO_ID),
                'portfolio_id', :P_PORTFOLIO_ID,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'rows_before', :v_rows_before,
                'rows_after', :v_rows_after,
                'signal_run_id', :v_signal_run_id,
                'proposal_result', :v_propose_result,
                'validation_result', :v_validate_result
            ),
            null
        );

        return object_construct(
            'portfolio_id', :P_PORTFOLIO_ID,
            'rows_before', :v_rows_before,
            'rows_after', :v_rows_after,
            'signal_run_id', :v_signal_run_id,
            'proposal_result', :v_propose_result,
            'validation_result', :v_validate_result
        );
    exception
        when other then
            v_step_end := current_timestamp();
            call MIP.APP.SP_AUDIT_LOG_STEP(
                :P_PARENT_RUN_ID,
                'MORNING_BRIEF',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'morning_brief',
                    'scope', 'PORTFOLIO',
                    'scope_key', to_varchar(:P_PORTFOLIO_ID),
                    'portfolio_id', :P_PORTFOLIO_ID,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end
                ),
                :sqlerrm
            );
            raise;
    end;
end;
$$;

create or replace procedure MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEFS(
    P_RUN_ID string,
    P_SIGNAL_RUN_ID number,
    P_PARENT_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, nullif(current_query_tag(), ''), uuid_string());
    v_portfolios resultset;
    v_portfolio_id number;
    v_result variant;
    v_results array := array_construct();
    v_portfolio_count number := 0;
begin
    v_portfolios := (
        select PORTFOLIO_ID
          from MIP.APP.PORTFOLIO
         where STATUS = 'ACTIVE'
         order by PORTFOLIO_ID
    );

    for rec in v_portfolios do
        v_portfolio_id := rec.PORTFOLIO_ID;
        v_portfolio_count := v_portfolio_count + 1;
        v_result := (call MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEF(
            :v_portfolio_id,
            :v_run_id,
            :P_SIGNAL_RUN_ID,
            :P_PARENT_RUN_ID
        ));
        v_results := array_append(:v_results, :v_result);
    end for;

    return object_construct(
        'status', 'SUCCESS',
        'portfolio_count', :v_portfolio_count,
        'results', :v_results
    );
end;
$$;

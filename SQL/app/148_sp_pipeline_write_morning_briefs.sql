-- 148_sp_pipeline_write_morning_briefs.sql
-- Purpose: Pipeline step to persist morning briefs for active portfolios.
-- Passes as_of_ts and run_id from pipeline; brief write is deterministic + idempotent.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEF(
    P_PORTFOLIO_ID number,
    P_AS_OF_TS timestamp_ntz,
    P_RUN_ID string,   -- pipeline run id (canonical); used for propose/validate and brief
    P_PARENT_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    -- Copy parameters to local variables to avoid binding issues with exception handlers
    v_portfolio_id number := :P_PORTFOLIO_ID;
    v_as_of_ts timestamp_ntz := :P_AS_OF_TS;
    v_parent_run_id string := :P_PARENT_RUN_ID;
    v_run_id string := coalesce(:P_RUN_ID, nullif(current_query_tag(), ''), uuid_string());
    v_step_start timestamp_ntz;
    v_step_end timestamp_ntz;
    v_rows_before number;
    v_rows_after number;
    v_propose_result variant;
    v_validate_result variant;
begin
    v_step_start := current_timestamp();

    begin
        select count(*)
          into :v_rows_before
          from MIP.AGENT_OUT.MORNING_BRIEF
         where PORTFOLIO_ID = :v_portfolio_id
           and RUN_ID = :v_run_id
           and AS_OF_TS = :v_as_of_ts;

        if (v_run_id is not null) then
            v_propose_result := (call MIP.APP.SP_AGENT_PROPOSE_TRADES(
                :v_portfolio_id,
                :v_run_id,
                :v_parent_run_id
            ));

            v_validate_result := (call MIP.APP.SP_VALIDATE_AND_EXECUTE_PROPOSALS(
                :v_portfolio_id,
                :v_run_id,
                :v_parent_run_id
            ));
        else
            v_propose_result := object_construct('status', 'SKIPPED', 'reason', 'NO_RUN_ID');
            v_validate_result := object_construct('status', 'SKIPPED', 'reason', 'NO_RUN_ID');
        end if;

        call MIP.APP.SP_WRITE_MORNING_BRIEF(:v_portfolio_id, :v_as_of_ts, :v_run_id, 'MORNING_BRIEF');

        select count(*)
          into :v_rows_after
          from MIP.AGENT_OUT.MORNING_BRIEF
         where PORTFOLIO_ID = :v_portfolio_id
           and RUN_ID = :v_run_id
           and AS_OF_TS = :v_as_of_ts;

        v_step_end := current_timestamp();

        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_parent_run_id,
            'MORNING_BRIEF',
            'SUCCESS',
            :v_rows_after,
            object_construct(
                'step_name', 'morning_brief',
                'scope', 'PORTFOLIO',
                'scope_key', to_varchar(:v_portfolio_id),
                'portfolio_id', :v_portfolio_id,
                'as_of_ts', :v_as_of_ts,
                'run_id', :v_run_id,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'rows_before', :v_rows_before,
                'rows_after', :v_rows_after,
                'proposal_result', :v_propose_result,
                'validation_result', :v_validate_result
            ),
            null
        );

        return object_construct(
            'portfolio_id', :v_portfolio_id,
            'as_of_ts', :v_as_of_ts,
            'run_id', :v_run_id,
            'rows_before', :v_rows_before,
            'rows_after', :v_rows_after,
            'proposal_result', :v_propose_result,
            'validation_result', :v_validate_result
        );
    exception
        when other then
            v_step_end := current_timestamp();
            call MIP.APP.SP_AUDIT_LOG_STEP(
                :v_parent_run_id,
                'MORNING_BRIEF',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'morning_brief',
                    'scope', 'PORTFOLIO',
                    'scope_key', to_varchar(:v_portfolio_id),
                    'portfolio_id', :v_portfolio_id,
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
    P_AS_OF_TS timestamp_ntz,
    P_RUN_ID string,
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
            :P_AS_OF_TS,
            :v_run_id,
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

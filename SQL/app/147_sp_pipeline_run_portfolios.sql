-- 147_sp_pipeline_run_portfolios.sql
-- Purpose: Pipeline step to run portfolio simulations for active portfolios

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_PIPELINE_RUN_PORTFOLIO(
    P_PORTFOLIO_ID number,
    P_FROM_TS timestamp_ntz,
    P_TO_TS timestamp_ntz,
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
    v_step_start timestamp_ntz;
    v_step_end timestamp_ntz;
    v_sim_result variant;
    v_sim_run_id string;
    v_rows_after number;
    v_status string;
    v_audit_status string;
    v_duration_ms number;
    v_error_query_id string;
begin
    v_step_start := current_timestamp();

    begin
        v_sim_result := (call MIP.APP.SP_RUN_PORTFOLIO_SIMULATION(
            :P_PORTFOLIO_ID,
            :P_FROM_TS,
            :P_TO_TS
        ));

        v_sim_run_id := v_sim_result:run_id::string;
        v_status := v_sim_result:status::string;
        v_audit_status := case
            when v_status = 'ERROR' then 'FAIL'
            else 'SUCCESS'
        end;

        if (v_sim_run_id is not null) then
            select count(*)
              into :v_rows_after
              from MIP.APP.PORTFOLIO_DAILY
             where PORTFOLIO_ID = :P_PORTFOLIO_ID
               and RUN_ID = :v_sim_run_id;
        else
            v_rows_after := null;
        end if;

        v_step_end := current_timestamp();
        v_duration_ms := timestampdiff(millisecond, v_step_start, v_step_end);

        call MIP.APP.SP_AUDIT_LOG_STEP(
            :P_PARENT_RUN_ID,
            'PORTFOLIO_SIMULATION',
            :v_audit_status,
            :v_rows_after,
            object_construct(
                'step_name', 'portfolio_simulation',
                'scope', 'PORTFOLIO',
                'scope_key', to_varchar(:P_PORTFOLIO_ID),
                'portfolio_id', :P_PORTFOLIO_ID,
                'portfolio_run_id', :v_sim_run_id,
                'from_ts', :P_FROM_TS,
                'to_ts', :P_TO_TS,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'daily_rows', :v_rows_after,
                'simulation', :v_sim_result
            ),
            null,  -- P_ERROR_MESSAGE
            null,  -- P_ERROR_SQLSTATE
            null,  -- P_ERROR_QUERY_ID
            null,  -- P_ERROR_CONTEXT
            :v_duration_ms
        );

        return :v_sim_result;
    exception
        when other then
            v_step_end := current_timestamp();
            v_duration_ms := timestampdiff(millisecond, v_step_start, v_step_end);
            v_error_query_id := last_query_id();
            
            call MIP.APP.SP_AUDIT_LOG_STEP(
                :P_PARENT_RUN_ID,
                'PORTFOLIO_SIMULATION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'portfolio_simulation',
                    'scope', 'PORTFOLIO',
                    'scope_key', to_varchar(:P_PORTFOLIO_ID),
                    'portfolio_id', :P_PORTFOLIO_ID,
                    'from_ts', :P_FROM_TS,
                    'to_ts', :P_TO_TS,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end
                ),
                :sqlerrm,                -- P_ERROR_MESSAGE
                :sqlstate,               -- P_ERROR_SQLSTATE
                :v_error_query_id,       -- P_ERROR_QUERY_ID
                object_construct(        -- P_ERROR_CONTEXT
                    'proc_name', 'SP_PIPELINE_RUN_PORTFOLIO',
                    'portfolio_id', :P_PORTFOLIO_ID,
                    'run_id', :v_run_id,
                    'parent_run_id', :P_PARENT_RUN_ID
                ),
                :v_duration_ms
            );
            raise;
    end;
end;
$$;

create or replace procedure MIP.APP.SP_PIPELINE_RUN_PORTFOLIOS(
    P_FROM_TS timestamp_ntz,
    P_TO_TS timestamp_ntz,
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
    v_sim_result variant;
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
        v_sim_result := (call MIP.APP.SP_PIPELINE_RUN_PORTFOLIO(
            :v_portfolio_id,
            :P_FROM_TS,
            :P_TO_TS,
            :v_run_id,
            :P_PARENT_RUN_ID
        ));
        v_results := array_append(:v_results, :v_sim_result);
    end for;

    return object_construct(
        'status', 'SUCCESS',
        'portfolio_count', :v_portfolio_count,
        'results', :v_results
    );
end;
$$;

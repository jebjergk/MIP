-- 147_sp_pipeline_run_portfolios.sql
-- Purpose: Pipeline step to process active live portfolios (sim retired)

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
    v_portfolio_id number := :P_PORTFOLIO_ID;
    v_from_ts timestamp_ntz := :P_FROM_TS;
    v_to_ts timestamp_ntz := :P_TO_TS;
    v_parent_run_id string := :P_PARENT_RUN_ID;
    v_run_id string := coalesce(:P_RUN_ID, nullif(current_query_tag(), ''), uuid_string());
    v_step_start timestamp_ntz;
    v_step_end timestamp_ntz;
    v_duration_ms number;
begin
    v_step_start := current_timestamp();
    v_step_end := current_timestamp();
    v_duration_ms := timestampdiff(millisecond, v_step_start, v_step_end);

    call MIP.APP.SP_AUDIT_LOG_STEP(
        :v_parent_run_id,
        'PORTFOLIO_LIVE_SYNC',
        'SUCCESS',
        0,
        object_construct(
            'step_name', 'portfolio_live_sync',
            'scope', 'PORTFOLIO',
            'scope_key', to_varchar(:v_portfolio_id),
            'portfolio_id', :v_portfolio_id,
            'from_ts', :v_from_ts,
            'to_ts', :v_to_ts,
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'note', 'Simulation retired; live portfolio sync is handled via LIVE_ACTIONS/LIVE_ORDERS.'
        ),
        null,
        null,
        null,
        null,
        :v_duration_ms
    );

    return object_construct(
        'status', 'SUCCESS',
        'portfolio_id', :v_portfolio_id,
        'run_id', :v_run_id,
        'from_ts', :v_from_ts,
        'to_ts', :v_to_ts,
        'trades', 0,
        'entries_blocked', false,
        'block_reason', null
    );
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
    v_live_result variant;
    v_results array := array_construct();
    v_portfolio_count number := 0;
begin
    v_portfolios := (
        select PORTFOLIO_ID
          from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
         where coalesce(IS_ACTIVE, true)
         order by PORTFOLIO_ID
    );

    for rec in v_portfolios do
        v_portfolio_id := rec.PORTFOLIO_ID;
        v_portfolio_count := v_portfolio_count + 1;
        v_live_result := (call MIP.APP.SP_PIPELINE_RUN_PORTFOLIO(
            :v_portfolio_id,
            :P_FROM_TS,
            :P_TO_TS,
            :v_run_id,
            :P_PARENT_RUN_ID
        ));
        v_results := array_append(:v_results, :v_live_result);
    end for;

    return object_construct(
        'status', 'SUCCESS',
        'portfolio_count', :v_portfolio_count,
        'results', :v_results
    );
end;
$$;

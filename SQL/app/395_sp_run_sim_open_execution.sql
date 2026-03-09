-- 395_sp_run_sim_open_execution.sql
-- Purpose: Run simulation committee+execution during opening session window (no human approval path).

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_SIM_OPEN_EXECUTION(
    P_PORTFOLIO_ID number default null,
    P_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_now_ny timestamp_tz := convert_timezone('America/New_York', current_timestamp());
    v_dow number := dayofweekiso(v_now_ny::date);
    v_mins_since_midnight number := date_part(hour, v_now_ny) * 60 + date_part(minute, v_now_ny);
    v_is_open boolean := false;
    v_stabilization_minutes number := 5;
    v_ready boolean := false;
    v_portfolios resultset;
    v_portfolio_id number;
    v_run_id string;
    v_results array := array_construct();
    v_result variant;
    v_executed_portfolios number := 0;
begin
    select coalesce(try_to_number(max(case when CONFIG_KEY = 'SIM_OPEN_STABILIZATION_MINUTES' then CONFIG_VALUE end)), 5)
      into :v_stabilization_minutes
      from MIP.APP.APP_CONFIG;

    v_is_open := (
        v_dow between 1 and 5
        and v_mins_since_midnight >= 570
        and v_mins_since_midnight < 960
    );

    v_ready := (
        v_is_open
        and v_mins_since_midnight >= (570 + v_stabilization_minutes)
    );

    if (not v_ready) then
        return object_construct(
            'status', 'SKIPPED',
            'reason', iff(v_is_open, 'STABILIZATION_WINDOW_NOT_REACHED', 'MARKET_NOT_OPEN'),
            'now_ny', to_varchar(v_now_ny),
            'stabilization_minutes', v_stabilization_minutes,
            'executed_portfolios', 0,
            'results', array_construct()
        );
    end if;

    if (P_PORTFOLIO_ID is not null) then
        v_portfolios := (
            select :P_PORTFOLIO_ID as PORTFOLIO_ID
        );
    else
        v_portfolios := (
            select PORTFOLIO_ID
              from MIP.APP.PORTFOLIO
             where STATUS = 'ACTIVE'
             order by PORTFOLIO_ID
        );
    end if;

    for rec in v_portfolios do
        v_portfolio_id := rec.PORTFOLIO_ID;
        if (P_RUN_ID is not null) then
            v_run_id := P_RUN_ID;
        else
            select max(RUN_ID_VARCHAR)
              into :v_run_id
              from MIP.AGENT_OUT.ORDER_PROPOSALS
             where PORTFOLIO_ID = :v_portfolio_id
               and STATUS = 'PROPOSED';
        end if;

        if (v_run_id is null) then
            v_result := object_construct(
                'portfolio_id', :v_portfolio_id,
                'status', 'SKIPPED',
                'reason', 'NO_PROPOSED_RUN'
            );
        else
            v_result := (call MIP.APP.SP_VALIDATE_AND_EXECUTE_PROPOSALS(
                :v_portfolio_id,
                :v_run_id,
                :v_run_id
            ));
            v_executed_portfolios := v_executed_portfolios + 1;
        end if;

        v_results := array_append(v_results, v_result);
    end for;

    return object_construct(
        'status', 'SUCCESS',
        'now_ny', to_varchar(v_now_ny),
        'stabilization_minutes', v_stabilization_minutes,
        'executed_portfolios', v_executed_portfolios,
        'results', v_results
    );
end;
$$;

grant usage on procedure MIP.APP.SP_RUN_SIM_OPEN_EXECUTION(number, string) to role MIP_UI_API_ROLE;
grant usage on procedure MIP.APP.SP_RUN_SIM_OPEN_EXECUTION(number, string) to role MIP_APP_ROLE;

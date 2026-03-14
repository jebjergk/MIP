-- 406_sp_run_parallel_worlds_research_only.sql
-- Purpose: Research-only Parallel Worlds implementation (no SIM execution table dependency).

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_PARALLEL_WORLDS(
    P_RUN_ID        varchar,
    P_AS_OF_TS      timestamp_ntz,
    P_PORTFOLIO_ID  number default null,
    P_SCENARIO_SET  varchar default 'DEFAULT_ACTIVE'
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id varchar := :P_RUN_ID;
    v_as_of_ts timestamp_ntz := :P_AS_OF_TS;
    v_portfolio_count number := 0;
    v_scenario_count number := 0;
    v_result_count number := 0;
begin
    insert into MIP.APP.PARALLEL_WORLD_RUN_LOG (
        RUN_ID, AS_OF_TS, PORTFOLIO_ID, SCENARIO_SET, STATUS, STARTED_AT
    ) values (
        :v_run_id, :v_as_of_ts, :P_PORTFOLIO_ID, :P_SCENARIO_SET, 'RUNNING', current_timestamp()
    );

    create or replace temporary table TMP_PW_PORTFOLIOS as
    select c.PORTFOLIO_ID
    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG c
    where coalesce(c.IS_ACTIVE, false) = true
      and upper(coalesce(c.ADAPTER_MODE, '')) = 'LIVE'
      and (:P_PORTFOLIO_ID is null or c.PORTFOLIO_ID = :P_PORTFOLIO_ID);

    create or replace temporary table TMP_PW_ACTUAL as
    with cfg as (
        select
            a.PORTFOLIO_ID,
            min(a.STARTING_CASH)::number(18,4) as STARTING_CASH
        from MIP.MART.V_PARALLEL_WORLD_ACTUAL a
        join TMP_PW_PORTFOLIOS tp on tp.PORTFOLIO_ID = a.PORTFOLIO_ID
        group by a.PORTFOLIO_ID
    ),
    day_actual as (
        select
            a.PORTFOLIO_ID,
            a.AS_OF_TS,
            a.EPISODE_ID,
            a.STARTING_CASH,
            a.TRADES_ACTUAL,
            a.OPEN_POSITIONS,
            a.DAILY_PNL,
            a.DAILY_RETURN,
            a.DRAWDOWN,
            a.TOTAL_EQUITY,
            a.CASH
        from MIP.MART.V_PARALLEL_WORLD_ACTUAL a
        where a.AS_OF_TS::date = :v_as_of_ts::date
    )
    select
        tp.PORTFOLIO_ID,
        :v_as_of_ts as AS_OF_TS,
        da.EPISODE_ID,
        coalesce(da.STARTING_CASH, cfg.STARTING_CASH) as STARTING_CASH,
        coalesce(da.TRADES_ACTUAL, 0) as TRADES_ACTUAL,
        coalesce(da.OPEN_POSITIONS, 0) as OPEN_POSITIONS,
        coalesce(da.DAILY_PNL, 0)::number(18,4) as ACTUAL_PNL,
        coalesce(da.DAILY_RETURN, 0)::number(18,8) as ACTUAL_RETURN,
        coalesce(da.DRAWDOWN, 0)::number(18,8) as ACTUAL_DRAWDOWN,
        coalesce(da.TOTAL_EQUITY, cfg.STARTING_CASH)::number(18,4) as ACTUAL_EQUITY,
        coalesce(da.CASH, cfg.STARTING_CASH)::number(18,4) as ACTUAL_CASH
    from TMP_PW_PORTFOLIOS tp
    join cfg on cfg.PORTFOLIO_ID = tp.PORTFOLIO_ID
    left join day_actual da on da.PORTFOLIO_ID = tp.PORTFOLIO_ID;

    merge into MIP.APP.PARALLEL_WORLD_RESULT t
    using (
        select
            :v_run_id as RUN_ID,
            a.PORTFOLIO_ID,
            a.AS_OF_TS,
            0 as SCENARIO_ID,
            'ACTUAL' as WORLD_KEY,
            a.EPISODE_ID,
            a.TRADES_ACTUAL as TRADES_SIMULATED,
            a.ACTUAL_PNL as PNL_SIMULATED,
            a.ACTUAL_RETURN as RETURN_PCT_SIMULATED,
            a.ACTUAL_DRAWDOWN as MAX_DRAWDOWN_PCT_SIMULATED,
            a.ACTUAL_EQUITY as END_EQUITY_SIMULATED,
            a.ACTUAL_CASH as CASH_END_SIMULATED,
            a.OPEN_POSITIONS as OPEN_POSITIONS_END,
            object_construct(
                'world', 'ACTUAL_RESEARCH',
                'source', 'LIVE.BROKER_SNAPSHOTS + LIVE_ACTIONS',
                'as_of_ts', a.AS_OF_TS
            ) as RESULT_JSON
        from TMP_PW_ACTUAL a
    ) s
    on t.RUN_ID = s.RUN_ID and t.PORTFOLIO_ID = s.PORTFOLIO_ID and t.AS_OF_TS = s.AS_OF_TS and t.SCENARIO_ID = 0
    when matched then update set
        t.WORLD_KEY = s.WORLD_KEY,
        t.EPISODE_ID = s.EPISODE_ID,
        t.TRADES_SIMULATED = s.TRADES_SIMULATED,
        t.PNL_SIMULATED = s.PNL_SIMULATED,
        t.RETURN_PCT_SIMULATED = s.RETURN_PCT_SIMULATED,
        t.MAX_DRAWDOWN_PCT_SIMULATED = s.MAX_DRAWDOWN_PCT_SIMULATED,
        t.END_EQUITY_SIMULATED = s.END_EQUITY_SIMULATED,
        t.CASH_END_SIMULATED = s.CASH_END_SIMULATED,
        t.OPEN_POSITIONS_END = s.OPEN_POSITIONS_END,
        t.RESULT_JSON = s.RESULT_JSON
    when not matched then insert (
        RUN_ID, PORTFOLIO_ID, AS_OF_TS, SCENARIO_ID, WORLD_KEY, EPISODE_ID,
        TRADES_SIMULATED, PNL_SIMULATED, RETURN_PCT_SIMULATED,
        MAX_DRAWDOWN_PCT_SIMULATED, END_EQUITY_SIMULATED, CASH_END_SIMULATED,
        OPEN_POSITIONS_END, RESULT_JSON, CREATED_AT
    ) values (
        s.RUN_ID, s.PORTFOLIO_ID, s.AS_OF_TS, s.SCENARIO_ID, s.WORLD_KEY, s.EPISODE_ID,
        s.TRADES_SIMULATED, s.PNL_SIMULATED, s.RETURN_PCT_SIMULATED,
        s.MAX_DRAWDOWN_PCT_SIMULATED, s.END_EQUITY_SIMULATED, s.CASH_END_SIMULATED,
        s.OPEN_POSITIONS_END, s.RESULT_JSON, current_timestamp()
    );

    merge into MIP.APP.PARALLEL_WORLD_RESULT t
    using (
        with scenarios as (
            select
                s.SCENARIO_ID,
                s.NAME,
                s.SCENARIO_TYPE,
                s.PARAMS_JSON,
                case
                    when s.SCENARIO_TYPE = 'BASELINE' then 0::number(18,8)
                    when s.SCENARIO_TYPE = 'SIZING' then coalesce(s.PARAMS_JSON:position_pct_multiplier::number(18,8), 1::number(18,8))
                    when s.SCENARIO_TYPE = 'TIMING' then greatest(0::number(18,8), 1::number(18,8) - 0.05::number(18,8) * coalesce(s.PARAMS_JSON:entry_delay_bars::number(18,8), 1::number(18,8)))
                    when s.SCENARIO_TYPE = 'HORIZON' then greatest(0::number(18,8), 1::number(18,8) + 0.01::number(18,8) * (coalesce(s.PARAMS_JSON:hold_horizon_bars::number(18,8), 5::number(18,8)) - 5::number(18,8)))
                    when s.SCENARIO_TYPE = 'EARLY_EXIT' then greatest(0::number(18,8), 0.8::number(18,8) + 0.2::number(18,8) * coalesce(s.PARAMS_JSON:payoff_multiplier::number(18,8), 1::number(18,8)))
                    when s.SCENARIO_TYPE = 'THRESHOLD' then greatest(
                        0::number(18,8),
                        1::number(18,8)
                        + 25::number(18,8) * coalesce(s.PARAMS_JSON:min_return_delta::number(18,8), 0::number(18,8))
                        + 0.1::number(18,8) * coalesce(s.PARAMS_JSON:min_zscore_delta::number(18,8), 0::number(18,8))
                    )
                    else 1::number(18,8)
                end as FACTOR
            from MIP.APP.PARALLEL_WORLD_SCENARIO s
            where s.IS_ACTIVE = true
              and (
                    (:P_SCENARIO_SET = 'SWEEP' and coalesce(s.IS_SWEEP, false) = true)
                    or (:P_SCENARIO_SET <> 'SWEEP' and coalesce(s.IS_SWEEP, false) = false)
                  )
        )
        select
            :v_run_id as RUN_ID,
            a.PORTFOLIO_ID,
            a.AS_OF_TS,
            s.SCENARIO_ID,
            'COUNTERFACTUAL' as WORLD_KEY,
            a.EPISODE_ID,
            iff(s.SCENARIO_TYPE = 'BASELINE', 0, greatest(round(a.TRADES_ACTUAL * abs(s.FACTOR)), 0))::number as TRADES_SIMULATED,
            iff(s.SCENARIO_TYPE = 'BASELINE', 0, a.ACTUAL_PNL * s.FACTOR)::number(18,4) as PNL_SIMULATED,
            iff(s.SCENARIO_TYPE = 'BASELINE', 0, a.ACTUAL_RETURN * s.FACTOR)::number(18,8) as RETURN_PCT_SIMULATED,
            iff(s.SCENARIO_TYPE = 'BASELINE', 0, a.ACTUAL_DRAWDOWN * s.FACTOR)::number(18,8) as MAX_DRAWDOWN_PCT_SIMULATED,
            iff(s.SCENARIO_TYPE = 'BASELINE', a.STARTING_CASH, a.STARTING_CASH + (a.ACTUAL_PNL * s.FACTOR))::number(18,4) as END_EQUITY_SIMULATED,
            iff(s.SCENARIO_TYPE = 'BASELINE', a.STARTING_CASH, greatest(a.ACTUAL_CASH * (2 - abs(s.FACTOR)), 0))::number(18,4) as CASH_END_SIMULATED,
            iff(s.SCENARIO_TYPE = 'BASELINE', 0, greatest(round(a.OPEN_POSITIONS * abs(s.FACTOR)), 0))::number as OPEN_POSITIONS_END,
            object_construct(
                'world', 'COUNTERFACTUAL_RESEARCH',
                'scenario_name', s.NAME,
                'scenario_type', s.SCENARIO_TYPE,
                'factor', s.FACTOR,
                'source', 'proposal/outcome replay'
            ) as RESULT_JSON
        from TMP_PW_ACTUAL a
        cross join scenarios s
    ) s
    on t.RUN_ID = s.RUN_ID and t.PORTFOLIO_ID = s.PORTFOLIO_ID and t.AS_OF_TS = s.AS_OF_TS and t.SCENARIO_ID = s.SCENARIO_ID
    when matched then update set
        t.WORLD_KEY = s.WORLD_KEY,
        t.EPISODE_ID = s.EPISODE_ID,
        t.TRADES_SIMULATED = s.TRADES_SIMULATED,
        t.PNL_SIMULATED = s.PNL_SIMULATED,
        t.RETURN_PCT_SIMULATED = s.RETURN_PCT_SIMULATED,
        t.MAX_DRAWDOWN_PCT_SIMULATED = s.MAX_DRAWDOWN_PCT_SIMULATED,
        t.END_EQUITY_SIMULATED = s.END_EQUITY_SIMULATED,
        t.CASH_END_SIMULATED = s.CASH_END_SIMULATED,
        t.OPEN_POSITIONS_END = s.OPEN_POSITIONS_END,
        t.RESULT_JSON = s.RESULT_JSON
    when not matched then insert (
        RUN_ID, PORTFOLIO_ID, AS_OF_TS, SCENARIO_ID, WORLD_KEY, EPISODE_ID,
        TRADES_SIMULATED, PNL_SIMULATED, RETURN_PCT_SIMULATED,
        MAX_DRAWDOWN_PCT_SIMULATED, END_EQUITY_SIMULATED, CASH_END_SIMULATED,
        OPEN_POSITIONS_END, RESULT_JSON, CREATED_AT
    ) values (
        s.RUN_ID, s.PORTFOLIO_ID, s.AS_OF_TS, s.SCENARIO_ID, s.WORLD_KEY, s.EPISODE_ID,
        s.TRADES_SIMULATED, s.PNL_SIMULATED, s.RETURN_PCT_SIMULATED,
        s.MAX_DRAWDOWN_PCT_SIMULATED, s.END_EQUITY_SIMULATED, s.CASH_END_SIMULATED,
        s.OPEN_POSITIONS_END, s.RESULT_JSON, current_timestamp()
    );

    select count(*) into :v_portfolio_count from TMP_PW_PORTFOLIOS;
    select count(*) into :v_scenario_count
    from MIP.APP.PARALLEL_WORLD_SCENARIO s
    where s.IS_ACTIVE = true
      and (
            (:P_SCENARIO_SET = 'SWEEP' and coalesce(s.IS_SWEEP, false) = true)
            or (:P_SCENARIO_SET <> 'SWEEP' and coalesce(s.IS_SWEEP, false) = false)
          );
    select count(*) into :v_result_count
    from MIP.APP.PARALLEL_WORLD_RESULT
    where RUN_ID = :v_run_id and AS_OF_TS = :v_as_of_ts;

    update MIP.APP.PARALLEL_WORLD_RUN_LOG
       set STATUS = 'COMPLETED',
           COMPLETED_AT = current_timestamp(),
           DETAILS = object_construct(
               'portfolio_count', :v_portfolio_count,
               'scenario_count', :v_scenario_count,
               'result_count', :v_result_count,
               'mode', 'RESEARCH_ONLY'
           )
     where RUN_ID = :v_run_id;

    return object_construct(
        'status', 'COMPLETED',
        'mode', 'RESEARCH_ONLY',
        'run_id', :v_run_id,
        'as_of_ts', :v_as_of_ts::string,
        'portfolio_count', :v_portfolio_count,
        'scenario_count', :v_scenario_count,
        'result_count', :v_result_count
    );
exception
    when other then
        update MIP.APP.PARALLEL_WORLD_RUN_LOG
           set STATUS = 'FAILED',
               COMPLETED_AT = current_timestamp(),
               DETAILS = object_construct('error', sqlerrm, 'mode', 'RESEARCH_ONLY')
         where RUN_ID = :v_run_id;
        raise;
end;
$$;

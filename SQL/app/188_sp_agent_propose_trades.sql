-- 188_sp_agent_propose_trades.sql
-- Purpose: Deterministic agent proposal generator for eligible signals

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_AGENT_PROPOSE_TRADES(
    P_RUN_ID number,
    P_PORTFOLIO_ID number
)
returns variant
language sql
execute as caller
as
$$
declare
    v_portfolio_profile_id number;
    v_max_positions number;
    v_max_position_pct float;
    v_candidate_count number := 0;
    v_inserted_count number := 0;
    v_selected_count number := 0;
    v_target_weight float;
begin
    select
        p.PROFILE_ID,
        prof.MAX_POSITIONS,
        prof.MAX_POSITION_PCT
      into v_portfolio_profile_id,
           v_max_positions,
           v_max_position_pct
      from MIP.APP.PORTFOLIO p
      left join MIP.APP.PORTFOLIO_PROFILE prof
        on prof.PROFILE_ID = p.PROFILE_ID
     where p.PORTFOLIO_ID = :P_PORTFOLIO_ID;

    if (v_portfolio_profile_id is null) then
        return object_construct(
            'status', 'ERROR',
            'message', 'Portfolio not found',
            'portfolio_id', :P_PORTFOLIO_ID
        );
    end if;

    v_max_positions := coalesce(v_max_positions, 5);
    v_max_position_pct := coalesce(v_max_position_pct, 1.0);

    if (v_max_positions <= 0) then
        return object_construct(
            'status', 'ERROR',
            'message', 'Invalid max positions configuration',
            'portfolio_id', :P_PORTFOLIO_ID,
            'max_positions', :v_max_positions
        );
    end if;

    select count(*)
      into :v_candidate_count
      from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
     where IS_ELIGIBLE
       and to_number(replace(RUN_ID, 'T', '')) = :P_RUN_ID;

    if (v_candidate_count = 0) then
        return object_construct(
            'status', 'NO_ELIGIBLE_SIGNALS',
            'run_id', :P_RUN_ID,
            'portfolio_id', :P_PORTFOLIO_ID,
            'inserted_count', 0
        );
    end if;

    v_selected_count := least(v_candidate_count, v_max_positions);
    v_target_weight := least(1.0 / v_selected_count, v_max_position_pct);

    insert into MIP.AGENT_OUT.ORDER_PROPOSALS (
        RUN_ID,
        PORTFOLIO_ID,
        SYMBOL,
        MARKET_TYPE,
        SIDE,
        TARGET_WEIGHT,
        SOURCE_SIGNALS,
        RATIONALE,
        STATUS
    )
    select
        :P_RUN_ID,
        :P_PORTFOLIO_ID,
        s.SYMBOL,
        s.MARKET_TYPE,
        'BUY',
        :v_target_weight,
        object_construct(
            'recommendation_id', s.RECOMMENDATION_ID,
            'pattern_id', s.PATTERN_ID,
            'ts', s.TS,
            'score', s.SCORE,
            'interval_minutes', s.INTERVAL_MINUTES,
            'run_id', s.RUN_ID
        ),
        object_construct(
            'strategy', 'equal_weight',
            'max_positions', :v_max_positions,
            'max_position_pct', :v_max_position_pct
        ),
        'PROPOSED'
    from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
    where s.IS_ELIGIBLE
      and to_number(replace(s.RUN_ID, 'T', '')) = :P_RUN_ID
    qualify row_number() over (
        order by s.SCORE desc, s.TS desc, s.SYMBOL
    ) <= :v_max_positions;

    get diagnostics v_inserted_count = row_count;

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :P_RUN_ID,
        'portfolio_id', :P_PORTFOLIO_ID,
        'inserted_count', :v_inserted_count,
        'target_weight', :v_target_weight
    );
end;
$$;

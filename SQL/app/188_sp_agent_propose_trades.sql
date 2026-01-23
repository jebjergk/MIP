-- 188_sp_agent_propose_trades.sql
-- Purpose: Deterministic agent proposal generator for eligible signals

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_AGENT_PROPOSE_TRADES(
    P_PORTFOLIO_ID number,
    P_RUN_ID number
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
    v_dedup_skipped_count number := 0;
    v_target_weight float;
    v_run_id_string string := to_varchar(:P_RUN_ID);
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
       and (
           RUN_ID = :v_run_id_string
           or try_to_number(replace(RUN_ID, 'T', '')) = :P_RUN_ID
       );

    if (v_candidate_count = 0) then
        return object_construct(
            'status', 'NO_ELIGIBLE_SIGNALS',
            'run_id', :P_RUN_ID,
            'portfolio_id', :P_PORTFOLIO_ID,
            'proposal_candidates', 0,
            'proposal_inserted', 0,
            'proposal_dedup_skipped', 0
        );
    end if;

    v_selected_count := least(v_candidate_count, v_max_positions);
    v_target_weight := least(1.0 / v_selected_count, v_max_position_pct);

    merge into MIP.AGENT_OUT.ORDER_PROPOSALS as target
    using (
        select
            :P_RUN_ID as RUN_ID,
            :P_PORTFOLIO_ID as PORTFOLIO_ID,
            s.SYMBOL,
            s.MARKET_TYPE,
            s.INTERVAL_MINUTES,
            'BUY' as SIDE,
            :v_target_weight as TARGET_WEIGHT,
            s.RECOMMENDATION_ID,
            s.TS as SIGNAL_TS,
            s.PATTERN_ID as SIGNAL_PATTERN_ID,
            s.INTERVAL_MINUTES as SIGNAL_INTERVAL_MINUTES,
            s.RUN_ID as SIGNAL_RUN_ID,
            s.DETAILS as SIGNAL_SNAPSHOT,
            object_construct(
                'recommendation_id', s.RECOMMENDATION_ID,
                'pattern_id', s.PATTERN_ID,
                'ts', s.TS,
                'score', s.SCORE,
                'interval_minutes', s.INTERVAL_MINUTES,
                'run_id', s.RUN_ID
            ) as SOURCE_SIGNALS,
            object_construct(
                'strategy', 'equal_weight',
                'max_positions', :v_max_positions,
                'max_position_pct', :v_max_position_pct
            ) as RATIONALE
        from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
        where s.IS_ELIGIBLE
          and (
              s.RUN_ID = :v_run_id_string
              or try_to_number(replace(s.RUN_ID, 'T', '')) = :P_RUN_ID
          )
        qualify row_number() over (
            order by s.SCORE desc, s.TS desc, s.SYMBOL
        ) <= :v_max_positions
    ) as source
    on target.PORTFOLIO_ID = source.PORTFOLIO_ID
   and target.RUN_ID = source.RUN_ID
   and target.RECOMMENDATION_ID = source.RECOMMENDATION_ID
    when not matched then
        insert (
            RUN_ID,
            PORTFOLIO_ID,
            SYMBOL,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            SIDE,
            TARGET_WEIGHT,
            RECOMMENDATION_ID,
            SIGNAL_TS,
            SIGNAL_PATTERN_ID,
            SIGNAL_INTERVAL_MINUTES,
            SIGNAL_RUN_ID,
            SIGNAL_SNAPSHOT,
            SOURCE_SIGNALS,
            RATIONALE,
            STATUS
        )
        values (
            source.RUN_ID,
            source.PORTFOLIO_ID,
            source.SYMBOL,
            source.MARKET_TYPE,
            source.INTERVAL_MINUTES,
            source.SIDE,
            source.TARGET_WEIGHT,
            source.RECOMMENDATION_ID,
            source.SIGNAL_TS,
            source.SIGNAL_PATTERN_ID,
            source.SIGNAL_INTERVAL_MINUTES,
            source.SIGNAL_RUN_ID,
            source.SIGNAL_SNAPSHOT,
            source.SOURCE_SIGNALS,
            source.RATIONALE,
            'PROPOSED'
        );

    v_inserted_count := SQLROWCOUNT;
    v_dedup_skipped_count := greatest(:v_selected_count - :v_inserted_count, 0);

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :P_RUN_ID,
        'portfolio_id', :P_PORTFOLIO_ID,
        'proposal_candidates', :v_candidate_count,
        'proposal_inserted', :v_inserted_count,
        'proposal_dedup_skipped', :v_dedup_skipped_count,
        'target_weight', :v_target_weight
    );
end;
$$;

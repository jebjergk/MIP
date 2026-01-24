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
    v_open_positions number := 0;
    v_remaining_capacity number := 0;
    v_inserted_count number := 0;
    v_selected_count number := 0;
    v_dedup_skipped_count number := 0;
    v_overflow_count number := 0;
    v_target_weight float := 0.05;
    v_run_id_string string := to_varchar(:P_RUN_ID);
    v_current_bar_index number := 0;
    v_overflow_preview variant := array_construct();
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
    v_max_position_pct := coalesce(v_max_position_pct, 0.05);

    if (v_max_positions <= 0) then
        return object_construct(
            'status', 'ERROR',
            'message', 'Invalid max positions configuration',
            'portfolio_id', :P_PORTFOLIO_ID,
            'max_positions', :v_max_positions
        );
    end if;

    select coalesce(
               max(bar_index),
               0
           )
      into :v_current_bar_index
      from (
        select
            BAR_INDEX
        from MIP.MART.V_BAR_INDEX
        qualify row_number() over (
            partition by TS
            order by BAR_INDEX
        ) = 1
        order by TS desc
        limit 1
    );

    select count(*)
      into :v_open_positions
      from MIP.APP.PORTFOLIO_POSITIONS p
     where p.PORTFOLIO_ID = :P_PORTFOLIO_ID
       and p.HOLD_UNTIL_INDEX >= :v_current_bar_index;

    v_remaining_capacity := greatest(:v_max_positions - :v_open_positions, 0);

    select count(*)
      into :v_candidate_count
      from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
     where IS_ELIGIBLE
       and (
           RUN_ID = :v_run_id_string
           or try_to_number(replace(RUN_ID, 'T', '')) = :P_RUN_ID
       );

    v_overflow_count := greatest(:v_candidate_count - :v_remaining_capacity, 0);

    if (v_candidate_count = 0) then
        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS
        )
        values (
            current_timestamp(),
            :v_run_id_string,
            'AGENT',
            'SP_AGENT_PROPOSE_TRADES',
            'INFO',
            0,
            object_construct(
                'portfolio_id', :P_PORTFOLIO_ID,
                'max_positions', :v_max_positions,
                'open_positions', :v_open_positions,
                'remaining_capacity', :v_remaining_capacity,
                'candidate_count', 0,
                'proposed_count', 0,
                'overflow_count', 0
            )
        );

        return object_construct(
            'status', 'NO_ELIGIBLE_SIGNALS',
            'run_id', :P_RUN_ID,
            'portfolio_id', :P_PORTFOLIO_ID,
            'max_positions', :v_max_positions,
            'open_positions', :v_open_positions,
            'remaining_capacity', :v_remaining_capacity,
            'proposal_candidates', 0,
            'proposal_selected', 0,
            'proposal_inserted', 0,
            'proposal_dedup_skipped', 0,
            'overflow_count', 0,
            'target_weight', :v_target_weight
        );
    end if;

    if (v_remaining_capacity = 0) then
        v_overflow_preview := (
            with ranked_candidates as (
                select
                    s.SYMBOL,
                    s.PATTERN_ID,
                    s.SCORE,
                    s.RECOMMENDATION_ID,
                    row_number() over (
                        order by
                            case s.TRUST_LABEL
                                when 'TRUSTED' then 3
                                when 'WATCH' then 2
                                else 1
                            end desc,
                            s.SCORE desc,
                            s.RECOMMENDATION_ID
                    ) as RN
                from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
                where s.IS_ELIGIBLE
                  and (
                      s.RUN_ID = :v_run_id_string
                      or try_to_number(replace(s.RUN_ID, 'T', '')) = :P_RUN_ID
                  )
            )
            select coalesce(
                array_agg(
                    object_construct(
                        'symbol', SYMBOL,
                        'pattern_id', PATTERN_ID,
                        'score', SCORE,
                        'recommendation_id', RECOMMENDATION_ID
                    )
                ),
                array_construct()
            )
            from ranked_candidates
            where RN <= 10
        );

        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS
        )
        values (
            current_timestamp(),
            :v_run_id_string,
            'AGENT',
            'SP_AGENT_PROPOSE_TRADES',
            'INFO',
            0,
            object_construct(
                'portfolio_id', :P_PORTFOLIO_ID,
                'max_positions', :v_max_positions,
                'open_positions', :v_open_positions,
                'remaining_capacity', 0,
                'candidate_count', :v_candidate_count,
                'proposed_count', 0,
                'overflow_count', :v_candidate_count,
                'overflow_preview', :v_overflow_preview
            )
        );

        return object_construct(
            'status', 'NO_CAPACITY',
            'run_id', :P_RUN_ID,
            'portfolio_id', :P_PORTFOLIO_ID,
            'max_positions', :v_max_positions,
            'open_positions', :v_open_positions,
            'remaining_capacity', 0,
            'proposal_candidates', :v_candidate_count,
            'proposal_selected', 0,
            'proposal_inserted', 0,
            'proposal_dedup_skipped', 0,
            'overflow_count', :v_candidate_count,
            'target_weight', :v_target_weight,
            'overflow_preview', :v_overflow_preview
        );
    end if;

    v_selected_count := least(:v_candidate_count, :v_remaining_capacity);

    v_overflow_preview := (
        with ranked_candidates as (
            select
                s.SYMBOL,
                s.PATTERN_ID,
                s.SCORE,
                s.RECOMMENDATION_ID,
                row_number() over (
                    order by
                        case s.TRUST_LABEL
                            when 'TRUSTED' then 3
                            when 'WATCH' then 2
                            else 1
                        end desc,
                        s.SCORE desc,
                        s.RECOMMENDATION_ID
                ) as RN
            from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
            where s.IS_ELIGIBLE
              and (
                  s.RUN_ID = :v_run_id_string
                  or try_to_number(replace(s.RUN_ID, 'T', '')) = :P_RUN_ID
              )
        )
        select coalesce(
            array_agg(
                object_construct(
                    'symbol', SYMBOL,
                    'pattern_id', PATTERN_ID,
                    'score', SCORE,
                    'recommendation_id', RECOMMENDATION_ID
                )
            ),
            array_construct()
        )
        from ranked_candidates
        where RN > :v_remaining_capacity
          and RN <= :v_remaining_capacity + 10
    );

    merge into MIP.AGENT_OUT.ORDER_PROPOSALS as target
    using (
        with ranked_candidates as (
            select
                s.*,
                row_number() over (
                    order by
                        case s.TRUST_LABEL
                            when 'TRUSTED' then 3
                            when 'WATCH' then 2
                            else 1
                        end desc,
                        s.SCORE desc,
                        s.RECOMMENDATION_ID
                ) as RN
            from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
            where s.IS_ELIGIBLE
              and (
                  s.RUN_ID = :v_run_id_string
                  or try_to_number(replace(s.RUN_ID, 'T', '')) = :P_RUN_ID
              )
        )
        select
            :P_RUN_ID as RUN_ID,
            :P_PORTFOLIO_ID as PORTFOLIO_ID,
            s.SYMBOL,
            s.MARKET_TYPE,
            s.INTERVAL_MINUTES,
            case s.RECOMMENDED_ACTION
                when 'DISABLE' then 'SELL'
                else 'BUY'
            end as SIDE,
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
                'run_id', s.RUN_ID,
                'trust_label', s.TRUST_LABEL,
                'recommended_action', s.RECOMMENDED_ACTION
            ) as SOURCE_SIGNALS,
            object_construct(
                'strategy', 'capacity_aware_top_n',
                'max_positions', :v_max_positions,
                'open_positions', :v_open_positions,
                'remaining_capacity', :v_remaining_capacity,
                'max_position_pct', :v_max_position_pct,
                'selection_rank', s.RN
            ) as RATIONALE
        from ranked_candidates s
        where s.RN <= :v_remaining_capacity
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

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS
    )
    values (
        current_timestamp(),
        :v_run_id_string,
        'AGENT',
        'SP_AGENT_PROPOSE_TRADES',
        'INFO',
        :v_inserted_count,
        object_construct(
            'portfolio_id', :P_PORTFOLIO_ID,
            'max_positions', :v_max_positions,
            'open_positions', :v_open_positions,
            'remaining_capacity', :v_remaining_capacity,
            'candidate_count', :v_candidate_count,
            'proposed_count', :v_inserted_count,
            'selected_count', :v_selected_count,
            'overflow_count', :v_overflow_count,
            'overflow_preview', :v_overflow_preview
        )
    );

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :P_RUN_ID,
        'portfolio_id', :P_PORTFOLIO_ID,
        'max_positions', :v_max_positions,
        'open_positions', :v_open_positions,
        'remaining_capacity', :v_remaining_capacity,
        'proposal_candidates', :v_candidate_count,
        'proposal_selected', :v_selected_count,
        'proposal_inserted', :v_inserted_count,
        'proposal_dedup_skipped', :v_dedup_skipped_count,
        'overflow_count', :v_overflow_count,
        'target_weight', :v_target_weight,
        'overflow_preview', :v_overflow_preview
    );
end;
$$;

-- 188_sp_agent_propose_trades.sql
-- Purpose: Deterministic agent proposal generator. Uses V_TRUSTED_SIGNALS_LATEST_TS (trusted-gate v1).

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_AGENT_PROPOSE_TRADES(
    P_PORTFOLIO_ID number,
    P_RUN_ID string,   -- pipeline run id for deterministic tie-back to recommendations
    P_PARENT_RUN_ID string default null
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
    v_entries_blocked boolean := false;
    v_stop_reason string;
    v_allowed_actions string;
    v_inserted_count number := 0;
    v_selected_count number := 0;
    v_target_weight float := 0.05;
    v_run_id_string string := :P_RUN_ID;
    v_current_bar_index number := 0;
    v_max_new_stock number := 0;
    v_max_new_fx number := 0;
    v_available_stock number := 0;
    v_available_fx number := 0;
    v_skipped_held_count number := 0;
    v_selected_stock number := 0;
    v_selected_fx number := 0;
    v_selected_etf number := 0;
    v_candidate_count_raw number := 0;
    v_candidate_count_trusted number := 0;
    v_trusted_rejected_count number := 0;
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
      from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL p
     where p.PORTFOLIO_ID = :P_PORTFOLIO_ID
       and p.CURRENT_BAR_INDEX = :v_current_bar_index;

    v_remaining_capacity := greatest(:v_max_positions - :v_open_positions, 0);

    select
        coalesce(max(ENTRIES_BLOCKED), false),
        max(STOP_REASON),
        max(ALLOWED_ACTIONS)
      into :v_entries_blocked,
           :v_stop_reason,
           :v_allowed_actions
      from MIP.MART.V_PORTFOLIO_RISK_STATE
     where PORTFOLIO_ID = :P_PORTFOLIO_ID;

    if (v_entries_blocked) then
        select count(*)
          into :v_candidate_count_raw
          from MIP.MART.V_SIGNALS_LATEST_TS
         where RUN_ID = :v_run_id_string
            or try_to_number(replace(to_varchar(RUN_ID), 'T', '')) = :P_RUN_ID;

        select count(*)
          into :v_candidate_count_trusted
          from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS
         where RUN_ID = :v_run_id_string
            or try_to_number(replace(to_varchar(RUN_ID), 'T', '')) = :P_RUN_ID;

        v_candidate_count := :v_candidate_count_trusted;
        v_trusted_rejected_count := greatest(:v_candidate_count_raw - :v_candidate_count_trusted, 0);
        v_selected_count := least(:v_candidate_count, :v_remaining_capacity);

        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            PARENT_RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS
        )
        select
            current_timestamp(),
            :v_run_id_string,
            :P_PARENT_RUN_ID,
            'AGENT',
            'SP_AGENT_PROPOSE_TRADES',
            'SKIP_ENTRIES_BLOCKED',
            0,
            object_construct(
                'entries_blocked', :v_entries_blocked,
                'stop_reason', :v_stop_reason,
                'allowed_actions', :v_allowed_actions,
                'max_positions', :v_max_positions,
                'open_positions', :v_open_positions,
                'remaining_capacity', :v_remaining_capacity,
                'candidate_count', :v_candidate_count,
                'proposed_count', :v_selected_count,
                'candidate_count_raw', :v_candidate_count_raw,
                'candidate_count_trusted', :v_candidate_count_trusted,
                'trusted_rejected_count', :v_trusted_rejected_count
            );

        return object_construct(
            'status', 'SKIP_ENTRIES_BLOCKED',
            'run_id', :P_RUN_ID,
            'portfolio_id', :P_PORTFOLIO_ID,
            'entries_blocked', :v_entries_blocked,
            'stop_reason', :v_stop_reason,
            'allowed_actions', :v_allowed_actions,
            'max_positions', :v_max_positions,
            'open_positions', :v_open_positions,
            'remaining_capacity', :v_remaining_capacity,
            'proposal_candidates', :v_candidate_count,
            'proposal_selected', :v_selected_count,
            'proposal_inserted', 0,
            'target_weight', :v_target_weight
        );
    end if;

    select count(*)
      into :v_candidate_count_raw
      from MIP.MART.V_SIGNALS_LATEST_TS
     where RUN_ID = :v_run_id_string
        or try_to_number(replace(to_varchar(RUN_ID), 'T', '')) = :P_RUN_ID;

    select count(*)
      into :v_candidate_count_trusted
      from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS
     where RUN_ID = :v_run_id_string
        or try_to_number(replace(to_varchar(RUN_ID), 'T', '')) = :P_RUN_ID;

    v_candidate_count := :v_candidate_count_trusted;
    v_trusted_rejected_count := greatest(:v_candidate_count_raw - :v_candidate_count_trusted, 0);
    v_selected_count := least(:v_candidate_count, :v_remaining_capacity);

    if (v_candidate_count = 0 or v_remaining_capacity = 0) then
        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            PARENT_RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS
        )
        select
            current_timestamp(),
            :v_run_id_string,
            :P_PARENT_RUN_ID,
            'AGENT',
            'SP_AGENT_PROPOSE_TRADES',
            'INFO',
            0,
            object_construct(
                'max_positions', :v_max_positions,
                'open_positions', :v_open_positions,
                'remaining_capacity', :v_remaining_capacity,
                'candidate_count', :v_candidate_count,
                'proposed_count', :v_selected_count,
                'candidate_count_raw', :v_candidate_count_raw,
                'candidate_count_trusted', :v_candidate_count_trusted,
                'trusted_rejected_count', :v_trusted_rejected_count
            );

        return object_construct(
            'status', iff(v_candidate_count = 0, 'NO_ELIGIBLE_SIGNALS', 'NO_CAPACITY'),
            'run_id', :P_RUN_ID,
            'portfolio_id', :P_PORTFOLIO_ID,
            'max_positions', :v_max_positions,
            'open_positions', :v_open_positions,
            'remaining_capacity', :v_remaining_capacity,
            'proposal_candidates', :v_candidate_count,
            'proposal_selected', :v_selected_count,
            'proposal_inserted', 0,
            'target_weight', :v_target_weight
        );
    end if;

    v_max_new_stock := ceil(:v_remaining_capacity * 0.6);
    v_max_new_fx := :v_remaining_capacity - :v_max_new_stock;

    select
        coalesce(sum(iff(market_type_group = 'STOCK', 1, 0)), 0) as stock_count,
        coalesce(sum(iff(market_type_group = 'FX', 1, 0)), 0) as fx_count
      into :v_available_stock,
           :v_available_fx
      from (
        with eligible_candidates as (
            select
                s.RECOMMENDATION_ID,
                s.SYMBOL,
                s.MARKET_TYPE,
                s.SCORE,
                case
                    when s.MARKET_TYPE = 'FX' then 'FX'
                    else 'STOCK'
                end as MARKET_TYPE_GROUP
            from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
            where s.RUN_ID = :v_run_id_string
               or s.RUN_ID = :P_RUN_ID
        ),
        deduped_candidates as (
            select
                e.*
            from eligible_candidates e
            qualify row_number() over (
                partition by e.SYMBOL
                order by e.SCORE desc, e.RECOMMENDATION_ID
            ) = 1
        )
        select MARKET_TYPE_GROUP
        from deduped_candidates
    ) available_counts;

    if (:v_available_stock = 0 and :v_available_fx > 0) then
        v_max_new_stock := 0;
        v_max_new_fx := :v_remaining_capacity;
    elseif (:v_available_fx = 0 and :v_available_stock > 0) then
        v_max_new_stock := :v_remaining_capacity;
        v_max_new_fx := 0;
    end if;

    select
        coalesce(sum(iff(is_already_held = 1, 1, 0)), 0) as held_count
      into :v_skipped_held_count
      from (
        with held_symbols as (
            select distinct
                p.SYMBOL
            from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL p
            where p.PORTFOLIO_ID = :P_PORTFOLIO_ID
              and p.CURRENT_BAR_INDEX = :v_current_bar_index
        ),
        eligible_candidates as (
            select
                s.RECOMMENDATION_ID,
                s.SYMBOL,
                s.SCORE
            from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
            where s.RUN_ID = :v_run_id_string
               or s.RUN_ID = :P_RUN_ID
        ),
        deduped_candidates as (
            select
                e.*
            from eligible_candidates e
            qualify row_number() over (
                partition by e.SYMBOL
                order by e.SCORE desc, e.RECOMMENDATION_ID
            ) = 1
        ),
        prioritized as (
            select
                d.*,
                iff(h.SYMBOL is null, 0, 1) as is_already_held
            from deduped_candidates d
            left join held_symbols h
              on h.SYMBOL = d.SYMBOL
        )
        select is_already_held
        from prioritized
        where exists (
            select 1
            from prioritized p2
            where p2.is_already_held = 0
        )
    ) eligible_counts;

    merge into MIP.AGENT_OUT.ORDER_PROPOSALS as target
    using (
        with held_symbols as (
            select distinct
                p.SYMBOL
            from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL p
            where p.PORTFOLIO_ID = :P_PORTFOLIO_ID
              and p.CURRENT_BAR_INDEX = :v_current_bar_index
        ),
        eligible_candidates as (
            select
                s.*,
                case
                    when s.MARKET_TYPE = 'FX' then 'FX'
                    else 'STOCK'
                end as MARKET_TYPE_GROUP
            from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
            where s.RUN_ID = :v_run_id_string
               or s.RUN_ID = :P_RUN_ID
        ),
        deduped_candidates as (
            select
                e.*
            from eligible_candidates e
            qualify row_number() over (
                partition by e.SYMBOL
                order by e.SCORE desc, e.RECOMMENDATION_ID
            ) = 1
        ),
        prioritized as (
            select
                d.*,
                iff(h.SYMBOL is null, 0, 1) as HELD_PRIORITY
            from deduped_candidates d
            left join held_symbols h
              on h.SYMBOL = d.SYMBOL
        ),
        ranked as (
            select
                p.*,
                row_number() over (
                    order by
                        p.HELD_PRIORITY asc,
                        p.SCORE desc,
                        p.RECOMMENDATION_ID
                ) as OVERALL_RANK,
                row_number() over (
                    partition by p.MARKET_TYPE_GROUP
                    order by
                        p.HELD_PRIORITY asc,
                        p.SCORE desc,
                        p.RECOMMENDATION_ID
                ) as TYPE_RANK
            from prioritized p
        ),
        stock_pass as (
            select
                r.*
            from ranked r
            where r.MARKET_TYPE_GROUP = 'STOCK'
              and r.TYPE_RANK <= :v_max_new_stock
        ),
        fx_pass as (
            select
                r.*
            from ranked r
            where r.MARKET_TYPE_GROUP = 'FX'
              and r.TYPE_RANK <= :v_max_new_fx
        ),
        quota_selected as (
            select * from stock_pass
            union all
            select * from fx_pass
        ),
        quota_limited as (
            select
                q.*,
                row_number() over (
                    order by q.OVERALL_RANK
                ) as QUOTA_ORDER
            from quota_selected q
        ),
        primary_selected as (
            select
                q.*
            from quota_limited q
            where q.QUOTA_ORDER <= :v_remaining_capacity
        ),
        remaining_slots as (
            select greatest(
                :v_remaining_capacity - (select count(*) from primary_selected),
                0
            ) as SLOTS
        ),
        backfill_candidates as (
            select
                r.*
            from ranked r
            left join primary_selected p
              on p.RECOMMENDATION_ID = r.RECOMMENDATION_ID
            where p.RECOMMENDATION_ID is null
        ),
        backfill_ranked as (
            select
                b.*,
                row_number() over (
                    order by b.OVERALL_RANK
                ) as BACKFILL_RANK
            from backfill_candidates b
        ),
        backfill_selected as (
            select
                b.*
            from backfill_ranked b
            cross join remaining_slots rs
            where b.BACKFILL_RANK <= rs.SLOTS
        ),
        final_selected as (
            select * from primary_selected
            union all
            select * from backfill_selected
        ),
        final_ranked as (
            select
                f.*,
                row_number() over (
                    order by f.OVERALL_RANK
                ) as SELECTION_RANK
            from final_selected f
        )
        select
            :P_RUN_ID as RUN_ID_VARCHAR,
            :P_PORTFOLIO_ID as PORTFOLIO_ID,
            s.SYMBOL,
            s.MARKET_TYPE,
            s.INTERVAL_MINUTES,
            'BUY' as SIDE,
            :v_target_weight as TARGET_WEIGHT,
            s.RECOMMENDATION_ID,
            s.SIGNAL_TS,
            s.PATTERN_ID as SIGNAL_PATTERN_ID,
            s.INTERVAL_MINUTES as SIGNAL_INTERVAL_MINUTES,
            s.RUN_ID as SIGNAL_RUN_ID,
            s.DETAILS as SIGNAL_SNAPSHOT,
            object_construct(
                'recommendation_id', s.RECOMMENDATION_ID,
                'pattern_id', s.PATTERN_ID,
                'ts', s.SIGNAL_TS,
                'score', s.SCORE,
                'interval_minutes', s.INTERVAL_MINUTES,
                'run_id', s.RUN_ID,
                'trust_label', 'TRUSTED',
                'recommended_action', 'ENABLE',
                'held_priority', s.HELD_PRIORITY,
                'market_type_group', s.MARKET_TYPE_GROUP,
                'trust_reason', s.TRUST_REASON
            ) as SOURCE_SIGNALS,
            object_construct(
                'strategy', 'diversified_capacity_aware_top_n',
                'max_positions', :v_max_positions,
                'open_positions', :v_open_positions,
                'remaining_capacity', :v_remaining_capacity,
                'max_position_pct', :v_max_position_pct,
                'market_type_quota', object_construct(
                    'STOCK', :v_max_new_stock,
                    'FX', :v_max_new_fx
                ),
                'selection_rank', s.SELECTION_RANK
            ) as RATIONALE
        from final_ranked s
        where s.SELECTION_RANK <= :v_remaining_capacity
    ) as source
    on target.PORTFOLIO_ID = source.PORTFOLIO_ID
   and target.RUN_ID_VARCHAR = source.RUN_ID_VARCHAR
   and target.RECOMMENDATION_ID = source.RECOMMENDATION_ID
    when not matched then
        insert (
            RUN_ID_VARCHAR,
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
            source.RUN_ID_VARCHAR,
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

    select
        coalesce(sum(iff(market_type_group = 'STOCK', 1, 0)), 0) as stock_selected,
        coalesce(sum(iff(market_type_group = 'FX', 1, 0)), 0) as fx_selected,
        coalesce(sum(iff(market_type = 'ETF', 1, 0)), 0) as etf_selected
      into :v_selected_stock,
           :v_selected_fx,
           :v_selected_etf
      from (
        select distinct
            s.RECOMMENDATION_ID,
            case
                when s.MARKET_TYPE = 'FX' then 'FX'
                else 'STOCK'
            end as market_type_group,
            s.MARKET_TYPE
        from MIP.AGENT_OUT.ORDER_PROPOSALS s
        where s.PORTFOLIO_ID = :P_PORTFOLIO_ID
          and s.RUN_ID_VARCHAR = :P_RUN_ID
          and s.STATUS = 'PROPOSED'
    ) selected_counts;

    v_selected_count := least(:v_remaining_capacity, :v_selected_stock + :v_selected_fx);

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        PARENT_RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS
    )
    select
        current_timestamp(),
        :v_run_id_string,
        :P_PARENT_RUN_ID,
        'AGENT',
        'SP_AGENT_PROPOSE_TRADES',
        'INFO',
        :v_inserted_count,
        object_construct(
            'max_positions', :v_max_positions,
            'open_positions', :v_open_positions,
            'remaining_capacity', :v_remaining_capacity,
            'candidate_count', :v_candidate_count,
            'proposed_count', :v_selected_count,
            'candidate_count_raw', :v_candidate_count_raw,
            'candidate_count_trusted', :v_candidate_count_trusted,
            'trusted_rejected_count', :v_trusted_rejected_count,
            'picked_by_market_type', object_construct(
                'STOCK', :v_selected_stock,
                'FX', :v_selected_fx,
                'ETF', :v_selected_etf
            ),
            'skipped_held_count', :v_skipped_held_count
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
        'target_weight', :v_target_weight,
        'market_type_quota', object_construct(
            'STOCK', :v_max_new_stock,
            'FX', :v_max_new_fx
        ),
        'picked_by_market_type', object_construct(
            'STOCK', :v_selected_stock,
            'FX', :v_selected_fx,
            'ETF', :v_selected_etf
        ),
        'skipped_held_count', :v_skipped_held_count
    );
end;
$$;

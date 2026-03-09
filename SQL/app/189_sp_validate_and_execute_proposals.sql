-- 189_sp_validate_and_execute_proposals.sql
-- Purpose: Validate proposals, apply constraints, and execute paper trades

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_VALIDATE_AND_EXECUTE_PROPOSALS(
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
    v_profile variant;
    v_profile_id number;
    v_max_positions number;
    v_max_position_pct float;
    v_total_equity number(18,2);
    v_available_cash number(18,2);
    v_rejected_count number := 0;
    v_approved_count number := 0;
    v_executed_count number := 0;
    v_proposal_count number := 0;
    v_validation_counts variant;
    v_entries_blocked boolean := false;
    v_stop_reason string;
    v_allowed_actions string;
    v_run_id_string string := :P_RUN_ID;
    v_buy_proposals_blocked number := 0;
    v_exposure_rejected number := 0;
    v_position_rejected number := 0;
    v_committee_blocked number := 0;
    v_slippage_bps number(18,8);
    v_fee_bps number(18,8);
    v_min_fee number(18,8);
    v_spread_bps number(18,8);
    v_execution_price_interval_minutes number := 1;
begin
    v_profile := (
        select object_construct(
            'profile_id', p.PROFILE_ID,
            'max_positions', prof.MAX_POSITIONS,
            'max_position_pct', prof.MAX_POSITION_PCT
        )
        from MIP.APP.PORTFOLIO p
        left join MIP.APP.PORTFOLIO_PROFILE prof
          on prof.PROFILE_ID = p.PROFILE_ID
       where p.PORTFOLIO_ID = :P_PORTFOLIO_ID
    );

    v_profile_id := v_profile:profile_id::number;
    v_max_positions := v_profile:max_positions::number;
    v_max_position_pct := v_profile:max_position_pct::float;

    if (v_profile_id is null) then
        return object_construct(
            'status', 'ERROR',
            'message', 'Portfolio not found',
            'portfolio_id', :P_PORTFOLIO_ID
        );
    end if;

    v_max_positions := coalesce(v_max_positions, 5);
    v_max_position_pct := coalesce(v_max_position_pct, 1.0);

    select
        coalesce(try_to_number(max(case when CONFIG_KEY = 'SLIPPAGE_BPS' then CONFIG_VALUE end)), 2),
        coalesce(try_to_number(max(case when CONFIG_KEY = 'FEE_BPS' then CONFIG_VALUE end)), 1),
        coalesce(try_to_number(max(case when CONFIG_KEY = 'MIN_FEE' then CONFIG_VALUE end)), 0),
        coalesce(try_to_number(max(case when CONFIG_KEY = 'SPREAD_BPS' then CONFIG_VALUE end)), 0),
        coalesce(try_to_number(max(case when CONFIG_KEY = 'SIM_EXECUTION_PRICE_INTERVAL_MINUTES' then CONFIG_VALUE end)), 1)
      into v_slippage_bps,
           v_fee_bps,
           v_min_fee,
           v_spread_bps,
           v_execution_price_interval_minutes
      from MIP.APP.APP_CONFIG;

    -- CRIT-001: Entry gate enforcement - check if entries are blocked
    select
        coalesce(max(ENTRIES_BLOCKED), false),
        max(STOP_REASON),
        max(ALLOWED_ACTIONS)
      into :v_entries_blocked,
           :v_stop_reason,
           :v_allowed_actions
      from MIP.MART.V_PORTFOLIO_RISK_STATE
     where PORTFOLIO_ID = :P_PORTFOLIO_ID;

    -- When entry gate is active, reject all BUY-side proposals immediately (exits-only mode)
    if (v_entries_blocked) then
        select count(*)
          into :v_buy_proposals_blocked
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID_VARCHAR = :P_RUN_ID
           and PORTFOLIO_ID = :P_PORTFOLIO_ID
           and STATUS in ('PROPOSED', 'APPROVED')
           and SIDE = 'BUY';

        -- Reject all BUY proposals due to entry gate
        if (v_buy_proposals_blocked > 0) then
            update MIP.AGENT_OUT.ORDER_PROPOSALS
               set STATUS = 'REJECTED',
                   VALIDATION_ERRORS = array_construct_compact('ENTRY_GATE_BLOCKED', :v_stop_reason),
                   APPROVED_AT = null
             where RUN_ID_VARCHAR = :P_RUN_ID
               and PORTFOLIO_ID = :P_PORTFOLIO_ID
               and STATUS in ('PROPOSED', 'APPROVED')
               and SIDE = 'BUY';

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
                'SP_VALIDATE_AND_EXECUTE_PROPOSALS',
                'ENTRY_GATE_BLOCKED',
                :v_buy_proposals_blocked,
                object_construct(
                    'entries_blocked', :v_entries_blocked,
                    'stop_reason', :v_stop_reason,
                    'allowed_actions', :v_allowed_actions,
                    'buy_proposals_rejected', :v_buy_proposals_blocked,
                    'portfolio_id', :P_PORTFOLIO_ID
                );
        end if;

        -- Note: SELL proposals are allowed to proceed (exits-only mode)
        -- Continue with validation for SELL proposals and any remaining proposals
    end if;

    create or replace temporary table TMP_PROPOSAL_VALIDATION as
    with latest_prices as (
        select
            SYMBOL,
            MARKET_TYPE,
            CLOSE,
            row_number() over (
                partition by SYMBOL, MARKET_TYPE
                order by
                    case when INTERVAL_MINUTES = :v_execution_price_interval_minutes then 0 else 1 end,
                    TS desc
            ) as rn
        from MIP.MART.MARKET_BARS
        where INTERVAL_MINUTES in (:v_execution_price_interval_minutes, 1440)
    ),
    proposals as (
        select
            p.*,
            row_number() over (
                order by p.PROPOSED_AT, p.PROPOSAL_ID
            ) as proposal_rank
        from MIP.AGENT_OUT.ORDER_PROPOSALS p
        where p.RUN_ID_VARCHAR = :P_RUN_ID
          and p.PORTFOLIO_ID = :P_PORTFOLIO_ID
          and p.STATUS = 'PROPOSED'
    ),
    open_positions as (
        select
            SYMBOL,
            MARKET_TYPE,
            count(*) as open_positions
        from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
        where PORTFOLIO_ID = :P_PORTFOLIO_ID
        group by SYMBOL, MARKET_TYPE
    ),
    proposal_dupes as (
        select
            SYMBOL,
            MARKET_TYPE,
            count(*) as proposal_count,
            count(distinct SIDE) as side_count
        from proposals
        group by SYMBOL, MARKET_TYPE
    )
    select
        p.PROPOSAL_ID,
        p.proposal_rank,
        p.TARGET_WEIGHT,
        p.SYMBOL,
        p.MARKET_TYPE,
        p.INTERVAL_MINUTES,
        p.SIDE,
        p.SOURCE_SIGNALS,
        v.SYMBOL as eligible_symbol,
        v.IS_ELIGIBLE as eligible_flag,
        lp.CLOSE as latest_price,
        op.open_positions,
        pd.proposal_count,
        pd.side_count,
        array_construct_compact(
            -- CRIT-001: Entry gate check - reject BUY proposals when entries_blocked=true
            iff(:v_entries_blocked and p.SIDE = 'BUY', 'ENTRY_GATE_BLOCKED', null),
            iff(:v_entries_blocked and p.SIDE = 'BUY', :v_stop_reason, null),
            iff(p.RECOMMENDATION_ID is null, 'MISSING_RECOMMENDATION_ID', null),
            iff(p.RECOMMENDATION_ID is not null and v.RECOMMENDATION_ID is null, 'NO_SIGNAL_MATCH', null),
            iff(v.RECOMMENDATION_ID is not null and not v.IS_ELIGIBLE, 'INELIGIBLE_SIGNAL', null),
            iff(p.TARGET_WEIGHT > :v_max_position_pct, 'EXCEEDS_MAX_POSITION_PCT', null),
            iff(p.proposal_rank > :v_max_positions, 'EXCEEDS_MAX_POSITIONS', null),
            iff(p.SIDE = 'BUY' and coalesce(op.open_positions, 0) > 0, 'ALREADY_OPEN_POSITION', null),
            iff(p.SIDE = 'SELL' and coalesce(op.open_positions, 0) = 0, 'NO_OPEN_POSITION', null),
            iff(coalesce(pd.proposal_count, 0) > 1, 'DUPLICATE_SYMBOL_PROPOSAL', null),
            iff(coalesce(pd.side_count, 0) > 1, 'CONFLICTING_SIDE_PROPOSALS', null),
            iff(lp.CLOSE is null, 'MISSING_PRICE', null)
        ) as validation_errors
    from proposals p
    left join MIP.APP.V_SIGNALS_ELIGIBLE_TODAY v
      on v.RECOMMENDATION_ID = p.RECOMMENDATION_ID
    left join open_positions op
      on op.SYMBOL = p.SYMBOL
     and op.MARKET_TYPE = p.MARKET_TYPE
    left join proposal_dupes pd
      on pd.SYMBOL = p.SYMBOL
     and pd.MARKET_TYPE = p.MARKET_TYPE
    left join latest_prices lp
      on lp.SYMBOL = p.SYMBOL
     and lp.MARKET_TYPE = p.MARKET_TYPE
     and lp.rn = 1;

    v_proposal_count := coalesce(
        (select count(*) from TMP_PROPOSAL_VALIDATION),
        0
    );

    v_validation_counts := (
        select object_construct(
            'rejected', count_if(array_size(validation_errors) > 0),
            'approved', count_if(array_size(validation_errors) = 0)
        )
        from TMP_PROPOSAL_VALIDATION
    );

    v_rejected_count := coalesce(v_validation_counts:rejected::number, 0);
    v_approved_count := coalesce(v_validation_counts:approved::number, 0);

    update MIP.AGENT_OUT.ORDER_PROPOSALS as p
       set STATUS = 'REJECTED',
           VALIDATION_ERRORS = v.validation_errors
      from TMP_PROPOSAL_VALIDATION v
     where p.PROPOSAL_ID = v.PROPOSAL_ID
       and array_size(v.validation_errors) > 0;

    -- Simulation committee enrichment:
    -- run a structured multi-agent-style decision prompt per valid proposal and
    -- apply committee outputs for enter/size/target/hold/early-exit.
    -- Any failure in this block must fail open to deterministic behavior.
    begin
        create or replace temporary table TMP_PROPOSAL_COMMITTEE as
        with candidate as (
            select
                p.PROPOSAL_ID,
                p.SYMBOL,
                p.MARKET_TYPE,
                p.SIDE,
                p.TARGET_WEIGHT,
                p.SOURCE_SIGNALS,
                p.RATIONALE
            from MIP.AGENT_OUT.ORDER_PROPOSALS p
            join TMP_PROPOSAL_VALIDATION v
              on v.PROPOSAL_ID = p.PROPOSAL_ID
            where p.RUN_ID_VARCHAR = :P_RUN_ID
              and p.PORTFOLIO_ID = :P_PORTFOLIO_ID
              and p.STATUS = 'PROPOSED'
              and array_size(v.validation_errors) = 0
        ),
        llm_raw as (
            select
                c.PROPOSAL_ID,
                snowflake.cortex.complete(
                    'claude-3-5-sonnet',
                    'You are a simulation trade committee. Return ONLY JSON with keys: '
                    || '{"should_enter":true|false,'
                    || '"size_factor":0.0-1.0,'
                    || '"target_return":number,'
                    || '"hold_bars":integer,'
                    || '"early_exit_target_return":number,'
                    || '"summary":"...",'
                    || '"reason_codes":["..."],'
                    || '"agent_dialogue":[{"role":"...","message":"..."}]}'
                    || ' Context: '
                    || to_json(
                        object_construct(
                            'symbol', c.SYMBOL,
                            'market_type', c.MARKET_TYPE,
                            'side', c.SIDE,
                            'target_weight', c.TARGET_WEIGHT,
                            'source_signals', c.SOURCE_SIGNALS,
                            'rationale', c.RATIONALE,
                            'parallel_worlds_evidence', object_construct(
                                'as_of_ts', (
                                    select max(d.AS_OF_TS)
                                    from MIP.MART.V_PARALLEL_WORLD_DIFF d
                                    where d.PORTFOLIO_ID = :P_PORTFOLIO_ID
                                ),
                                'top_outperformers', (
                                    select array_agg(
                                        object_construct(
                                            'scenario_name', x.SCENARIO_NAME,
                                            'scenario_type', x.SCENARIO_TYPE,
                                            'pnl_delta', x.PNL_DELTA,
                                            'return_pct_delta', x.RETURN_PCT_DELTA,
                                            'drawdown_delta', x.DRAWDOWN_DELTA
                                        )
                                    ) within group (order by x.PNL_DELTA desc)
                                    from (
                                        select
                                            d.SCENARIO_NAME,
                                            d.SCENARIO_TYPE,
                                            d.PNL_DELTA,
                                            d.RETURN_PCT_DELTA,
                                            d.DRAWDOWN_DELTA
                                        from MIP.MART.V_PARALLEL_WORLD_DIFF d
                                        where d.PORTFOLIO_ID = :P_PORTFOLIO_ID
                                          and d.AS_OF_TS = (
                                              select max(d2.AS_OF_TS)
                                              from MIP.MART.V_PARALLEL_WORLD_DIFF d2
                                              where d2.PORTFOLIO_ID = :P_PORTFOLIO_ID
                                          )
                                          and coalesce(d.OUTPERFORMED, false) = true
                                        order by d.PNL_DELTA desc
                                        limit 3
                                    ) x
                                ),
                                'latest_recommendations', (
                                    select array_agg(
                                        object_construct(
                                            'type', y.RECOMMENDATION_TYPE,
                                            'domain', y.DOMAIN,
                                            'recommended_value', y.RECOMMENDED_VALUE,
                                            'expected_daily_delta', y.EXPECTED_DAILY_DELTA,
                                            'confidence_class', y.CONFIDENCE_CLASS
                                        )
                                    ) within group (order by y.REC_RANK)
                                    from (
                                        select
                                            r.RECOMMENDATION_TYPE,
                                            r.DOMAIN,
                                            r.RECOMMENDED_VALUE,
                                            r.EXPECTED_DAILY_DELTA,
                                            r.CONFIDENCE_CLASS,
                                            r.REC_RANK
                                        from MIP.MART.V_PW_RECOMMENDATIONS r
                                        where r.PORTFOLIO_ID = :P_PORTFOLIO_ID
                                          and r.AS_OF_TS = (
                                              select max(r2.AS_OF_TS)
                                              from MIP.MART.V_PW_RECOMMENDATIONS r2
                                              where r2.PORTFOLIO_ID = :P_PORTFOLIO_ID
                                          )
                                        order by r.REC_RANK
                                        limit 3
                                    ) y
                                )
                            )
                        )
                    )
                ) as RESPONSE
            from candidate c
        ),
        parsed as (
            select
                r.PROPOSAL_ID,
                try_parse_json(
                    regexp_replace(
                        regexp_replace(
                            trim(
                                coalesce(
                                    r.RESPONSE:choices[0]:messages::string,
                                    r.RESPONSE:choices[0]:message:content::string,
                                    r.RESPONSE::string
                                )
                            ),
                            '^```json\\s*',
                            ''
                        ),
                        '\\s*```$',
                        ''
                    )
                ) as OUT_JSON
            from llm_raw r
        )
        select
            p.PROPOSAL_ID,
            coalesce(p.OUT_JSON:should_enter::boolean, true) as SHOULD_ENTER,
            least(greatest(coalesce(p.OUT_JSON:size_factor::float, 1.0), 0.0), 1.0) as SIZE_FACTOR,
            p.OUT_JSON:target_return::float as TARGET_RETURN,
            p.OUT_JSON:hold_bars::number as HOLD_BARS,
            p.OUT_JSON:early_exit_target_return::float as EARLY_EXIT_TARGET_RETURN,
            coalesce(p.OUT_JSON:summary::string, 'Committee default (fallback).') as SUMMARY,
            coalesce(p.OUT_JSON:reason_codes, array_construct()) as REASON_CODES,
            coalesce(p.OUT_JSON:agent_dialogue, array_construct()) as AGENT_DIALOGUE,
            coalesce(p.OUT_JSON, object_construct()) as OUT_JSON
        from parsed p;
    exception
        when other then
            create or replace temporary table TMP_PROPOSAL_COMMITTEE as
            select
                p.PROPOSAL_ID,
                true as SHOULD_ENTER,
                1.0 as SIZE_FACTOR,
                null::float as TARGET_RETURN,
                null::number as HOLD_BARS,
                null::float as EARLY_EXIT_TARGET_RETURN,
                'Committee fallback: deterministic path (Cortex unavailable).' as SUMMARY,
                array_construct('COMMITTEE_FALLBACK') as REASON_CODES,
                array_construct() as AGENT_DIALOGUE,
                object_construct('fallback', true) as OUT_JSON
            from MIP.AGENT_OUT.ORDER_PROPOSALS p
            join TMP_PROPOSAL_VALIDATION v
              on v.PROPOSAL_ID = p.PROPOSAL_ID
            where p.RUN_ID_VARCHAR = :P_RUN_ID
              and p.PORTFOLIO_ID = :P_PORTFOLIO_ID
              and p.STATUS = 'PROPOSED'
              and array_size(v.validation_errors) = 0;
    end;

    -- Apply committee decision details to proposal rationale and size.
    update MIP.AGENT_OUT.ORDER_PROPOSALS p
       set TARGET_WEIGHT = greatest(
               0.01,
               least(
                   :v_max_position_pct,
                   coalesce(p.TARGET_WEIGHT, 0.05) * coalesce(c.SIZE_FACTOR, 1.0)
               )
           ),
           RATIONALE = object_insert(
               coalesce(p.RATIONALE, object_construct()),
               'sim_committee',
               object_construct(
                   'should_enter', c.SHOULD_ENTER,
                   'size_factor', c.SIZE_FACTOR,
                   'target_return', c.TARGET_RETURN,
                   'hold_bars', c.HOLD_BARS,
                   'early_exit_target_return', c.EARLY_EXIT_TARGET_RETURN,
                   'summary', c.SUMMARY,
                   'reason_codes', c.REASON_CODES,
                   'agent_dialogue', c.AGENT_DIALOGUE
               ),
               true
           )
      from TMP_PROPOSAL_COMMITTEE c
     where p.PROPOSAL_ID = c.PROPOSAL_ID
       and p.RUN_ID_VARCHAR = :P_RUN_ID
       and p.PORTFOLIO_ID = :P_PORTFOLIO_ID
       and p.STATUS = 'PROPOSED';

    -- Committee can block entering a position in simulation.
    update MIP.AGENT_OUT.ORDER_PROPOSALS p
       set STATUS = 'REJECTED',
           VALIDATION_ERRORS = array_cat(
               coalesce(p.VALIDATION_ERRORS, array_construct()),
               array_construct('SIM_COMMITTEE_BLOCKED')
           ),
           APPROVED_AT = null
      from TMP_PROPOSAL_COMMITTEE c
     where p.PROPOSAL_ID = c.PROPOSAL_ID
       and p.RUN_ID_VARCHAR = :P_RUN_ID
       and p.PORTFOLIO_ID = :P_PORTFOLIO_ID
       and p.STATUS = 'PROPOSED'
       and coalesce(c.SHOULD_ENTER, true) = false;

    v_committee_blocked := SQLROWCOUNT;

    update MIP.AGENT_OUT.ORDER_PROPOSALS as p
       set STATUS = 'APPROVED',
           APPROVED_AT = current_timestamp(),
           VALIDATION_ERRORS = null
      from TMP_PROPOSAL_VALIDATION v
      left join TMP_PROPOSAL_COMMITTEE c
        on c.PROPOSAL_ID = v.PROPOSAL_ID
     where p.PROPOSAL_ID = v.PROPOSAL_ID
       and p.RUN_ID_VARCHAR = :P_RUN_ID
       and p.PORTFOLIO_ID = :P_PORTFOLIO_ID
       and p.STATUS = 'PROPOSED'
       and array_size(v.validation_errors) = 0
       and coalesce(c.SHOULD_ENTER, true) = true;

    -- Recompute approval/rejection counts after committee step.
    select
        count_if(STATUS = 'APPROVED'),
        count_if(STATUS = 'REJECTED')
      into :v_approved_count,
           :v_rejected_count
      from MIP.AGENT_OUT.ORDER_PROPOSALS
     where RUN_ID_VARCHAR = :P_RUN_ID
       and PORTFOLIO_ID = :P_PORTFOLIO_ID;

    v_total_equity := coalesce(
        (
            select TOTAL_EQUITY
              from MIP.APP.PORTFOLIO_DAILY
             where PORTFOLIO_ID = :P_PORTFOLIO_ID
             order by TS desc
             limit 1
        ),
        (
            select STARTING_CASH
              from MIP.APP.PORTFOLIO
             where PORTFOLIO_ID = :P_PORTFOLIO_ID
        )
    );

    -- Use available CASH for position sizing, not total equity.
    -- Priority: latest trade's CASH_AFTER (actual money after last trade),
    -- then PORTFOLIO_DAILY cash, then starting cash as final fallback.
    -- IMPORTANT: Scope to active episode so we don't pull cash from a prior episode.
    let v_active_episode_id number := null;
    begin
        select EPISODE_ID into :v_active_episode_id
          from MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE
         where PORTFOLIO_ID = :P_PORTFOLIO_ID;
    exception
        when other then v_active_episode_id := null;
    end;

    v_available_cash := coalesce(
        (
            select CASH_AFTER
              from MIP.APP.PORTFOLIO_TRADES
             where PORTFOLIO_ID = :P_PORTFOLIO_ID
               and (EPISODE_ID = :v_active_episode_id or (:v_active_episode_id is null and EPISODE_ID is null))
             order by TRADE_TS desc, TRADE_ID desc
             limit 1
        ),
        (
            select CASH
              from MIP.APP.PORTFOLIO_DAILY
             where PORTFOLIO_ID = :P_PORTFOLIO_ID
               and (EPISODE_ID = :v_active_episode_id or (:v_active_episode_id is null and EPISODE_ID is null))
             order by TS desc
             limit 1
        ),
        (
            select STARTING_CASH
              from MIP.APP.PORTFOLIO
             where PORTFOLIO_ID = :P_PORTFOLIO_ID
        )
    );

    -- Phase 3.6: Strengthen position sizing validation - check individual position size and total exposure
    declare
        v_total_exposure_pct float;
        v_existing_exposure_pct float;
        v_combined_exposure_pct float;
        v_open_positions_count number;
        v_max_total_exposure_pct float;
        v_oversized_count number;
    begin
        -- Calculate total exposure from approved proposals in this run
        select coalesce(sum(TARGET_WEIGHT), 0)
          into v_total_exposure_pct
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID_VARCHAR = :P_RUN_ID
           and PORTFOLIO_ID = :P_PORTFOLIO_ID
           and STATUS = 'APPROVED'
           and SIDE = 'BUY';

        -- Calculate existing exposure from open positions (cost_basis / total_equity)
        select coalesce(sum(COST_BASIS) / nullif(:v_total_equity, 0), 0)
          into v_existing_exposure_pct
          from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
         where PORTFOLIO_ID = :P_PORTFOLIO_ID;

        v_combined_exposure_pct := v_existing_exposure_pct + v_total_exposure_pct;

        -- Get current open positions count
        select count(*)
          into v_open_positions_count
          from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
         where PORTFOLIO_ID = :P_PORTFOLIO_ID;

        -- Max total exposure = max_positions * max_position_pct (e.g., 5 positions * 10% = 50%)
        -- Or use 100% as ceiling if that calculation is too low
        v_max_total_exposure_pct := greatest(:v_max_positions * :v_max_position_pct, 1.0);

        -- First: Reject any individual proposal that exceeds single position limit
        select count(*)
          into v_oversized_count
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID_VARCHAR = :P_RUN_ID
           and PORTFOLIO_ID = :P_PORTFOLIO_ID
           and STATUS = 'APPROVED'
           and SIDE = 'BUY'
           and TARGET_WEIGHT > :v_max_position_pct * 1.01;

        if (v_oversized_count > 0) then
            update MIP.AGENT_OUT.ORDER_PROPOSALS
               set STATUS = 'REJECTED',
                   VALIDATION_ERRORS = array_construct('EXCEEDS_SINGLE_POSITION_LIMIT'),
                   APPROVED_AT = null
             where RUN_ID_VARCHAR = :P_RUN_ID
               and PORTFOLIO_ID = :P_PORTFOLIO_ID
               and STATUS = 'APPROVED'
               and SIDE = 'BUY'
               and TARGET_WEIGHT > :v_max_position_pct * 1.01;
        end if;

        -- Second: Check if combined exposure would exceed total limit
        if (v_combined_exposure_pct > v_max_total_exposure_pct * 1.01) then
            -- Reject excess proposals (keep oldest approved ones up to limit)
            update MIP.AGENT_OUT.ORDER_PROPOSALS
               set STATUS = 'REJECTED',
                   VALIDATION_ERRORS = array_construct('TOTAL_EXPOSURE_EXCEEDS_LIMIT'),
                   APPROVED_AT = null
             where RUN_ID_VARCHAR = :P_RUN_ID
               and PORTFOLIO_ID = :P_PORTFOLIO_ID
               and STATUS = 'APPROVED'
               and SIDE = 'BUY'
               and PROPOSAL_ID in (
                   select PROPOSAL_ID
                     from (
                       select 
                           PROPOSAL_ID,
                           sum(TARGET_WEIGHT) over (order by APPROVED_AT rows unbounded preceding) as cumulative_weight
                         from MIP.AGENT_OUT.ORDER_PROPOSALS
                        where RUN_ID_VARCHAR = :P_RUN_ID
                          and PORTFOLIO_ID = :P_PORTFOLIO_ID
                          and STATUS = 'APPROVED'
                          and SIDE = 'BUY'
                     )
                    where cumulative_weight + :v_existing_exposure_pct > :v_max_total_exposure_pct
               );

            v_exposure_rejected := SQLROWCOUNT;

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
                'SP_VALIDATE_AND_EXECUTE_PROPOSALS',
                'TOTAL_EXPOSURE_EXCEEDED',
                :v_exposure_rejected,
                object_construct(
                    'new_exposure_pct', :v_total_exposure_pct,
                    'existing_exposure_pct', :v_existing_exposure_pct,
                    'combined_exposure_pct', :v_combined_exposure_pct,
                    'max_total_exposure_pct', :v_max_total_exposure_pct,
                    'max_position_pct', :v_max_position_pct,
                    'open_positions', :v_open_positions_count,
                    'max_positions', :v_max_positions
                );
        end if;

        -- Also check position count limit
        if (v_open_positions_count + v_approved_count > v_max_positions) then
            -- Reject excess proposals beyond position limit
            create or replace temporary table TMP_EXCESS_POSITION_PROPOSALS as
            select PROPOSAL_ID
              from MIP.AGENT_OUT.ORDER_PROPOSALS
             where RUN_ID_VARCHAR = :P_RUN_ID
               and PORTFOLIO_ID = :P_PORTFOLIO_ID
               and STATUS = 'APPROVED'
               and SIDE = 'BUY'
            qualify row_number() over (
                order by PROPOSED_AT desc
            ) > greatest(:v_max_positions - :v_open_positions_count, 0);

            update MIP.AGENT_OUT.ORDER_PROPOSALS
               set STATUS = 'REJECTED',
                   VALIDATION_ERRORS = array_construct('EXCEEDS_MAX_POSITIONS'),
                   APPROVED_AT = null
              from TMP_EXCESS_POSITION_PROPOSALS x
             where RUN_ID_VARCHAR = :P_RUN_ID
               and PORTFOLIO_ID = :P_PORTFOLIO_ID
               and STATUS = 'APPROVED'
               and SIDE = 'BUY'
               and ORDER_PROPOSALS.PROPOSAL_ID = x.PROPOSAL_ID;

            v_position_rejected := SQLROWCOUNT;

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
                'SP_VALIDATE_AND_EXECUTE_PROPOSALS',
                'POSITION_COUNT_EXCEEDED',
                :v_position_rejected,
                object_construct(
                    'open_positions', :v_open_positions_count,
                    'approved_count', :v_approved_count,
                    'max_positions', :v_max_positions
                );
        end if;
    end;

    merge into MIP.APP.PORTFOLIO_TRADES as target
    using (
        with latest_prices as (
            select SYMBOL, MARKET_TYPE, CLOSE
            from (
                select
                    SYMBOL,
                    MARKET_TYPE,
                    CLOSE,
                    row_number() over (
                        partition by SYMBOL, MARKET_TYPE
                        order by
                            case when INTERVAL_MINUTES = :v_execution_price_interval_minutes then 0 else 1 end,
                            TS desc
                    ) as rn
                from MIP.MART.MARKET_BARS
                where INTERVAL_MINUTES in (:v_execution_price_interval_minutes, 1440)
            )
            where rn = 1
        ),
        approved_proposals as (
            select
                p.PROPOSAL_ID,
                p.SYMBOL,
                p.MARKET_TYPE,
                p.SIDE,
                p.TARGET_WEIGHT,
                p.SOURCE_SIGNALS:score::number as SCORE,
                p.APPROVED_AT
            from MIP.AGENT_OUT.ORDER_PROPOSALS p
            where p.RUN_ID_VARCHAR = :P_RUN_ID
              and p.PORTFOLIO_ID = :P_PORTFOLIO_ID
              and p.STATUS = 'APPROVED'
              and not (:v_entries_blocked and p.SIDE = 'BUY')
        ),
        buy_ranked as (
            select
                ap.PROPOSAL_ID,
                greatest(least(coalesce(ap.TARGET_WEIGHT, 0), 1), 0) as TARGET_WEIGHT,
                row_number() over (
                    order by ap.APPROVED_AT, ap.PROPOSAL_ID
                ) as BUY_RN
            from approved_proposals ap
            where ap.SIDE = 'BUY'
        ),
        buy_sized as (
            with recursive r as (
                select
                    br.PROPOSAL_ID,
                    br.BUY_RN,
                    br.TARGET_WEIGHT,
                    :v_available_cash::number(18,8) as CASH_BEFORE,
                    least(:v_available_cash::number(18,8), :v_available_cash::number(18,8) * br.TARGET_WEIGHT) as NOTIONAL
                from buy_ranked br
                where br.BUY_RN = 1

                union all

                select
                    br.PROPOSAL_ID,
                    br.BUY_RN,
                    br.TARGET_WEIGHT,
                    greatest(0, r.CASH_BEFORE - r.NOTIONAL) as CASH_BEFORE,
                    least(
                        greatest(0, r.CASH_BEFORE - r.NOTIONAL),
                        greatest(0, r.CASH_BEFORE - r.NOTIONAL) * br.TARGET_WEIGHT
                    ) as NOTIONAL
                from r
                join buy_ranked br
                  on br.BUY_RN = r.BUY_RN + 1
            )
            select
                PROPOSAL_ID,
                NOTIONAL
            from r
        ),
        base as (
            select
                ap.PROPOSAL_ID,
                :P_PORTFOLIO_ID as PORTFOLIO_ID,
                to_varchar(:P_RUN_ID) as RUN_ID,
                :v_active_episode_id as EPISODE_ID,
                ap.SYMBOL,
                ap.MARKET_TYPE,
                1440 as INTERVAL_MINUTES,
                current_timestamp() as TRADE_TS,
                ap.SIDE,
                lp.CLOSE as MID_PRICE,
                case
                    when ap.SIDE = 'BUY' then coalesce(bs.NOTIONAL, 0)
                    else :v_available_cash * greatest(least(coalesce(ap.TARGET_WEIGHT, 0), 1), 0)
                end as NOTIONAL,
                ap.SCORE
            from approved_proposals ap
            join latest_prices lp
              on lp.SYMBOL = ap.SYMBOL
             and lp.MARKET_TYPE = ap.MARKET_TYPE
            left join buy_sized bs
              on bs.PROPOSAL_ID = ap.PROPOSAL_ID
        ),
        priced as (
            select
                *,
                case
                    when MID_PRICE is null then null
                    when SIDE = 'BUY' then MID_PRICE * (1 + ((:v_slippage_bps + (:v_spread_bps / 2)) / 10000))
                    when SIDE = 'SELL' then MID_PRICE * (1 - ((:v_slippage_bps + (:v_spread_bps / 2)) / 10000))
                    else MID_PRICE
                end as PRICE
            from base
        ),
        costed as (
            select
                PROPOSAL_ID,
                PORTFOLIO_ID,
                RUN_ID,
                EPISODE_ID,
                SYMBOL,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                TRADE_TS,
                SIDE,
                PRICE,
                iff(PRICE is null or PRICE = 0, null, NOTIONAL / nullif(PRICE, 0)) as QUANTITY,
                NOTIONAL,
                null as REALIZED_PNL,
                greatest(coalesce(:v_min_fee, 0), abs(NOTIONAL) * :v_fee_bps / 10000) as FEE,
                SCORE
            from priced
        ),
        -- Compute cumulative cash: each trade subtracts/adds from running balance
        cumulative as (
            select
                PROPOSAL_ID,
                PORTFOLIO_ID,
                RUN_ID,
                EPISODE_ID,
                SYMBOL,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                TRADE_TS,
                SIDE,
                PRICE,
                QUANTITY,
                NOTIONAL,
                REALIZED_PNL,
                FEE,
                SCORE,
                :v_available_cash - sum(
                    case
                        when SIDE = 'BUY' then NOTIONAL + FEE
                        when SIDE = 'SELL' then -(NOTIONAL - FEE)
                        else 0
                    end
                ) over (order by PROPOSAL_ID rows between unbounded preceding and current row) as CASH_AFTER
            from costed
        )
        select
            PROPOSAL_ID,
            PORTFOLIO_ID,
            RUN_ID,
            EPISODE_ID,
            SYMBOL,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            TRADE_TS,
            SIDE,
            PRICE,
            QUANTITY,
            NOTIONAL,
            REALIZED_PNL,
            CASH_AFTER,
            SCORE
        from cumulative
    ) as source
    on target.PORTFOLIO_ID = source.PORTFOLIO_ID
       and target.PROPOSAL_ID = source.PROPOSAL_ID
    when not matched then
        insert (
            PROPOSAL_ID,
            PORTFOLIO_ID,
            RUN_ID,
            EPISODE_ID,
            SYMBOL,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            TRADE_TS,
            SIDE,
            PRICE,
            QUANTITY,
            NOTIONAL,
            REALIZED_PNL,
            CASH_AFTER,
            SCORE
        )
        values (
            source.PROPOSAL_ID,
            source.PORTFOLIO_ID,
            source.RUN_ID,
            source.EPISODE_ID,
            source.SYMBOL,
            source.MARKET_TYPE,
            source.INTERVAL_MINUTES,
            source.TRADE_TS,
            source.SIDE,
            source.PRICE,
            source.QUANTITY,
            source.NOTIONAL,
            source.REALIZED_PNL,
            source.CASH_AFTER,
            source.SCORE
        );

    update MIP.AGENT_OUT.ORDER_PROPOSALS
       set STATUS = 'EXECUTED',
           EXECUTED_AT = current_timestamp()
     where RUN_ID_VARCHAR = :P_RUN_ID
       and PORTFOLIO_ID = :P_PORTFOLIO_ID
       and STATUS = 'APPROVED'
       and not (:v_entries_blocked and SIDE = 'BUY');

    v_executed_count := SQLROWCOUNT;

    -- Create positions for BUY trades
    -- Position tracks what we hold and when to exit
    insert into MIP.APP.PORTFOLIO_POSITIONS (
        PORTFOLIO_ID,
        RUN_ID,
        EPISODE_ID,
        SYMBOL,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        ENTRY_TS,
        ENTRY_PRICE,
        QUANTITY,
        COST_BASIS,
        ENTRY_SCORE,
        ENTRY_INDEX,
        HOLD_UNTIL_INDEX
    )
    select
        t.PORTFOLIO_ID,
        t.RUN_ID,
        ae.EPISODE_ID,
        t.SYMBOL,
        t.MARKET_TYPE,
        t.INTERVAL_MINUTES,
        t.TRADE_TS as ENTRY_TS,
        t.PRICE as ENTRY_PRICE,
        t.QUANTITY,
        t.NOTIONAL as COST_BASIS,
        t.SCORE as ENTRY_SCORE,
        bi.BAR_INDEX as ENTRY_INDEX,
        bi.BAR_INDEX + coalesce(try_to_number(op.RATIONALE:sim_committee:hold_bars::string), ts.HORIZON_BARS, 5) as HOLD_UNTIL_INDEX
    from MIP.APP.PORTFOLIO_TRADES t
    cross join (
        select max(BAR_INDEX) as BAR_INDEX
        from MIP.MART.V_BAR_INDEX
        where INTERVAL_MINUTES = 1440
    ) bi
    left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE ae
      on ae.PORTFOLIO_ID = t.PORTFOLIO_ID
    left join MIP.AGENT_OUT.ORDER_PROPOSALS op
      on op.PROPOSAL_ID = t.PROPOSAL_ID
    left join MIP.MART.V_TRUSTED_SIGNALS ts
      on ts.PATTERN_ID = op.SIGNAL_PATTERN_ID
     and ts.MARKET_TYPE = t.MARKET_TYPE
     and ts.INTERVAL_MINUTES = t.INTERVAL_MINUTES
     and ts.IS_TRUSTED = true
    where t.RUN_ID = :P_RUN_ID
      and t.PORTFOLIO_ID = :P_PORTFOLIO_ID
      and t.SIDE = 'BUY'
      and not exists (
          -- Don't create duplicate positions
          select 1 from MIP.APP.PORTFOLIO_POSITIONS p
          where p.PORTFOLIO_ID = t.PORTFOLIO_ID
            and p.SYMBOL = t.SYMBOL
            and p.ENTRY_TS = t.TRADE_TS
      )
    qualify row_number() over (partition by t.TRADE_ID order by ts.AVG_RETURN desc, ts.HORIZON_BARS) = 1;

    -- Learning-to-Decision ledger append (non-fatal).
    begin
        call MIP.APP.SP_LEDGER_APPEND_EVENT(
            'DECISION_EVENT',
            'PROPOSAL_VALIDATION_EXECUTION',
            'SUCCESS',
            :P_RUN_ID,
            :P_PARENT_RUN_ID,
            :P_PORTFOLIO_ID,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            object_construct(
                'entries_blocked', :v_entries_blocked,
                'stop_reason', :v_stop_reason
            ),
            object_construct(
                'proposal_count', :v_proposal_count,
                'approved_count', :v_approved_count,
                'rejected_count', :v_rejected_count,
                'buy_proposals_blocked', :v_buy_proposals_blocked,
                'committee_blocked_count', :v_committee_blocked
            ),
            object_construct(
                'executed_count', :v_executed_count,
                'exposure_rejected', :v_exposure_rejected,
                'position_rejected', :v_position_rejected
            ),
            object_construct(
                'eligibility_blocked', :v_entries_blocked,
                'size_constraints_applied', true,
                'live_execution_candidates', :v_executed_count,
                'sim_committee_applied', true
            ),
            object_construct(
                'run_id', :P_RUN_ID,
                'portfolio_id', :P_PORTFOLIO_ID,
                'event_source', 'SP_VALIDATE_AND_EXECUTE_PROPOSALS'
            ),
            object_construct(
                'executed_count', :v_executed_count
            ),
            null
        );
    exception
        when other then null;
    end;

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :P_RUN_ID,
        'portfolio_id', :P_PORTFOLIO_ID,
        'proposal_count', :v_proposal_count,
        'approved_count', :v_approved_count,
        'rejected_count', :v_rejected_count,
        'executed_count', :v_executed_count,
        'entries_blocked', :v_entries_blocked,
        'stop_reason', :v_stop_reason,
        'allowed_actions', :v_allowed_actions,
        'buy_proposals_blocked', :v_buy_proposals_blocked,
        'committee_blocked_count', :v_committee_blocked
    );
end;
$$;

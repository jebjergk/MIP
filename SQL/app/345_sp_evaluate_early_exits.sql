-- 345_sp_evaluate_early_exits.sql
-- Purpose: Evaluate open daily positions for intraday early exit.
-- Two-stage policy:
--   Stage A: Has the position reached its payoff target (using 15-min bars)?
--   Stage B: Is there evidence of giveback / reversal after the peak?
-- Modes: SHADOW (log only), PAPER (close in sim), ACTIVE (live close).
-- Non-fatal: if no intraday bars exist for a position, skip it.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_EVALUATE_EARLY_EXITS(
    P_RUN_ID    varchar
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id            varchar := :P_RUN_ID;
    v_decision_ts       timestamp_ntz := current_timestamp();
    v_enabled           boolean := false;
    v_mode              varchar := 'SHADOW';
    v_interval_minutes  number := 15;
    v_payoff_mult       number(18,4) := 1.0;
    v_giveback_pct      number(18,4) := 0.40;
    v_no_new_high_bars  number := 3;
    v_quick_mins        number := 60;
    v_quick_giveback    number(18,4) := 0.25;
    v_portfolio_filter  varchar := 'ALL';
    v_market_types      varchar := 'STOCK,FX,ETF';
    v_slippage_bps      number(18,8) := 2;
    v_fee_bps           number(18,8) := 1;
    v_spread_bps        number(18,8) := 0;

    v_positions         resultset;
    v_pos_portfolio_id  number;
    v_pos_symbol        varchar;
    v_pos_market_type   varchar;
    v_pos_entry_ts      timestamp_ntz;
    v_pos_entry_price   number(18,8);
    v_pos_quantity      number(18,8);
    v_pos_cost_basis    number(18,8);
    v_pos_hold_until    number;
    v_pos_episode_id    number;
    v_pos_run_id        varchar;

    v_target_return     number(18,8);
    v_effective_target  number(18,8);
    v_current_price     number(18,8);
    v_bar_close_ts      timestamp_ntz;
    v_unrealized_return number(18,8);

    -- MFE / excursion tracking
    v_mfe_return        number(18,8);
    v_mfe_ts            timestamp_ntz;
    v_payoff_reached    boolean;
    v_first_hit_ts      timestamp_ntz;
    v_first_hit_return  number(18,8);
    v_hit_after_mins    number;

    -- Giveback
    v_giveback_from_peak number(18,8);
    v_giveback_ratio    number(18,8);
    v_no_high_count     number;
    v_giveback_triggered boolean;
    v_is_quick_payoff   boolean;
    v_active_giveback_threshold number(18,4);

    -- Decision
    v_exit_signal       boolean;
    v_exit_price        number(18,8);
    v_fees              number(18,8);
    v_early_pnl         number(18,4);
    v_hold_end_return   number(18,8);
    v_hold_end_pnl      number(18,4);
    v_pnl_delta         number(18,4);
    v_reason_codes      variant;

    -- Counters
    v_evaluated         number := 0;
    v_payoff_count      number := 0;
    v_signal_count      number := 0;
    v_executed_count    number := 0;
    v_skipped_count     number := 0;
    v_error_count       number := 0;

    -- State from prior runs
    v_prior_first_hit_ts    timestamp_ntz;
    v_prior_first_hit_ret   number(18,8);
    v_prior_mfe_return      number(18,8);
    v_prior_mfe_ts          timestamp_ntz;
    v_prior_fired           boolean;
begin
    -- Load config
    begin
        select
            coalesce(max(case when CONFIG_KEY = 'EARLY_EXIT_ENABLED' then try_to_boolean(CONFIG_VALUE) end), false),
            coalesce(max(case when CONFIG_KEY = 'EARLY_EXIT_MODE' then CONFIG_VALUE end), 'SHADOW'),
            coalesce(max(case when CONFIG_KEY = 'EARLY_EXIT_INTERVAL_MINUTES' then CONFIG_VALUE::number(18,0) end), 15),
            coalesce(max(case when CONFIG_KEY = 'EARLY_EXIT_PAYOFF_MULTIPLIER' then CONFIG_VALUE::number(18,4) end), 1.0),
            coalesce(max(case when CONFIG_KEY = 'EARLY_EXIT_GIVEBACK_PCT' then CONFIG_VALUE::number(18,4) end), 0.40),
            coalesce(max(case when CONFIG_KEY = 'EARLY_EXIT_NO_NEW_HIGH_BARS' then CONFIG_VALUE::number(18,0) end), 3),
            coalesce(max(case when CONFIG_KEY = 'EARLY_EXIT_QUICK_PAYOFF_MINS' then CONFIG_VALUE::number(18,0) end), 60),
            coalesce(max(case when CONFIG_KEY = 'EARLY_EXIT_QUICK_GIVEBACK_PCT' then CONFIG_VALUE::number(18,4) end), 0.25),
            coalesce(max(case when CONFIG_KEY = 'EARLY_EXIT_PORTFOLIOS' then CONFIG_VALUE end), 'ALL'),
            coalesce(max(case when CONFIG_KEY = 'EARLY_EXIT_MARKET_TYPES' then CONFIG_VALUE end), 'STOCK,FX,ETF'),
            coalesce(max(case when CONFIG_KEY = 'SLIPPAGE_BPS' then CONFIG_VALUE::number(18,4) end), 2),
            coalesce(max(case when CONFIG_KEY = 'FEE_BPS' then CONFIG_VALUE::number(18,4) end), 1),
            coalesce(max(case when CONFIG_KEY = 'SPREAD_BPS' then CONFIG_VALUE::number(18,4) end), 0)
        into :v_enabled, :v_mode, :v_interval_minutes,
             :v_payoff_mult, :v_giveback_pct, :v_no_new_high_bars,
             :v_quick_mins, :v_quick_giveback, :v_portfolio_filter, :v_market_types,
             :v_slippage_bps, :v_fee_bps, :v_spread_bps
        from MIP.APP.APP_CONFIG
        where CONFIG_KEY like 'EARLY_EXIT_%'
           or CONFIG_KEY in ('SLIPPAGE_BPS', 'FEE_BPS', 'SPREAD_BPS');
    exception when other then
        v_enabled := false;
    end;

    if (not :v_enabled) then
        return object_construct('status', 'SKIPPED', 'reason', 'EARLY_EXIT_ENABLED is false');
    end if;

    -- Load open daily positions
    v_positions := (
        select
            op.PORTFOLIO_ID, op.SYMBOL, op.MARKET_TYPE,
            op.ENTRY_TS, op.ENTRY_PRICE, op.QUANTITY, op.COST_BASIS,
            op.HOLD_UNTIL_INDEX, op.EPISODE_ID, op.RUN_ID
        from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL op
        where op.INTERVAL_MINUTES = 1440
          and op.IS_OPEN = true
          and (
              :v_portfolio_filter = 'ALL'
              or op.PORTFOLIO_ID in (
                  select try_to_number(trim(f.value))
                  from table(split_to_table(:v_portfolio_filter, ',')) f
              )
          )
          and op.MARKET_TYPE in (
              select trim(f.value)
              from table(split_to_table(:v_market_types, ',')) f
          )
        order by op.PORTFOLIO_ID, op.SYMBOL
    );

    for pos in v_positions do
        v_pos_portfolio_id := pos.PORTFOLIO_ID;
        v_pos_symbol       := pos.SYMBOL;
        v_pos_market_type  := pos.MARKET_TYPE;
        v_pos_entry_ts     := pos.ENTRY_TS;
        v_pos_entry_price  := pos.ENTRY_PRICE;
        v_pos_quantity     := pos.QUANTITY;
        v_pos_cost_basis   := pos.COST_BASIS;
        v_pos_hold_until   := pos.HOLD_UNTIL_INDEX;
        v_pos_episode_id   := pos.EPISODE_ID;
        v_pos_run_id       := pos.RUN_ID;

        begin
            -- Skip if already fired in a prior run
            begin
                select FIRST_HIT_TS, FIRST_HIT_RETURN, MFE_RETURN, MFE_TS, EARLY_EXIT_FIRED
                into :v_prior_first_hit_ts, :v_prior_first_hit_ret,
                     :v_prior_mfe_return, :v_prior_mfe_ts, :v_prior_fired
                from MIP.APP.EARLY_EXIT_POSITION_STATE
                where PORTFOLIO_ID = :v_pos_portfolio_id
                  and SYMBOL = :v_pos_symbol
                  and ENTRY_TS = :v_pos_entry_ts;
            exception when other then
                v_prior_first_hit_ts := null;
                v_prior_first_hit_ret := null;
                v_prior_mfe_return := null;
                v_prior_mfe_ts := null;
                v_prior_fired := false;
            end;

            if (coalesce(:v_prior_fired, false)) then
                v_skipped_count := :v_skipped_count + 1;
                continue;
            end if;

            -- Get target return for this position's pattern/horizon
            begin
                select ts.AVG_RETURN
                into :v_target_return
                from MIP.APP.RECOMMENDATION_LOG rl
                join MIP.MART.V_TRUSTED_SIGNALS ts
                  on ts.PATTERN_ID = rl.PATTERN_ID
                 and ts.MARKET_TYPE = rl.MARKET_TYPE
                 and ts.INTERVAL_MINUTES = 1440
                 and ts.IS_TRUSTED = true
                where rl.SYMBOL = :v_pos_symbol
                  and rl.MARKET_TYPE = :v_pos_market_type
                  and rl.INTERVAL_MINUTES = 1440
                  and rl.TS::date = :v_pos_entry_ts::date
                qualify row_number() over (
                    partition by rl.RECOMMENDATION_ID
                    order by ts.AVG_RETURN desc
                ) = 1
                limit 1;
            exception when other then
                v_target_return := null;
            end;

            if (:v_target_return is null or :v_target_return <= 0) then
                v_skipped_count := :v_skipped_count + 1;
                continue;
            end if;

            v_effective_target := :v_target_return * :v_payoff_mult;

            -- ═══ SCAN 15-MIN BARS ═══
            -- Get the latest bar, MFE, and first payoff hit from intraday bars
            begin
                select
                    latest.CLOSE,
                    latest.TS,
                    mfe.MFE_RETURN,
                    mfe.MFE_TS,
                    hit.FIRST_HIT_TS,
                    hit.FIRST_HIT_RETURN,
                    nnh.NO_HIGH_COUNT
                into :v_current_price, :v_bar_close_ts,
                     :v_mfe_return, :v_mfe_ts,
                     :v_first_hit_ts, :v_first_hit_return,
                     :v_no_high_count
                from (
                    select CLOSE, TS
                    from MIP.MART.MARKET_BARS
                    where SYMBOL = :v_pos_symbol
                      and MARKET_TYPE = :v_pos_market_type
                      and INTERVAL_MINUTES = :v_interval_minutes
                      and TS > :v_pos_entry_ts
                    order by TS desc limit 1
                ) latest
                cross join (
                    select
                        max((CLOSE - :v_pos_entry_price) / :v_pos_entry_price) as MFE_RETURN,
                        max_by(TS, (CLOSE - :v_pos_entry_price) / :v_pos_entry_price) as MFE_TS
                    from MIP.MART.MARKET_BARS
                    where SYMBOL = :v_pos_symbol
                      and MARKET_TYPE = :v_pos_market_type
                      and INTERVAL_MINUTES = :v_interval_minutes
                      and TS > :v_pos_entry_ts
                ) mfe
                cross join (
                    select
                        min(case when (CLOSE - :v_pos_entry_price) / :v_pos_entry_price
                                     >= :v_effective_target then TS end) as FIRST_HIT_TS,
                        min(case when (CLOSE - :v_pos_entry_price) / :v_pos_entry_price
                                     >= :v_effective_target
                                 then (CLOSE - :v_pos_entry_price) / :v_pos_entry_price end) as FIRST_HIT_RETURN
                    from MIP.MART.MARKET_BARS
                    where SYMBOL = :v_pos_symbol
                      and MARKET_TYPE = :v_pos_market_type
                      and INTERVAL_MINUTES = :v_interval_minutes
                      and TS > :v_pos_entry_ts
                ) hit
                cross join (
                    select count(*) as NO_HIGH_COUNT
                    from (
                        select TS, CLOSE,
                               max(CLOSE) over (order by TS rows between unbounded preceding and 1 preceding) as PRIOR_MAX
                        from MIP.MART.MARKET_BARS
                        where SYMBOL = :v_pos_symbol
                          and MARKET_TYPE = :v_pos_market_type
                          and INTERVAL_MINUTES = :v_interval_minutes
                          and TS > :v_pos_entry_ts
                        order by TS desc
                        limit :v_no_new_high_bars
                    )
                    where CLOSE < PRIOR_MAX
                ) nnh;
            exception when other then
                v_skipped_count := :v_skipped_count + 1;
                continue;
            end;

            if (:v_current_price is null) then
                v_skipped_count := :v_skipped_count + 1;
                continue;
            end if;

            -- Merge with prior state (carry forward first_hit from earlier runs)
            if (:v_first_hit_ts is null and :v_prior_first_hit_ts is not null) then
                v_first_hit_ts := :v_prior_first_hit_ts;
                v_first_hit_return := :v_prior_first_hit_ret;
            end if;
            if (:v_prior_mfe_return is not null and :v_prior_mfe_return > coalesce(:v_mfe_return, 0)) then
                v_mfe_return := :v_prior_mfe_return;
                v_mfe_ts := :v_prior_mfe_ts;
            end if;

            v_unrealized_return := (:v_current_price - :v_pos_entry_price) / :v_pos_entry_price;
            v_evaluated := :v_evaluated + 1;

            -- ═══ STAGE A: PAYOFF CHECK ═══
            v_payoff_reached := (:v_first_hit_ts is not null);
            v_hit_after_mins := null;
            if (:v_payoff_reached) then
                v_hit_after_mins := datediff('minute', :v_pos_entry_ts, :v_first_hit_ts);
                v_payoff_count := :v_payoff_count + 1;
            end if;

            -- ═══ STAGE B: GIVEBACK CHECK ═══
            v_giveback_triggered := false;
            v_giveback_from_peak := 0;
            v_giveback_ratio := 0;

            if (:v_payoff_reached and :v_mfe_return is not null and :v_mfe_return > 0) then
                v_giveback_from_peak := :v_mfe_return - :v_unrealized_return;
                v_giveback_ratio := :v_giveback_from_peak / :v_mfe_return;

                v_is_quick_payoff := (coalesce(:v_hit_after_mins, 9999) <= :v_quick_mins);
                v_active_giveback_threshold := iff(:v_is_quick_payoff, :v_quick_giveback, :v_giveback_pct);

                -- Trigger if: significant giveback AND no new highs in recent bars
                if (:v_giveback_ratio >= :v_active_giveback_threshold
                    and :v_no_high_count >= :v_no_new_high_bars) then
                    v_giveback_triggered := true;
                end if;
            end if;

            -- ═══ EXIT DECISION ═══
            v_exit_signal := (:v_payoff_reached and :v_giveback_triggered);

            -- Compute exit price with fees
            v_fees := (:v_slippage_bps + :v_fee_bps + :v_spread_bps / 2.0) / 10000.0;
            v_exit_price := :v_current_price * (1 - :v_fees);
            v_early_pnl := (:v_exit_price - :v_pos_entry_price) * :v_pos_quantity;

            -- Hold-to-end return (from RECOMMENDATION_OUTCOMES if available)
            begin
                select ro.REALIZED_RETURN
                into :v_hold_end_return
                from MIP.APP.RECOMMENDATION_LOG rl
                join MIP.MART.V_PORTFOLIO_SIGNALS ps
                  on ps.RECOMMENDATION_ID = rl.RECOMMENDATION_ID
                join MIP.APP.RECOMMENDATION_OUTCOMES ro
                  on ro.RECOMMENDATION_ID = rl.RECOMMENDATION_ID
                 and ro.HORIZON_BARS = ps.HORIZON_BARS
                 and ro.EVAL_STATUS = 'SUCCESS'
                where rl.SYMBOL = :v_pos_symbol
                  and rl.MARKET_TYPE = :v_pos_market_type
                  and rl.INTERVAL_MINUTES = 1440
                  and rl.TS::date = :v_pos_entry_ts::date
                limit 1;
            exception when other then
                v_hold_end_return := null;
            end;

            v_hold_end_pnl := null;
            v_pnl_delta := null;
            if (:v_hold_end_return is not null) then
                v_hold_end_pnl := :v_hold_end_return * :v_pos_cost_basis;
                v_pnl_delta := :v_early_pnl - :v_hold_end_pnl;
            end if;

            -- Build reason codes
            v_reason_codes := object_construct(
                'payoff_reached', :v_payoff_reached,
                'hit_after_mins', :v_hit_after_mins,
                'is_quick_payoff', :v_is_quick_payoff,
                'mfe_return_pct', round(:v_mfe_return * 100, 4),
                'current_return_pct', round(:v_unrealized_return * 100, 4),
                'giveback_ratio', round(:v_giveback_ratio, 4),
                'giveback_threshold_used', :v_active_giveback_threshold,
                'no_new_high_bars', :v_no_high_count,
                'no_new_high_threshold', :v_no_new_high_bars,
                'exit_signal', :v_exit_signal
            );

            if (:v_exit_signal) then
                v_signal_count := :v_signal_count + 1;
            end if;

            -- ═══ WRITE LOG ═══
            insert into MIP.APP.EARLY_EXIT_LOG (
                RUN_ID, PORTFOLIO_ID, SYMBOL, MARKET_TYPE,
                ENTRY_TS, ENTRY_PRICE, QUANTITY, COST_BASIS, HOLD_UNTIL_INDEX,
                BAR_CLOSE_TS, DECISION_TS,
                TARGET_RETURN, PAYOFF_MULTIPLIER, EFFECTIVE_TARGET,
                CURRENT_PRICE, UNREALIZED_RETURN, MFE_RETURN, MFE_TS,
                PAYOFF_REACHED, PAYOFF_FIRST_HIT_TS, PAYOFF_HIT_AFTER_MINS,
                GIVEBACK_FROM_PEAK, GIVEBACK_PCT, NO_NEW_HIGH_BARS, GIVEBACK_TRIGGERED,
                EXIT_SIGNAL, EXIT_PRICE, FEES_APPLIED, EARLY_EXIT_PNL,
                HOLD_TO_END_RETURN, HOLD_TO_END_PNL, PNL_DELTA,
                MODE, EXECUTION_STATUS, REASON_CODES
            ) values (
                :v_run_id, :v_pos_portfolio_id, :v_pos_symbol, :v_pos_market_type,
                :v_pos_entry_ts, :v_pos_entry_price, :v_pos_quantity, :v_pos_cost_basis, :v_pos_hold_until,
                :v_bar_close_ts, :v_decision_ts,
                :v_target_return, :v_payoff_mult, :v_effective_target,
                :v_current_price, :v_unrealized_return, :v_mfe_return, :v_mfe_ts,
                :v_payoff_reached, :v_first_hit_ts, :v_hit_after_mins,
                :v_giveback_from_peak, :v_giveback_ratio, :v_no_high_count, :v_giveback_triggered,
                :v_exit_signal, :v_exit_price, :v_fees, :v_early_pnl,
                :v_hold_end_return, :v_hold_end_pnl, :v_pnl_delta,
                :v_mode,
                iff(:v_exit_signal and :v_mode != 'SHADOW', 'EXECUTED', 'SIGNAL_ONLY'),
                :v_reason_codes
            );

            -- ═══ UPDATE POSITION STATE ═══
            merge into MIP.APP.EARLY_EXIT_POSITION_STATE t
            using (select 1) s
            on t.PORTFOLIO_ID = :v_pos_portfolio_id
               and t.SYMBOL = :v_pos_symbol
               and t.ENTRY_TS = :v_pos_entry_ts
            when matched then update set
                t.FIRST_HIT_TS = coalesce(t.FIRST_HIT_TS, :v_first_hit_ts),
                t.FIRST_HIT_RETURN = coalesce(t.FIRST_HIT_RETURN, :v_first_hit_return),
                t.MFE_RETURN = greatest(coalesce(t.MFE_RETURN, 0), coalesce(:v_mfe_return, 0)),
                t.MFE_TS = iff(coalesce(:v_mfe_return, 0) > coalesce(t.MFE_RETURN, 0), :v_mfe_ts, t.MFE_TS),
                t.LAST_EVALUATED_TS = :v_bar_close_ts,
                t.EARLY_EXIT_FIRED = iff(:v_exit_signal and :v_mode != 'SHADOW', true, t.EARLY_EXIT_FIRED),
                t.EARLY_EXIT_TS = iff(:v_exit_signal and :v_mode != 'SHADOW', :v_bar_close_ts, t.EARLY_EXIT_TS),
                t.UPDATED_AT = current_timestamp()
            when not matched then insert (
                PORTFOLIO_ID, SYMBOL, ENTRY_TS,
                FIRST_HIT_TS, FIRST_HIT_RETURN, MFE_RETURN, MFE_TS,
                LAST_EVALUATED_TS, EARLY_EXIT_FIRED, EARLY_EXIT_TS
            ) values (
                :v_pos_portfolio_id, :v_pos_symbol, :v_pos_entry_ts,
                :v_first_hit_ts, :v_first_hit_return, :v_mfe_return, :v_mfe_ts,
                :v_bar_close_ts, false, null
            );

            -- ═══ PAPER/ACTIVE EXECUTION ═══
            if (:v_exit_signal and :v_mode in ('PAPER', 'ACTIVE')) then
                -- Write SELL trade
                insert into MIP.APP.PORTFOLIO_TRADES (
                    PORTFOLIO_ID, RUN_ID, EPISODE_ID, SYMBOL, MARKET_TYPE,
                    INTERVAL_MINUTES, TRADE_TS, SIDE, PRICE, QUANTITY, NOTIONAL,
                    REALIZED_PNL, SCORE
                ) values (
                    :v_pos_portfolio_id, :v_pos_run_id, :v_pos_episode_id,
                    :v_pos_symbol, :v_pos_market_type, 1440,
                    :v_bar_close_ts, 'SELL', :v_exit_price,
                    :v_pos_quantity, :v_exit_price * :v_pos_quantity,
                    :v_early_pnl, null
                );

                v_executed_count := :v_executed_count + 1;
            end if;

        exception when other then
            v_error_count := :v_error_count + 1;
        end;
    end for;

    return object_construct(
        'status', iff(:v_error_count = 0, 'SUCCESS', 'COMPLETED_WITH_ERRORS'),
        'run_id', :v_run_id,
        'mode', :v_mode,
        'positions_evaluated', :v_evaluated,
        'payoff_reached', :v_payoff_count,
        'exit_signals', :v_signal_count,
        'exits_executed', :v_executed_count,
        'skipped', :v_skipped_count,
        'errors', :v_error_count,
        'config', object_construct(
            'payoff_multiplier', :v_payoff_mult,
            'giveback_pct', :v_giveback_pct,
            'no_new_high_bars', :v_no_new_high_bars,
            'quick_payoff_mins', :v_quick_mins,
            'quick_giveback_pct', :v_quick_giveback
        )
    );
end;
$$;

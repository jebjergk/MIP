-- 170_sp_simulate_portfolio.sql
-- Purpose: Simulate a paper portfolio over daily bars

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_SIMULATE_PORTFOLIO(
    P_PORTFOLIO_ID number,
    P_FROM_DATE date,
    P_TO_DATE date,
    P_HOLD_DAYS number default 5,
    P_MAX_POSITIONS number default null,
    P_MAX_POSITION_PCT number default null,
    P_MIN_ABS_SCORE number default 0.0,
    P_MARKET_TYPE string default 'STOCK',
    P_LIQUIDATE_ON_BUST boolean default true,
    P_DRAWDOWN_RECOVERY_PCT number default null
)
returns variant
language sql
execute as owner
as
$$
declare
    v_run_id string := uuid_string();
    v_market_type string;
    v_interval_minutes number := 1440;
    v_starting_cash number(18,2);
    v_profile_id number;
    v_profile_name string;
    v_profile_max_positions number;
    v_profile_max_position_pct number(18,6);
    v_profile_bust_equity_pct number(18,6);
    v_profile_bust_action string;
    v_profile_drawdown_stop_pct number(18,6);
    v_effective_max_positions number;
    v_effective_max_position_pct number(18,6);
    v_bust_equity_pct number(18,6);
    v_bust_equity_threshold number(18,2);
    v_bust_action string;
    v_bust_liquidate_index number;
    v_drawdown_stop_pct number(18,6);
    v_drawdown_recovery_pct number(18,6);
    v_cash number(18,2);
    v_day_index number := 0;
    v_open_positions number := 0;
    v_trade_count number := 0;
    v_days_simulated number := 0;
    v_total_equity number(18,2);
    v_equity_value number(18,2);
    v_prev_total_equity number(18,2);
    v_peak_equity number(18,2);
    v_drawdown number(18,6);
    v_max_drawdown number(18,6) := 0;
    v_daily_pnl number(18,2);
    v_daily_return number(18,6);
    v_win_days number := 0;
    v_loss_days number := 0;
    v_opportunity_view_exists boolean := false;
    v_opportunity_source string;
    v_max_position_value number(18,2);
    v_sim_status string := 'ACTIVE';
    v_bust_ts timestamp_ntz;
    v_slippage_bps number(18,8);
    v_fee_bps number(18,8);
    v_min_fee number(18,8);
    v_spread_bps number(18,8);
begin
    v_market_type := coalesce(P_MARKET_TYPE, 'STOCK');

    select
        p.STARTING_CASH,
        p.PROFILE_ID,
        prof.NAME,
        prof.MAX_POSITIONS,
        prof.MAX_POSITION_PCT,
        prof.BUST_EQUITY_PCT,
        prof.BUST_ACTION,
        prof.DRAWDOWN_STOP_PCT
      into v_starting_cash
           , v_profile_id
           , v_profile_name
           , v_profile_max_positions
           , v_profile_max_position_pct
           , v_profile_bust_equity_pct
           , v_profile_bust_action
           , v_profile_drawdown_stop_pct
      from MIP.APP.PORTFOLIO p
      left join MIP.APP.PORTFOLIO_PROFILE prof
        on prof.PROFILE_ID = p.PROFILE_ID
     where PORTFOLIO_ID = :P_PORTFOLIO_ID;

    if (v_starting_cash is null) then
        return object_construct(
            'status', 'ERROR',
            'message', 'Portfolio not found',
            'portfolio_id', :P_PORTFOLIO_ID
        );
    end if;

    v_cash := v_starting_cash;
    v_peak_equity := v_starting_cash;
    v_total_equity := v_starting_cash;
    v_effective_max_positions := coalesce(P_MAX_POSITIONS, v_profile_max_positions, 10);
    v_effective_max_position_pct := coalesce(P_MAX_POSITION_PCT, v_profile_max_position_pct, 0.10);
    v_bust_equity_pct := coalesce(v_profile_bust_equity_pct, 0);
    v_bust_equity_threshold := v_starting_cash * v_bust_equity_pct;
    v_bust_action := coalesce(v_profile_bust_action, 'ALLOW_EXITS_ONLY');
    if (not P_LIQUIDATE_ON_BUST) then
        v_bust_action := 'ALLOW_EXITS_ONLY';
    end if;
    if (v_interval_minutes = 1440 and v_bust_action = 'LIQUIDATE_IMMEDIATE') then
        v_bust_action := 'LIQUIDATE_NEXT_BAR';
    end if;
    v_drawdown_stop_pct := v_profile_drawdown_stop_pct;
    v_drawdown_recovery_pct := P_DRAWDOWN_RECOVERY_PCT;
    v_bust_liquidate_index := null;

    select
        coalesce(try_to_number(max(case when CONFIG_KEY = 'SLIPPAGE_BPS' then CONFIG_VALUE end)), 2),
        coalesce(try_to_number(max(case when CONFIG_KEY = 'FEE_BPS' then CONFIG_VALUE end)), 1),
        coalesce(try_to_number(max(case when CONFIG_KEY = 'MIN_FEE' then CONFIG_VALUE end)), 0),
        coalesce(try_to_number(max(case when CONFIG_KEY = 'SPREAD_BPS' then CONFIG_VALUE end)), 0)
      into v_slippage_bps,
           v_fee_bps,
           v_min_fee,
           v_spread_bps
      from MIP.APP.APP_CONFIG;

    select count(*) > 0
      into v_opportunity_view_exists
      from MIP.information_schema.views
     where table_schema = 'APP'
       and table_name = 'V_OPPORTUNITY_FEED';

    v_opportunity_source := case
        when v_opportunity_view_exists then 'MIP.APP.V_OPPORTUNITY_FEED'
        else 'MIP.APP.RECOMMENDATION_LOG'
    end;

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
        CURRENT_TIMESTAMP(),
        :v_run_id,
        'PORTFOLIO_SIM',
        'SIMULATION_START',
        'INFO',
        null,
        object_construct(
            'portfolio_id', :P_PORTFOLIO_ID,
            'from_date', :P_FROM_DATE,
            'to_date', :P_TO_DATE,
            'hold_days', :P_HOLD_DAYS,
            'max_positions', :v_effective_max_positions,
            'max_position_pct', :v_effective_max_position_pct,
            'min_abs_score', :P_MIN_ABS_SCORE,
            'market_type', :v_market_type,
            'interval_minutes', :v_interval_minutes,
            'opportunity_source', :v_opportunity_source,
            'profile_id', :v_profile_id,
            'profile_name', :v_profile_name,
            'bust_equity_pct', :v_bust_equity_pct,
            'bust_action', :v_bust_action,
            'drawdown_stop_pct', :v_drawdown_stop_pct,
            'liquidate_on_bust', :P_LIQUIDATE_ON_BUST,
            'drawdown_recovery_pct', :v_drawdown_recovery_pct
        )
    );

    create temporary table TEMP_POSITIONS (
        SYMBOL string,
        ENTRY_TS timestamp_ntz,
        ENTRY_PRICE number(18,8),
        QUANTITY number(18,8),
        COST_BASIS number(18,8),
        ENTRY_SCORE number(18,10),
        ENTRY_INDEX number,
        HOLD_UNTIL_INDEX number
    );

    for bar in (
        select distinct TS
        from MIP.MART.MARKET_BARS
        where MARKET_TYPE = :v_market_type
          and INTERVAL_MINUTES = :v_interval_minutes
          and TS::date between :P_FROM_DATE and :P_TO_DATE
        order by TS
    ) do
        v_day_index := v_day_index + 1;
        v_days_simulated := v_days_simulated + 1;

        for position_row in (
            select *
            from TEMP_POSITIONS
            where HOLD_UNTIL_INDEX <= :v_day_index
        ) do
            declare
                v_sell_price number(18,8);
                v_sell_exec_price number(18,8);
                v_sell_notional number(18,8);
                v_sell_fee number(18,8);
                v_sell_pnl number(18,8);
            begin
                select CLOSE
                  into v_sell_price
                  from MIP.MART.MARKET_BARS
                 where MARKET_TYPE = :v_market_type
                   and INTERVAL_MINUTES = :v_interval_minutes
                   and SYMBOL = :position_row.SYMBOL
                   and TS = bar.TS;

                if (v_sell_price is not null) then
                    v_sell_exec_price := v_sell_price * (1 - ((v_slippage_bps + (v_spread_bps / 2)) / 10000));
                    v_sell_notional := v_sell_exec_price * position_row.QUANTITY;
                    v_sell_fee := greatest(coalesce(v_min_fee, 0), abs(v_sell_notional) * v_fee_bps / 10000);
                    v_sell_pnl := v_sell_notional - v_sell_fee - position_row.COST_BASIS;
                    v_cash := v_cash + v_sell_notional - v_sell_fee;

                    insert into MIP.APP.PORTFOLIO_TRADES (
                        PORTFOLIO_ID,
                        RUN_ID,
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
                        :P_PORTFOLIO_ID,
                        :v_run_id,
                        :position_row.SYMBOL,
                        :v_market_type,
                        :v_interval_minutes,
                        bar.TS,
                        'SELL',
                        v_sell_exec_price,
                        :position_row.QUANTITY,
                        v_sell_notional,
                        v_sell_pnl,
                        v_cash,
                        :position_row.ENTRY_SCORE
                    );

                    delete from TEMP_POSITIONS
                     where SYMBOL = :position_row.SYMBOL
                       and ENTRY_TS = :position_row.ENTRY_TS;

                    v_trade_count := v_trade_count + 1;
                end if;
            end;
        end for;

        if (v_bust_liquidate_index is not null and v_day_index >= v_bust_liquidate_index) then
            for bust_pos in (
                select *
                from TEMP_POSITIONS
            ) do
                declare
                    v_bust_sell_price number(18,8);
                    v_bust_sell_exec_price number(18,8);
                    v_bust_sell_notional number(18,8);
                    v_bust_sell_fee number(18,8);
                    v_bust_sell_pnl number(18,8);
                begin
                    select CLOSE
                      into v_bust_sell_price
                      from MIP.MART.MARKET_BARS
                     where MARKET_TYPE = :v_market_type
                       and INTERVAL_MINUTES = :v_interval_minutes
                       and SYMBOL = :bust_pos.SYMBOL
                       and TS = bar.TS;

                    if (v_bust_sell_price is not null) then
                        v_bust_sell_exec_price := v_bust_sell_price * (1 - ((v_slippage_bps + (v_spread_bps / 2)) / 10000));
                        v_bust_sell_notional := v_bust_sell_exec_price * bust_pos.QUANTITY;
                        v_bust_sell_fee := greatest(coalesce(v_min_fee, 0), abs(v_bust_sell_notional) * v_fee_bps / 10000);
                        v_bust_sell_pnl := v_bust_sell_notional - v_bust_sell_fee - bust_pos.COST_BASIS;
                        v_cash := v_cash + v_bust_sell_notional - v_bust_sell_fee;

                        insert into MIP.APP.PORTFOLIO_TRADES (
                            PORTFOLIO_ID,
                            RUN_ID,
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
                            :P_PORTFOLIO_ID,
                            :v_run_id,
                            :bust_pos.SYMBOL,
                            :v_market_type,
                            :v_interval_minutes,
                            bar.TS,
                            'SELL',
                            v_bust_sell_exec_price,
                            :bust_pos.QUANTITY,
                            v_bust_sell_notional,
                            v_bust_sell_pnl,
                            v_cash,
                            :bust_pos.ENTRY_SCORE
                        );

                        delete from TEMP_POSITIONS
                         where SYMBOL = :bust_pos.SYMBOL
                           and ENTRY_TS = :bust_pos.ENTRY_TS;

                        v_trade_count := v_trade_count + 1;
                    end if;
                end;
            end for;

            v_bust_liquidate_index := null;
        end if;

        select coalesce(sum(tp.QUANTITY * mb.CLOSE), 0)
          into v_equity_value
          from TEMP_POSITIONS tp
          join MIP.MART.MARKET_BARS mb
            on mb.SYMBOL = tp.SYMBOL
           and mb.MARKET_TYPE = :v_market_type
           and mb.INTERVAL_MINUTES = :v_interval_minutes
           and mb.TS = bar.TS;

        v_total_equity := v_cash + v_equity_value;

        select count(*)
          into v_open_positions
          from TEMP_POSITIONS;

        v_peak_equity := greatest(v_peak_equity, v_total_equity);
        v_drawdown := case
            when v_peak_equity = 0 then null
            else (v_total_equity - v_peak_equity) / v_peak_equity
        end;
        v_max_drawdown := least(v_max_drawdown, coalesce(v_drawdown, 0));

        if (
            v_sim_status <> 'BUST'
            and v_bust_equity_pct is not null
            and v_bust_equity_pct > 0
            and v_total_equity <= v_bust_equity_threshold
        ) then
            v_sim_status := 'BUST';
            v_bust_ts := bar.TS;

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
                CURRENT_TIMESTAMP(),
                :v_run_id,
                'PORTFOLIO_SIM',
                'SIMULATION_BUST',
                'WARN',
                null,
                object_construct(
                    'portfolio_id', :P_PORTFOLIO_ID,
                    'bust_equity_pct', :v_bust_equity_pct,
                    'bust_threshold', :v_bust_equity_threshold,
                    'total_equity', :v_total_equity,
                    'bust_action', :v_bust_action,
                    'ts', bar.TS
                )
            );

            if (v_bust_action = 'LIQUIDATE_IMMEDIATE') then
                for bust_pos in (
                    select *
                    from TEMP_POSITIONS
                ) do
                    declare
                        v_bust_sell_price number(18,8);
                        v_bust_sell_exec_price number(18,8);
                        v_bust_sell_notional number(18,8);
                        v_bust_sell_fee number(18,8);
                        v_bust_sell_pnl number(18,8);
                    begin
                        select CLOSE
                          into v_bust_sell_price
                          from MIP.MART.MARKET_BARS
                         where MARKET_TYPE = :v_market_type
                           and INTERVAL_MINUTES = :v_interval_minutes
                           and SYMBOL = :bust_pos.SYMBOL
                           and TS = bar.TS;

                        if (v_bust_sell_price is not null) then
                            v_bust_sell_exec_price := v_bust_sell_price * (1 - ((v_slippage_bps + (v_spread_bps / 2)) / 10000));
                            v_bust_sell_notional := v_bust_sell_exec_price * bust_pos.QUANTITY;
                            v_bust_sell_fee := greatest(coalesce(v_min_fee, 0), abs(v_bust_sell_notional) * v_fee_bps / 10000);
                            v_bust_sell_pnl := v_bust_sell_notional - v_bust_sell_fee - bust_pos.COST_BASIS;
                            v_cash := v_cash + v_bust_sell_notional - v_bust_sell_fee;

                            insert into MIP.APP.PORTFOLIO_TRADES (
                                PORTFOLIO_ID,
                                RUN_ID,
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
                                :P_PORTFOLIO_ID,
                                :v_run_id,
                                :bust_pos.SYMBOL,
                                :v_market_type,
                                :v_interval_minutes,
                                bar.TS,
                                'SELL',
                                v_bust_sell_exec_price,
                                :bust_pos.QUANTITY,
                                v_bust_sell_notional,
                                v_bust_sell_pnl,
                                v_cash,
                                :bust_pos.ENTRY_SCORE
                            );

                            delete from TEMP_POSITIONS
                             where SYMBOL = :bust_pos.SYMBOL
                               and ENTRY_TS = :bust_pos.ENTRY_TS;

                            v_trade_count := v_trade_count + 1;
                        end if;
                    end;
                end for;

                select coalesce(sum(tp.QUANTITY * mb.CLOSE), 0)
                  into v_equity_value
                  from TEMP_POSITIONS tp
                  join MIP.MART.MARKET_BARS mb
                    on mb.SYMBOL = tp.SYMBOL
                   and mb.MARKET_TYPE = :v_market_type
                   and mb.INTERVAL_MINUTES = :v_interval_minutes
                   and mb.TS = bar.TS;

                v_total_equity := v_cash + v_equity_value;
            elseif (v_bust_action = 'LIQUIDATE_NEXT_BAR') then
                v_bust_liquidate_index := v_day_index + 1;
            end if;
        end if;

        if (
            v_sim_status <> 'BUST'
            and v_drawdown_stop_pct is not null
            and v_drawdown_stop_pct > 0
        ) then
            if (v_sim_status = 'ACTIVE' and v_max_drawdown <= -abs(v_drawdown_stop_pct)) then
                v_sim_status := 'PAUSED';

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
                    CURRENT_TIMESTAMP(),
                    :v_run_id,
                    'PORTFOLIO_SIM',
                    'SIMULATION_PAUSED',
                    'INFO',
                    null,
                    object_construct(
                        'portfolio_id', :P_PORTFOLIO_ID,
                        'drawdown_stop_pct', :v_drawdown_stop_pct,
                        'max_drawdown', :v_max_drawdown,
                        'ts', bar.TS
                    )
                );
            elseif (
                v_sim_status = 'PAUSED'
                and v_drawdown_recovery_pct is not null
                and v_drawdown_recovery_pct > 0
                and v_drawdown >= -abs(v_drawdown_recovery_pct)
            ) then
                v_sim_status := 'ACTIVE';

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
                    CURRENT_TIMESTAMP(),
                    :v_run_id,
                    'PORTFOLIO_SIM',
                    'SIMULATION_RESUMED',
                    'INFO',
                    null,
                    object_construct(
                        'portfolio_id', :P_PORTFOLIO_ID,
                        'drawdown_recovery_pct', :v_drawdown_recovery_pct,
                        'drawdown', :v_drawdown,
                        'ts', bar.TS
                    )
                );
            end if;
        end if;

        select count(*)
          into v_open_positions
          from TEMP_POSITIONS;

        v_max_position_value := v_total_equity * v_effective_max_position_pct;

        if (v_sim_status = 'ACTIVE' and v_open_positions < v_effective_max_positions) then
            for rec in (
                select
                    SYMBOL,
                    MARKET_TYPE,
                    INTERVAL_MINUTES,
                    TS,
                    SCORE
                from identifier(:v_opportunity_source)
                where MARKET_TYPE = :v_market_type
                  and INTERVAL_MINUTES = :v_interval_minutes
                  and TS::date = bar.TS::date
                  and abs(coalesce(SCORE, 0)) >= coalesce(P_MIN_ABS_SCORE, 0.0)
                qualify row_number() over (
                    partition by SYMBOL
                    order by TS desc, abs(coalesce(SCORE, 0)) desc
                ) = 1
                order by TS desc, abs(coalesce(SCORE, 0)) desc
            ) do
                declare
                    v_buy_price number(18,8);
                    v_buy_exec_price number(18,8);
                    v_target_value number(18,8);
                    v_buy_qty number(18,8);
                    v_buy_notional number(18,8);
                    v_buy_fee number(18,8);
                    v_total_cost number(18,8);
                begin
                    if (v_open_positions < v_effective_max_positions) then
                        if (not exists (
                            select 1 from TEMP_POSITIONS where SYMBOL = rec.SYMBOL
                        )) then
                            select CLOSE
                              into v_buy_price
                              from MIP.MART.MARKET_BARS
                             where MARKET_TYPE = :v_market_type
                               and INTERVAL_MINUTES = :v_interval_minutes
                               and SYMBOL = rec.SYMBOL
                               and TS = bar.TS;

                            if (v_buy_price is not null) then
                                v_target_value := least(v_max_position_value, v_cash);
                                v_buy_qty := floor(v_target_value / nullif(v_buy_price, 0));
                                v_buy_exec_price := v_buy_price * (1 + ((v_slippage_bps + (v_spread_bps / 2)) / 10000));
                                v_buy_notional := v_buy_qty * v_buy_exec_price;
                                v_buy_fee := greatest(coalesce(v_min_fee, 0), abs(v_buy_notional) * v_fee_bps / 10000);
                                v_total_cost := v_buy_notional + v_buy_fee;

                                if (v_buy_qty > 0 and v_total_cost <= v_cash) then
                                    insert into TEMP_POSITIONS (
                                        SYMBOL,
                                        ENTRY_TS,
                                        ENTRY_PRICE,
                                        QUANTITY,
                                        COST_BASIS,
                                        ENTRY_SCORE,
                                        ENTRY_INDEX,
                                        HOLD_UNTIL_INDEX
                                    )
                                    values (
                                        rec.SYMBOL,
                                        bar.TS,
                                        v_buy_exec_price,
                                        v_buy_qty,
                                        v_total_cost,
                                        rec.SCORE,
                                        v_day_index,
                                        v_day_index + coalesce(P_HOLD_DAYS, 5)
                                    );

                                    v_cash := v_cash - v_total_cost;

                                    insert into MIP.APP.PORTFOLIO_TRADES (
                                        PORTFOLIO_ID,
                                        RUN_ID,
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
                                        :P_PORTFOLIO_ID,
                                        :v_run_id,
                                        rec.SYMBOL,
                                        :v_market_type,
                                        :v_interval_minutes,
                                        bar.TS,
                                        'BUY',
                                        v_buy_exec_price,
                                        v_buy_qty,
                                        v_buy_notional,
                                        null,
                                        v_cash,
                                        rec.SCORE
                                    );

                                    v_trade_count := v_trade_count + 1;
                                    v_open_positions := v_open_positions + 1;
                                end if;
                            end if;
                        end if;
                    end if;
                end;
            end for;
        end if;

        select coalesce(sum(tp.QUANTITY * mb.CLOSE), 0)
          into v_equity_value
          from TEMP_POSITIONS tp
          join MIP.MART.MARKET_BARS mb
            on mb.SYMBOL = tp.SYMBOL
           and mb.MARKET_TYPE = :v_market_type
           and mb.INTERVAL_MINUTES = :v_interval_minutes
           and mb.TS = bar.TS;

        v_total_equity := v_cash + v_equity_value;

        if (v_prev_total_equity is null) then
            v_daily_pnl := 0;
            v_daily_return := null;
        else
            v_daily_pnl := v_total_equity - v_prev_total_equity;
            v_daily_return := case
                when v_prev_total_equity = 0 then null
                else v_daily_pnl / v_prev_total_equity
            end;
        end if;

        v_peak_equity := greatest(v_peak_equity, v_total_equity);
        v_drawdown := case
            when v_peak_equity = 0 then null
            else (v_total_equity - v_peak_equity) / v_peak_equity
        end;
        v_max_drawdown := least(v_max_drawdown, coalesce(v_drawdown, 0));

        if (v_daily_return is not null and v_daily_return > 0) then
            v_win_days := v_win_days + 1;
        elseif (v_daily_return is not null and v_daily_return < 0) then
            v_loss_days := v_loss_days + 1;
        end if;

        insert into MIP.APP.PORTFOLIO_DAILY (
            PORTFOLIO_ID,
            RUN_ID,
            TS,
            CASH,
            EQUITY_VALUE,
            TOTAL_EQUITY,
            OPEN_POSITIONS,
            DAILY_PNL,
            DAILY_RETURN,
            PEAK_EQUITY,
            DRAWDOWN,
            STATUS
        )
        values (
            :P_PORTFOLIO_ID,
            :v_run_id,
            bar.TS,
            v_cash,
            v_equity_value,
            v_total_equity,
            v_open_positions,
            v_daily_pnl,
            v_daily_return,
            v_peak_equity,
            v_drawdown,
            v_sim_status
        );

        v_prev_total_equity := v_total_equity;
    end for;

    insert into MIP.APP.PORTFOLIO_POSITIONS (
        PORTFOLIO_ID,
        RUN_ID,
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
        :P_PORTFOLIO_ID,
        :v_run_id,
        SYMBOL,
        :v_market_type,
        :v_interval_minutes,
        ENTRY_TS,
        ENTRY_PRICE,
        QUANTITY,
        COST_BASIS,
        ENTRY_SCORE,
        ENTRY_INDEX,
        HOLD_UNTIL_INDEX
    from TEMP_POSITIONS;

    update MIP.APP.PORTFOLIO
       set LAST_SIMULATION_RUN_ID = :v_run_id,
           LAST_SIMULATED_AT = CURRENT_TIMESTAMP(),
           FINAL_EQUITY = :v_total_equity,
           TOTAL_RETURN = case
               when v_starting_cash = 0 then null
               else (v_total_equity - v_starting_cash) / v_starting_cash
           end,
           MAX_DRAWDOWN = :v_max_drawdown,
           WIN_DAYS = :v_win_days,
           LOSS_DAYS = :v_loss_days,
           STATUS = :v_sim_status,
           BUST_AT = case
               when v_sim_status = 'BUST' then :v_bust_ts
               else null
           end,
           UPDATED_AT = CURRENT_TIMESTAMP()
     where PORTFOLIO_ID = :P_PORTFOLIO_ID;

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
        CURRENT_TIMESTAMP(),
        :v_run_id,
        'PORTFOLIO_SIM',
        'SIMULATION_END',
        'INFO',
        null,
        object_construct(
            'portfolio_id', :P_PORTFOLIO_ID,
            'days_simulated', :v_days_simulated,
            'trade_count', :v_trade_count,
            'final_equity', :v_total_equity,
            'total_return', case
                when v_starting_cash = 0 then null
                else (v_total_equity - v_starting_cash) / v_starting_cash
            end,
            'max_drawdown', :v_max_drawdown,
            'status', :v_sim_status,
            'bust_at', :v_bust_ts,
            'win_days', :v_win_days,
            'loss_days', :v_loss_days
        )
    );

    return object_construct(
        'status', 'OK',
        'run_id', :v_run_id,
        'portfolio_id', :P_PORTFOLIO_ID,
        'days_simulated', :v_days_simulated,
        'trade_count', :v_trade_count,
        'final_equity', :v_total_equity,
        'total_return', case
            when v_starting_cash = 0 then null
            else (v_total_equity - v_starting_cash) / v_starting_cash
        end,
        'max_drawdown', :v_max_drawdown,
        'portfolio_status', :v_sim_status,
        'bust_at', :v_bust_ts,
        'win_days', :v_win_days,
        'loss_days', :v_loss_days
    );
end;
$$;

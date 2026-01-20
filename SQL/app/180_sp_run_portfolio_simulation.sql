-- 180_sp_run_portfolio_simulation.sql
-- Purpose: Deterministic v1 portfolio simulation for paper portfolios

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_PORTFOLIO_SIMULATION(
    P_PORTFOLIO_ID number,
    P_FROM_TS timestamp_ntz,
    P_TO_TS timestamp_ntz
)
returns variant
language sql
execute as owner
as
$$
declare
    v_run_id string := uuid_string();
    v_starting_cash number(18,2);
    v_profile_id number;
    v_max_positions number;
    v_max_position_pct number(18,6);
    v_bust_equity_pct number(18,6);
    v_drawdown_stop_pct number(18,6);
    v_cash number(18,2);
    v_total_equity number(18,2);
    v_equity_value number(18,2);
    v_prev_total_equity number(18,2);
    v_daily_pnl number(18,2);
    v_daily_return number(18,6);
    v_peak_equity number(18,2);
    v_drawdown number(18,6);
    v_open_positions number := 0;
    v_entries_blocked boolean := false;
    v_block_reason string;
    v_trade_count number := 0;
    v_position_count number := 0;
    v_daily_count number := 0;
    v_max_position_value number(18,2);
begin
    select
        p.STARTING_CASH,
        p.PROFILE_ID,
        prof.MAX_POSITIONS,
        prof.MAX_POSITION_PCT,
        prof.BUST_EQUITY_PCT,
        prof.DRAWDOWN_STOP_PCT
      into v_starting_cash,
           v_profile_id,
           v_max_positions,
           v_max_position_pct,
           v_bust_equity_pct,
           v_drawdown_stop_pct
      from MIP.APP.PORTFOLIO p
      left join MIP.APP.PORTFOLIO_PROFILE prof
        on prof.PROFILE_ID = p.PROFILE_ID
     where p.PORTFOLIO_ID = :P_PORTFOLIO_ID;

    call MIP.APP.SP_LOG_EVENT(
        'PORTFOLIO_SIM',
        'START',
        'INFO',
        null,
        object_construct(
            'portfolio_id', :P_PORTFOLIO_ID,
            'from_ts', :P_FROM_TS,
            'to_ts', :P_TO_TS,
            'profile_id', :v_profile_id,
            'max_positions', :v_max_positions,
            'max_position_pct', :v_max_position_pct,
            'bust_equity_pct', :v_bust_equity_pct,
            'drawdown_stop_pct', :v_drawdown_stop_pct
        ),
        null,
        :v_run_id,
        null
    );

    if (v_starting_cash is null) then
        call MIP.APP.SP_LOG_EVENT(
            'PORTFOLIO_SIM',
            'FAIL',
            'ERROR',
            null,
            object_construct(
                'portfolio_id', :P_PORTFOLIO_ID,
                'reason', 'PORTFOLIO_NOT_FOUND'
            ),
            'Portfolio not found',
            :v_run_id,
            null
        );

        return object_construct(
            'status', 'ERROR',
            'message', 'Portfolio not found',
            'portfolio_id', :P_PORTFOLIO_ID,
            'run_id', :v_run_id
        );
    end if;

    v_max_positions := coalesce(v_max_positions, 10);
    v_max_position_pct := coalesce(v_max_position_pct, 0.10);
    v_cash := v_starting_cash;
    v_total_equity := v_starting_cash;
    v_peak_equity := v_starting_cash;

    create temporary table TEMP_POSITIONS (
        SYMBOL string,
        MARKET_TYPE string,
        ENTRY_TS timestamp_ntz,
        ENTRY_PRICE number(18,8),
        QUANTITY number(18,8),
        COST_BASIS number(18,8),
        ENTRY_SCORE number(18,10),
        ENTRY_INDEX number,
        HOLD_UNTIL_INDEX number
    );

    create temporary table TEMP_SIGNALS as
    select
        s.RECOMMENDATION_ID,
        s.TS as ENTRY_TS,
        s.SYMBOL,
        s.MARKET_TYPE,
        s.INTERVAL_MINUTES,
        s.PATTERN_ID,
        s.SCORE,
        s.HORIZON_BARS,
        entry_bar.BAR_INDEX as ENTRY_INDEX,
        entry_bar.CLOSE as ENTRY_PRICE,
        exit_bar.BAR_INDEX as HOLD_UNTIL_INDEX,
        exit_bar.TS as EXIT_TS,
        exit_bar.CLOSE as EXIT_PRICE
    from MIP.MART.V_PORTFOLIO_SIGNALS s
    join MIP.MART.V_BAR_INDEX entry_bar
      on entry_bar.SYMBOL = s.SYMBOL
     and entry_bar.MARKET_TYPE = s.MARKET_TYPE
     and entry_bar.INTERVAL_MINUTES = s.INTERVAL_MINUTES
     and entry_bar.TS = s.TS
    join MIP.MART.V_BAR_INDEX exit_bar
      on exit_bar.SYMBOL = s.SYMBOL
     and exit_bar.MARKET_TYPE = s.MARKET_TYPE
     and exit_bar.INTERVAL_MINUTES = s.INTERVAL_MINUTES
     and exit_bar.BAR_INDEX = entry_bar.BAR_INDEX + s.HORIZON_BARS
    where s.INTERVAL_MINUTES = 1440
      and s.TS between :P_FROM_TS and :P_TO_TS
      and exit_bar.TS <= :P_TO_TS;

    for bar in (
        select TS, BAR_INDEX
        from MIP.MART.V_BAR_INDEX
        where INTERVAL_MINUTES = 1440
          and TS between :P_FROM_TS and :P_TO_TS
        order by TS
    ) do
        for position_row in (
            select *
            from TEMP_POSITIONS
            where HOLD_UNTIL_INDEX <= :bar.BAR_INDEX
        ) do
            declare
                v_sell_price number(18,8);
                v_sell_notional number(18,8);
                v_sell_pnl number(18,8);
            begin
                select CLOSE
                  into v_sell_price
                  from MIP.MART.V_BAR_INDEX
                 where SYMBOL = :position_row.SYMBOL
                   and MARKET_TYPE = :position_row.MARKET_TYPE
                   and INTERVAL_MINUTES = 1440
                   and TS = :bar.TS;

                if (v_sell_price is not null) then
                    v_sell_notional := v_sell_price * position_row.QUANTITY;
                    v_sell_pnl := (v_sell_price - position_row.ENTRY_PRICE) * position_row.QUANTITY;
                    v_cash := v_cash + v_sell_notional;

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
                        :position_row.MARKET_TYPE,
                        1440,
                        :bar.TS,
                        'SELL',
                        v_sell_price,
                        :position_row.QUANTITY,
                        v_sell_notional,
                        v_sell_pnl,
                        v_cash,
                        :position_row.ENTRY_SCORE
                    );

                    delete from TEMP_POSITIONS
                     where SYMBOL = :position_row.SYMBOL
                       and MARKET_TYPE = :position_row.MARKET_TYPE
                       and ENTRY_TS = :position_row.ENTRY_TS;

                    v_trade_count := v_trade_count + 1;
                end if;
            end;
        end for;

        select coalesce(sum(tp.QUANTITY * vb.CLOSE), 0)
          into v_equity_value
          from TEMP_POSITIONS tp
          join MIP.MART.V_BAR_INDEX vb
            on vb.SYMBOL = tp.SYMBOL
           and vb.MARKET_TYPE = tp.MARKET_TYPE
           and vb.INTERVAL_MINUTES = 1440
           and vb.TS = :bar.TS;

        v_total_equity := v_cash + v_equity_value;

        select count(*)
          into v_open_positions
          from TEMP_POSITIONS;

        v_peak_equity := greatest(v_peak_equity, v_total_equity);
        v_drawdown := case
            when v_peak_equity = 0 then null
            else (v_peak_equity - v_total_equity) / v_peak_equity
        end;

        if (
            not v_entries_blocked
            and v_bust_equity_pct is not null
            and v_bust_equity_pct > 0
            and v_total_equity <= v_starting_cash * v_bust_equity_pct
        ) then
            v_entries_blocked := true;
            v_block_reason := 'BUST_EQUITY';
        elseif (
            not v_entries_blocked
            and v_drawdown_stop_pct is not null
            and v_drawdown_stop_pct > 0
            and v_drawdown >= v_drawdown_stop_pct
        ) then
            v_entries_blocked := true;
            v_block_reason := 'DRAWDOWN_STOP';
        end if;

        v_max_position_value := v_total_equity * v_max_position_pct;

        if (not v_entries_blocked and v_open_positions < v_max_positions) then
            for rec in (
                select *
                from TEMP_SIGNALS
                where ENTRY_TS = :bar.TS
                order by SCORE desc
            ) do
                declare
                    v_buy_price number(18,8);
                    v_target_value number(18,8);
                    v_buy_qty number(18,8);
                    v_buy_cost number(18,8);
                begin
                    if (v_open_positions < v_max_positions) then
                        if (not exists (
                            select 1
                            from TEMP_POSITIONS
                            where SYMBOL = rec.SYMBOL
                              and MARKET_TYPE = rec.MARKET_TYPE
                        )) then
                            v_buy_price := rec.ENTRY_PRICE;
                            v_target_value := least(v_max_position_value, v_cash);
                            v_buy_qty := v_target_value / nullif(v_buy_price, 0);
                            v_buy_cost := v_buy_qty * v_buy_price;

                            if (v_buy_qty > 0 and v_buy_cost <= v_cash) then
                                insert into TEMP_POSITIONS (
                                    SYMBOL,
                                    MARKET_TYPE,
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
                                    rec.MARKET_TYPE,
                                    rec.ENTRY_TS,
                                    v_buy_price,
                                    v_buy_qty,
                                    v_buy_cost,
                                    rec.SCORE,
                                    rec.ENTRY_INDEX,
                                    rec.HOLD_UNTIL_INDEX
                                );

                                v_cash := v_cash - v_buy_cost;

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
                                    rec.MARKET_TYPE,
                                    1440,
                                    rec.ENTRY_TS,
                                    'BUY',
                                    v_buy_price,
                                    v_buy_qty,
                                    v_buy_cost,
                                    null,
                                    v_cash,
                                    rec.SCORE
                                );

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
                                values (
                                    :P_PORTFOLIO_ID,
                                    :v_run_id,
                                    rec.SYMBOL,
                                    rec.MARKET_TYPE,
                                    1440,
                                    rec.ENTRY_TS,
                                    v_buy_price,
                                    v_buy_qty,
                                    v_buy_cost,
                                    rec.SCORE,
                                    rec.ENTRY_INDEX,
                                    rec.HOLD_UNTIL_INDEX
                                );

                                v_trade_count := v_trade_count + 1;
                                v_position_count := v_position_count + 1;
                                v_open_positions := v_open_positions + 1;
                            end if;
                        end if;
                    end if;
                end;
            end for;
        end if;

        select coalesce(sum(tp.QUANTITY * vb.CLOSE), 0)
          into v_equity_value
          from TEMP_POSITIONS tp
          join MIP.MART.V_BAR_INDEX vb
            on vb.SYMBOL = tp.SYMBOL
           and vb.MARKET_TYPE = tp.MARKET_TYPE
           and vb.INTERVAL_MINUTES = 1440
           and vb.TS = :bar.TS;

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
            else (v_peak_equity - v_total_equity) / v_peak_equity
        end;

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
            :bar.TS,
            v_cash,
            v_equity_value,
            v_total_equity,
            v_open_positions,
            v_daily_pnl,
            v_daily_return,
            v_peak_equity,
            v_drawdown,
            'ACTIVE'
        );

        v_daily_count := v_daily_count + 1;
        v_prev_total_equity := v_total_equity;
    end for;

    call MIP.APP.SP_LOG_EVENT(
        'PORTFOLIO_SIM',
        'SUCCESS',
        'INFO',
        null,
        object_construct(
            'portfolio_id', :P_PORTFOLIO_ID,
            'run_id', :v_run_id,
            'trades', :v_trade_count,
            'positions', :v_position_count,
            'daily_rows', :v_daily_count,
            'final_equity', :v_total_equity,
            'entries_blocked', :v_entries_blocked,
            'block_reason', :v_block_reason
        ),
        null,
        :v_run_id,
        null
    );

    return object_construct(
        'status', 'OK',
        'run_id', :v_run_id,
        'portfolio_id', :P_PORTFOLIO_ID,
        'trades', :v_trade_count,
        'positions', :v_position_count,
        'daily_rows', :v_daily_count,
        'final_equity', :v_total_equity,
        'entries_blocked', :v_entries_blocked,
        'block_reason', :v_block_reason
    );
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'PORTFOLIO_SIM',
            'FAIL',
            'ERROR',
            null,
            object_construct(
                'portfolio_id', :P_PORTFOLIO_ID,
                'run_id', :v_run_id
            ),
            :sqlerrm,
            :v_run_id,
            null
        );

        return object_construct(
            'status', 'ERROR',
            'run_id', :v_run_id,
            'portfolio_id', :P_PORTFOLIO_ID,
            'error', :sqlerrm
        );
end;
$$;

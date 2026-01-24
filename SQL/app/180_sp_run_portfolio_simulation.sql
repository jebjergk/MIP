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
execute as caller
as
$$
declare
    v_run_id string := uuid_string();
    v_starting_cash number(18,2);
    v_profile_id number;
    v_max_positions number;
    v_max_position_pct number(18,6);
    v_bust_equity_pct number(18,6);
    v_bust_action string;
    v_drawdown_stop_pct number(18,6);
    v_cash number(18,2);
    v_total_equity number(18,2);
    v_equity_value number(18,2);
    v_peak_equity number(18,2);
    v_drawdown number(18,6);
    v_open_positions number := 0;
    v_entries_blocked boolean := false;
    v_block_reason string;
    v_trade_count number := 0;
    v_trade_candidates number := 0;
    v_trade_inserted number := 0;
    v_trade_dedup_skipped number := 0;
    v_position_count number := 0;
    v_daily_count number := 0;
    v_position_days_expanded number := 0;
    v_max_position_value number(18,2);
    v_bar_ts timestamp_ntz;
    v_bar_index number;
    v_bar_sql string;
    v_bar_rs resultset;
    v_position_sql string;
    v_position_rs resultset;
    v_signal_sql string;
    v_signal_rs resultset;
    v_trade_day timestamp_ntz;
    v_trade_rows_affected number := 0;
    v_final_equity number(18,2);
    v_total_return number(18,6);
    v_max_drawdown number(18,6);
    v_win_days number;
    v_loss_days number;
    v_bust_at timestamp_ntz;
    v_last_simulated_at timestamp_ntz;
begin
    select
        p.STARTING_CASH,
        p.PROFILE_ID,
        prof.MAX_POSITIONS,
        prof.MAX_POSITION_PCT,
        prof.BUST_EQUITY_PCT,
        prof.BUST_ACTION,
        prof.DRAWDOWN_STOP_PCT
      into v_starting_cash,
           v_profile_id,
           v_max_positions,
           v_max_position_pct,
           v_bust_equity_pct,
           v_bust_action,
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
            'bust_action', :v_bust_action,
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

    v_max_positions := coalesce(v_max_positions, 5);
    v_max_position_pct := coalesce(v_max_position_pct, 0.05);
    v_bust_equity_pct := coalesce(v_bust_equity_pct, 0.60);
    v_bust_action := coalesce(v_bust_action, 'ALLOW_EXITS_ONLY');
    v_drawdown_stop_pct := coalesce(v_drawdown_stop_pct, 0.10);
    v_cash := v_starting_cash;
    v_total_equity := v_starting_cash;
    v_peak_equity := v_starting_cash;

    create or replace temporary table TEMP_POSITIONS (
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

    create or replace temporary table TEMP_SIGNALS (
        RECOMMENDATION_ID number,
        ENTRY_TS timestamp_ntz,
        SYMBOL string,
        MARKET_TYPE string,
        INTERVAL_MINUTES number,
        PATTERN_ID number,
        SCORE number(18,10),
        HORIZON_BARS number,
        ENTRY_INDEX number,
        ENTRY_PRICE number(18,8),
        HOLD_UNTIL_INDEX number,
        EXIT_TS timestamp_ntz,
        EXIT_PRICE number(18,8)
    );

    create or replace temporary table TEMP_DAILY_CASH (
        TS timestamp_ntz,
        CASH number(18,2)
    );

    insert into TEMP_SIGNALS
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

    v_bar_sql := '
        select TS, BAR_INDEX
        from MIP.MART.V_BAR_INDEX
        where INTERVAL_MINUTES = 1440
          and TS between ? and ?
        qualify row_number() over (partition by TS order by BAR_INDEX) = 1
        order by TS
    ';
    v_bar_rs := (execute immediate :v_bar_sql using (P_FROM_TS, P_TO_TS));

    for bar_row in v_bar_rs do
        v_bar_ts := bar_row.TS;
        v_bar_index := bar_row.BAR_INDEX;
        v_position_sql := '
            select *
            from TEMP_POSITIONS
            where HOLD_UNTIL_INDEX <= ?
        ';
        v_position_rs := (execute immediate :v_position_sql using (v_bar_index));
        for position_row in v_position_rs do
            declare
                v_sell_price number(18,8);
                v_sell_notional number(18,8);
                v_sell_pnl number(18,8);
                v_position_symbol string;
                v_position_market_type string;
                v_position_entry_ts timestamp_ntz;
                v_position_entry_price number(18,8);
                v_position_qty number(18,8);
                v_position_entry_score number(18,10);
            begin
                v_position_symbol := position_row.SYMBOL;
                v_position_market_type := position_row.MARKET_TYPE;
                v_position_entry_ts := position_row.ENTRY_TS;
                v_position_entry_price := position_row.ENTRY_PRICE;
                v_position_qty := position_row.QUANTITY;
                v_position_entry_score := position_row.ENTRY_SCORE;

                select CLOSE
                  into v_sell_price
                  from MIP.MART.V_BAR_INDEX
                 where SYMBOL = :v_position_symbol
                   and MARKET_TYPE = :v_position_market_type
                   and INTERVAL_MINUTES = 1440
                   and TS = :v_bar_ts;

                if (v_sell_price is not null) then
                    v_sell_notional := v_sell_price * v_position_qty;
                    v_sell_pnl := (v_sell_price - v_position_entry_price) * v_position_qty;
                    v_cash := v_cash + v_sell_notional;
                    v_trade_candidates := v_trade_candidates + 1;
                    v_trade_day := date_trunc('day', v_bar_ts);

                    merge into MIP.APP.PORTFOLIO_TRADES as target
                    using (
                        select
                            :P_PORTFOLIO_ID as PORTFOLIO_ID,
                            :v_run_id as RUN_ID,
                            :v_position_symbol as SYMBOL,
                            :v_position_market_type as MARKET_TYPE,
                            1440 as INTERVAL_MINUTES,
                            :v_bar_ts as TRADE_TS,
                            'SELL' as SIDE,
                            :v_sell_price as PRICE,
                            :v_position_qty as QUANTITY,
                            :v_sell_notional as NOTIONAL,
                            :v_sell_pnl as REALIZED_PNL,
                            :v_cash as CASH_AFTER,
                            :v_position_entry_score as SCORE,
                            :v_trade_day as TRADE_DAY
                    ) as source
                    on target.PORTFOLIO_ID = source.PORTFOLIO_ID
                       and target.PROPOSAL_ID is null
                       and date_trunc('day', target.TRADE_TS) = source.TRADE_DAY
                       and target.SYMBOL = source.SYMBOL
                       and target.SIDE = source.SIDE
                       and target.PRICE = source.PRICE
                       and target.QUANTITY = source.QUANTITY
                    when not matched then
                        insert (
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
                            source.PORTFOLIO_ID,
                            source.RUN_ID,
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

                    v_trade_rows_affected := SQLROWCOUNT;
                    if (v_trade_rows_affected > 0) then
                        v_trade_inserted := v_trade_inserted + v_trade_rows_affected;
                        v_trade_count := v_trade_count + v_trade_rows_affected;
                    else
                        v_trade_dedup_skipped := v_trade_dedup_skipped + 1;
                    end if;

                    delete from TEMP_POSITIONS
                     where SYMBOL = :v_position_symbol
                       and MARKET_TYPE = :v_position_market_type
                       and ENTRY_TS = :v_position_entry_ts;
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
           and vb.TS = :v_bar_ts;

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
            v_signal_sql := '
                select *
                from TEMP_SIGNALS
                where ENTRY_TS = ?
                order by SCORE desc
            ';
            v_signal_rs := (execute immediate :v_signal_sql using (v_bar_ts));
            for rec in v_signal_rs do
                declare
                    v_buy_price number(18,8);
                    v_target_value number(18,8);
                    v_buy_qty number(18,8);
                    v_buy_cost number(18,8);
                    v_signal_symbol string;
                    v_signal_market_type string;
                    v_signal_entry_ts timestamp_ntz;
                    v_signal_entry_index number;
                    v_signal_hold_until_index number;
                    v_signal_score number(18,10);
                begin
                    v_signal_symbol := rec.SYMBOL;
                    v_signal_market_type := rec.MARKET_TYPE;
                    v_signal_entry_ts := rec.ENTRY_TS;
                    v_signal_entry_index := rec.ENTRY_INDEX;
                    v_signal_hold_until_index := rec.HOLD_UNTIL_INDEX;
                    v_signal_score := rec.SCORE;

                    if (v_open_positions < v_max_positions) then
                        if (not exists (
                            select 1
                            from TEMP_POSITIONS
                            where SYMBOL = :v_signal_symbol
                              and MARKET_TYPE = :v_signal_market_type
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
                                    :v_signal_symbol,
                                    :v_signal_market_type,
                                    :v_signal_entry_ts,
                                    :v_buy_price,
                                    :v_buy_qty,
                                    :v_buy_cost,
                                    :v_signal_score,
                                    :v_signal_entry_index,
                                    :v_signal_hold_until_index
                                );

                                v_cash := v_cash - v_buy_cost;
                                v_trade_candidates := v_trade_candidates + 1;
                                v_trade_day := date_trunc('day', v_signal_entry_ts);

                                merge into MIP.APP.PORTFOLIO_TRADES as target
                                using (
                                    select
                                        :P_PORTFOLIO_ID as PORTFOLIO_ID,
                                        :v_run_id as RUN_ID,
                                        :v_signal_symbol as SYMBOL,
                                        :v_signal_market_type as MARKET_TYPE,
                                        1440 as INTERVAL_MINUTES,
                                        :v_signal_entry_ts as TRADE_TS,
                                        'BUY' as SIDE,
                                        :v_buy_price as PRICE,
                                        :v_buy_qty as QUANTITY,
                                        :v_buy_cost as NOTIONAL,
                                        null as REALIZED_PNL,
                                        :v_cash as CASH_AFTER,
                                        :v_signal_score as SCORE,
                                        :v_trade_day as TRADE_DAY
                                ) as source
                                on target.PORTFOLIO_ID = source.PORTFOLIO_ID
                                   and target.PROPOSAL_ID is null
                                   and date_trunc('day', target.TRADE_TS) = source.TRADE_DAY
                                   and target.SYMBOL = source.SYMBOL
                                   and target.SIDE = source.SIDE
                                   and target.PRICE = source.PRICE
                                   and target.QUANTITY = source.QUANTITY
                                when not matched then
                                    insert (
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
                                        source.PORTFOLIO_ID,
                                        source.RUN_ID,
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

                                v_trade_rows_affected := SQLROWCOUNT;
                                if (v_trade_rows_affected > 0) then
                                    v_trade_inserted := v_trade_inserted + v_trade_rows_affected;
                                    v_trade_count := v_trade_count + v_trade_rows_affected;
                                else
                                    v_trade_dedup_skipped := v_trade_dedup_skipped + 1;
                                end if;

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
                                    :v_signal_symbol,
                                    :v_signal_market_type,
                                    1440,
                                    :v_signal_entry_ts,
                                    :v_buy_price,
                                    :v_buy_qty,
                                    :v_buy_cost,
                                    :v_signal_score,
                                    :v_signal_entry_index,
                                    :v_signal_hold_until_index
                                );

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
           and vb.TS = :v_bar_ts;

        v_total_equity := v_cash + v_equity_value;

        insert into TEMP_DAILY_CASH (TS, CASH)
        values (:v_bar_ts, :v_cash);
    end for;

    create or replace temporary table TEMP_DAY_SPINE as
    select TS, BAR_INDEX
    from MIP.MART.V_BAR_INDEX
    where INTERVAL_MINUTES = 1440
      and TS between :P_FROM_TS and :P_TO_TS
    qualify row_number() over (partition by TS order by BAR_INDEX) = 1;

    create or replace temporary table TEMP_POSITION_DAYS as
    select
        d.TS,
        d.BAR_INDEX,
        p.SYMBOL,
        p.MARKET_TYPE,
        p.QUANTITY
    from MIP.APP.PORTFOLIO_POSITIONS p
    join TEMP_DAY_SPINE d
      on d.BAR_INDEX between p.ENTRY_INDEX and p.HOLD_UNTIL_INDEX
    where p.PORTFOLIO_ID = :P_PORTFOLIO_ID
      and p.RUN_ID = :v_run_id
      and p.INTERVAL_MINUTES = 1440;

    select count(*)
      into v_position_days_expanded
      from TEMP_POSITION_DAYS;

    create or replace temporary table TEMP_DAILY_EQUITY as
    select
        pd.TS,
        sum(pd.QUANTITY * vb.CLOSE) as EQUITY_VALUE,
        count(distinct pd.SYMBOL) as OPEN_POSITIONS
    from TEMP_POSITION_DAYS pd
    join MIP.MART.V_BAR_INDEX vb
      on vb.SYMBOL = pd.SYMBOL
     and vb.MARKET_TYPE = pd.MARKET_TYPE
     and vb.INTERVAL_MINUTES = 1440
     and vb.BAR_INDEX = pd.BAR_INDEX
    group by pd.TS;

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
    with daily_base as (
        select
            d.TS,
            d.BAR_INDEX,
            coalesce(c.CASH, 0) as CASH,
            coalesce(e.EQUITY_VALUE, 0) as EQUITY_VALUE,
            coalesce(e.OPEN_POSITIONS, 0) as OPEN_POSITIONS
        from TEMP_DAY_SPINE d
        left join TEMP_DAILY_CASH c
          on c.TS = d.TS
        left join TEMP_DAILY_EQUITY e
          on e.TS = d.TS
    ),
    daily_calc as (
        select
            TS,
            CASH,
            EQUITY_VALUE,
            OPEN_POSITIONS,
            CASH + EQUITY_VALUE as TOTAL_EQUITY,
            lag(CASH + EQUITY_VALUE) over (order by TS) as PREV_TOTAL_EQUITY,
            max(CASH + EQUITY_VALUE) over (order by TS rows between unbounded preceding and current row) as PEAK_EQUITY
        from daily_base
    )
    select
        :P_PORTFOLIO_ID,
        :v_run_id,
        TS,
        CASH,
        EQUITY_VALUE,
        TOTAL_EQUITY,
        OPEN_POSITIONS,
        case
            when PREV_TOTAL_EQUITY is null then 0
            else TOTAL_EQUITY - PREV_TOTAL_EQUITY
        end as DAILY_PNL,
        case
            when PREV_TOTAL_EQUITY is null or PREV_TOTAL_EQUITY = 0 then null
            else (TOTAL_EQUITY - PREV_TOTAL_EQUITY) / PREV_TOTAL_EQUITY
        end as DAILY_RETURN,
        PEAK_EQUITY,
        case
            when PEAK_EQUITY = 0 then null
            else (PEAK_EQUITY - TOTAL_EQUITY) / PEAK_EQUITY
        end as DRAWDOWN,
        'ACTIVE'
    from daily_calc
    order by TS;

    select count(*)
      into v_daily_count
      from TEMP_DAY_SPINE;

    select
        max(case when rn = 1 then TOTAL_EQUITY else null end),
        max(DRAWDOWN),
        sum(case when DAILY_PNL > 0 then 1 else 0 end),
        sum(case when DAILY_PNL < 0 then 1 else 0 end),
        min(case
            when TOTAL_EQUITY <= :v_starting_cash * :v_bust_equity_pct then TS
            else null
        end)
      into v_final_equity,
           v_max_drawdown,
           v_win_days,
           v_loss_days,
           v_bust_at
      from (
        select
            TS,
            TOTAL_EQUITY,
            DAILY_PNL,
            DRAWDOWN,
            row_number() over (order by TS desc) as rn
        from MIP.APP.PORTFOLIO_DAILY
        where PORTFOLIO_ID = :P_PORTFOLIO_ID
          and RUN_ID = :v_run_id
      ) run_daily;

    v_total_return := case
        when v_starting_cash is null or v_starting_cash = 0 then null
        else v_final_equity / v_starting_cash - 1
    end;

    v_last_simulated_at := current_timestamp();

    update MIP.APP.PORTFOLIO
       set LAST_SIMULATION_RUN_ID = :v_run_id,
           LAST_SIMULATED_AT = :v_last_simulated_at,
           FINAL_EQUITY = :v_final_equity,
           TOTAL_RETURN = :v_total_return,
           MAX_DRAWDOWN = :v_max_drawdown,
           WIN_DAYS = :v_win_days,
           LOSS_DAYS = :v_loss_days,
           BUST_AT = :v_bust_at,
           UPDATED_AT = :v_last_simulated_at
     where PORTFOLIO_ID = :P_PORTFOLIO_ID;

    call MIP.APP.SP_LOG_EVENT(
        'PORTFOLIO_SIM',
        'SUCCESS',
        'SUCCESS',
        :v_daily_count,
        object_construct(
            'portfolio_id', :P_PORTFOLIO_ID,
            'run_id', :v_run_id,
            'trades', :v_trade_count,
            'trade_candidates', :v_trade_candidates,
            'trade_inserted', :v_trade_inserted,
            'trade_dedup_skipped', :v_trade_dedup_skipped,
            'positions', :v_position_count,
            'daily_rows', :v_daily_count,
            'position_days_expanded', :v_position_days_expanded,
            'final_equity', :v_final_equity,
            'total_return', :v_total_return,
            'max_drawdown', :v_max_drawdown,
            'win_days', :v_win_days,
            'loss_days', :v_loss_days,
            'bust_at', :v_bust_at,
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
        'trade_candidates', :v_trade_candidates,
        'trade_inserted', :v_trade_inserted,
        'trade_dedup_skipped', :v_trade_dedup_skipped,
        'positions', :v_position_count,
        'daily_rows', :v_daily_count,
        'position_days_expanded', :v_position_days_expanded,
        'final_equity', :v_final_equity,
        'total_return', :v_total_return,
        'max_drawdown', :v_max_drawdown,
        'win_days', :v_win_days,
        'loss_days', :v_loss_days,
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

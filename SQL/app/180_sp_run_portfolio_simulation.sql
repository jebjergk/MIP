-- 180_sp_run_portfolio_simulation.sql
-- Purpose: Deterministic v1 portfolio simulation for paper portfolios
--
-- ARCHITECTURE NOTE — BAR_INDEX drift protection:
-- V_BAR_INDEX computes BAR_INDEX as ROW_NUMBER() over MARKET_BARS. This value
-- shifts whenever bars are added to or removed from MARKET_BARS (e.g. by the
-- ingest step). PORTFOLIO_POSITIONS stores ENTRY_INDEX and HOLD_UNTIL_INDEX as
-- materialized numbers, which become stale after any bar data change.
--
-- To protect against this, we NEVER trust stored BAR_INDEX values directly.
-- Instead, we re-derive the current BAR_INDEX from ENTRY_TS (which is stable)
-- and preserve the relative horizon (HOLD_UNTIL_INDEX - ENTRY_INDEX) to compute
-- the current hold-until bar. This applies to:
--   1. Loading positions into TEMP_POSITIONS (join V_BAR_INDEX on ENTRY_TS)
--   2. TEMP_DAILY_SNAPSHOT stores cash + equity from the bar loop (single source of truth)
--   3. MERGE key for PORTFOLIO_POSITIONS (uses ENTRY_TS, not ENTRY_INDEX)

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
    -- Copy parameters to local variables to avoid binding issues with exception handlers
    v_portfolio_id number := :P_PORTFOLIO_ID;
    v_from_ts timestamp_ntz := :P_FROM_TS;
    v_to_ts timestamp_ntz := :P_TO_TS;
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
    v_slippage_bps number(18,8);
    v_fee_bps number(18,8);
    v_min_fee number(18,8);
    v_spread_bps number(18,8);
    v_last_sim_run_id string;
    v_effective_from_ts timestamp_ntz;
    v_error_query_id string;
begin
    -- Declare episode variables
    let v_episode_id number;
    let v_episode_start_ts timestamp_ntz;
    
    let v_cooldown_until_ts timestamp_ntz;

    select
        p.STARTING_CASH,
        p.PROFILE_ID,
        p.LAST_SIMULATION_RUN_ID,
        prof.MAX_POSITIONS,
        prof.MAX_POSITION_PCT,
        prof.BUST_EQUITY_PCT,
        prof.BUST_ACTION,
        prof.DRAWDOWN_STOP_PCT,
        p.COOLDOWN_UNTIL_TS
      into v_starting_cash,
           v_profile_id,
           v_last_sim_run_id,
           v_max_positions,
           v_max_position_pct,
           v_bust_equity_pct,
           v_bust_action,
           v_drawdown_stop_pct,
           v_cooldown_until_ts
      from MIP.APP.PORTFOLIO p
      left join MIP.APP.PORTFOLIO_PROFILE prof
        on prof.PROFILE_ID = p.PROFILE_ID
     where p.PORTFOLIO_ID = :v_portfolio_id;

    -- Get the active episode ID and start timestamp
    -- This scopes all data to the current episode lifecycle
    begin
        select EPISODE_ID, START_TS
          into :v_episode_id, :v_episode_start_ts
          from MIP.APP.PORTFOLIO_EPISODE
         where PORTFOLIO_ID = :v_portfolio_id
           and STATUS = 'ACTIVE'
         order by START_TS desc
         limit 1;
    exception
        when other then
            v_episode_id := null;
            v_episode_start_ts := null;
    end;

    -- Determine effective_from_ts with episode boundary awareness:
    --
    -- CRITICAL: episode_start_ts includes the time of day (e.g. 07:30 AM),
    -- but bar timestamps are midnight (00:00:00). We must truncate to day,
    -- otherwise effective_from > to_ts and the bar loop processes zero rows.
    --
    -- Priority:
    -- 1. If an active episode exists, ALWAYS process from episode start day.
    --    This ensures sells, daily snapshots, and PNL are computed correctly
    --    even after a fresh reset (LAST_SIMULATION_RUN_ID = null).
    --    The MERGE dedup keys prevent duplicate trades on re-processed bars.
    -- 2. If no episode but have last run, use requested from_ts.
    -- 3. If no episode and no last run, use today only.
    if (v_episode_start_ts is not null) then
        -- Has active episode: process from episode start (or requested from, whichever is later)
        v_effective_from_ts := greatest(date_trunc('day', :v_episode_start_ts), :v_from_ts);
    elseif (v_last_sim_run_id is null) then
        -- Fresh reset without episode: only simulate today
        v_effective_from_ts := date_trunc('day', v_to_ts);
    else
        -- No episode tracking: use requested from
        v_effective_from_ts := v_from_ts;
    end if;

    call MIP.APP.SP_LOG_EVENT(
        'PORTFOLIO_SIM',
        'START',
        'INFO',
        null,
        object_construct(
            'portfolio_id', :v_portfolio_id,
            'from_ts', :v_from_ts,
            'to_ts', :v_to_ts,
            'effective_from_ts', :v_effective_from_ts,
            'episode_id', :v_episode_id,
            'episode_start_ts', :v_episode_start_ts,
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
                'portfolio_id', :v_portfolio_id,
                'reason', 'PORTFOLIO_NOT_FOUND'
            ),
            'Portfolio not found',
            :v_run_id,
            null
        );

        return object_construct(
            'status', 'ERROR',
            'message', 'Portfolio not found',
            'portfolio_id', :v_portfolio_id,
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

    -- COOLDOWN enforcement: block new entries if still within cooldown window.
    -- COOLDOWN_UNTIL_TS is set by SP_CHECK_CRYSTALLIZE after profit target hit.
    -- During cooldown the portfolio should hold (exits via bar expiry still allowed)
    -- but NOT open any new positions.
    if (v_cooldown_until_ts is not null and v_to_ts < v_cooldown_until_ts) then
        v_entries_blocked := true;
        v_block_reason := 'COOLDOWN';
    end if;

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

    -- Stores cash, equity value, and open position count per bar from the
    -- authoritative bar loop.  The bar loop correctly removes sold positions
    -- from TEMP_POSITIONS before calculating equity, so these values are the
    -- single source of truth for PORTFOLIO_DAILY.
    create or replace temporary table TEMP_DAILY_SNAPSHOT (
        TS timestamp_ntz,
        CASH number(18,2),
        EQUITY_VALUE number(18,2),
        OPEN_POSITIONS number
    );

    -- ============================================================
    -- LOAD EXISTING OPEN POSITIONS into TEMP_POSITIONS
    -- This ensures the simulation knows about agent-created positions.
    --
    -- CRITICAL: BAR_INDEX drift protection + ENTRY_TS mismatch handling.
    -- 1. V_BAR_INDEX computes BAR_INDEX via ROW_NUMBER(), which drifts when
    --    bars are added/removed. Stored ENTRY_INDEX becomes stale.
    -- 2. ENTRY_TS may be the pipeline execution timestamp (e.g. 21:31:10 or
    --    even the next day for weekend runs), NOT the bar midnight timestamp.
    -- Solution: find the latest bar ON OR BEFORE ENTRY_TS using <= + QUALIFY.
    -- This handles both BAR_INDEX drift AND timestamp granularity mismatches.
    -- ============================================================
    insert into TEMP_POSITIONS (
        SYMBOL, MARKET_TYPE, ENTRY_TS, ENTRY_PRICE,
        QUANTITY, COST_BASIS, ENTRY_SCORE, ENTRY_INDEX, HOLD_UNTIL_INDEX
    )
    select
        p.SYMBOL,
        p.MARKET_TYPE,
        p.ENTRY_TS,
        p.ENTRY_PRICE,
        p.QUANTITY,
        p.COST_BASIS,
        p.ENTRY_SCORE,
        vb.BAR_INDEX as ENTRY_INDEX,
        vb.BAR_INDEX + (p.HOLD_UNTIL_INDEX - p.ENTRY_INDEX) as HOLD_UNTIL_INDEX
    from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL p
    join MIP.MART.V_BAR_INDEX vb
      on vb.SYMBOL = p.SYMBOL
     and vb.MARKET_TYPE = p.MARKET_TYPE
     and vb.INTERVAL_MINUTES = 1440
     and vb.TS <= p.ENTRY_TS
    where p.PORTFOLIO_ID = :v_portfolio_id
    qualify row_number() over (
        partition by p.PORTFOLIO_ID, p.SYMBOL, p.MARKET_TYPE, p.ENTRY_TS
        order by vb.TS desc
    ) = 1;

    -- Determine current cash balance. Priority:
    -- 1. Latest CASH_AFTER from PORTFOLIO_TRADES within the current episode
    --    (scoped by EPISODE_ID to avoid pulling cash from a prior episode)
    -- 2. Fallback: starting_cash - cost of all open positions
    --
    -- After loading trade cash, adjust for any DEPOSIT/WITHDRAW lifecycle events
    -- that occurred SINCE the last trade. SP_PORTFOLIO_CASH_EVENT updates
    -- PORTFOLIO.STARTING_CASH but that change is NOT reflected in
    -- PORTFOLIO_TRADES.CASH_AFTER until a new trade is recorded.
    begin
        let v_trade_cash number(18,2) := null;
        let v_last_trade_ts timestamp_ntz := null;
        let v_cash_event_delta number(18,2) := 0;
        begin
            -- CRITICAL: Also filter TRADE_TS >= episode_start_ts.
            -- Without this, trades from prior episodes that were mis-assigned
            -- (e.g. via backfill) to the current EPISODE_ID can corrupt v_cash
            -- with a stale CASH_AFTER from a completely different cash state.
            select CASH_AFTER, TRADE_TS into :v_trade_cash, :v_last_trade_ts
              from MIP.APP.PORTFOLIO_TRADES
             where PORTFOLIO_ID = :v_portfolio_id
               and (EPISODE_ID = :v_episode_id or (:v_episode_id is null and EPISODE_ID is null))
               and (:v_episode_start_ts is null or TRADE_TS >= :v_episode_start_ts)
             order by TRADE_TS desc, TRADE_ID desc
             limit 1;
            if (v_trade_cash is not null) then
                -- Check for DEPOSIT/WITHDRAW events after the last trade
                select coalesce(sum(
                    case when EVENT_TYPE = 'DEPOSIT'  then AMOUNT
                         when EVENT_TYPE = 'WITHDRAW' then -AMOUNT
                         else 0 end
                ), 0) into :v_cash_event_delta
                  from MIP.APP.PORTFOLIO_LIFECYCLE_EVENT
                 where PORTFOLIO_ID = :v_portfolio_id
                   and EVENT_TYPE in ('DEPOSIT', 'WITHDRAW')
                   and EVENT_TS > :v_last_trade_ts;
                v_cash := v_trade_cash + v_cash_event_delta;
            end if;
        exception
            when other then null; -- No trade records yet
        end;
    end;

    -- If no trades exist, calculate cash from starting cash minus open position cost
    if (v_cash = v_starting_cash) then
        let v_existing_cost number(18,2) := 0;
        select coalesce(sum(COST_BASIS), 0) into :v_existing_cost from TEMP_POSITIONS;
        if (v_existing_cost > 0) then
            v_cash := v_starting_cash - v_existing_cost;
        end if;
    end if;

    select count(*) into :v_open_positions from TEMP_POSITIONS;

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
      and s.TS between :v_effective_from_ts and :v_to_ts
      and exit_bar.TS <= :v_to_ts
      -- Exclude signals for symbols that already have open positions
      -- (prevents simulation from re-entering agent-managed positions)
      and not exists (
          select 1 from TEMP_POSITIONS tp
          where tp.SYMBOL = s.SYMBOL
            and tp.MARKET_TYPE = s.MARKET_TYPE
      )
      -- ALSO exclude signals that already have a BUY trade in the current episode
      -- on the same day. Without this, re-runs re-enter signals for positions that
      -- were opened and then sold within the same episode, causing duplicate trades
      -- (the sold position is no longer in TEMP_POSITIONS but the trade record exists).
      and not exists (
          select 1 from MIP.APP.PORTFOLIO_TRADES t
          where t.PORTFOLIO_ID = :v_portfolio_id
            and (t.EPISODE_ID = :v_episode_id or (:v_episode_id is null and t.EPISODE_ID is null))
            and t.PROPOSAL_ID is null
            and t.SYMBOL = s.SYMBOL
            and t.SIDE = 'BUY'
            and date_trunc('day', t.TRADE_TS) = s.TS
      );

    v_bar_sql := '
        select TS, BAR_INDEX
        from MIP.MART.V_BAR_INDEX
        where INTERVAL_MINUTES = 1440
          and TS between ? and ?
        qualify row_number() over (partition by TS order by BAR_INDEX) = 1
        order by TS
    ';
    v_bar_rs := (execute immediate :v_bar_sql using (v_effective_from_ts, v_to_ts));

    for bar_row in v_bar_rs do
        v_bar_ts := bar_row.TS;
        v_bar_index := bar_row.BAR_INDEX;
        -- CRITICAL: Compare each position's HOLD_UNTIL_INDEX against its OWN
        -- symbol's BAR_INDEX for the current bar date, NOT the cross-symbol
        -- minimum v_bar_index from the bar loop. Different symbols (e.g. stocks
        -- vs FX) have different BAR_INDEX sequences because BAR_INDEX = ROW_NUMBER
        -- per symbol. Using the minimum would cause sells to fire too late (or never)
        -- for symbols whose BAR_INDEX is higher than the minimum.
        v_position_sql := '
            select tp.*
            from TEMP_POSITIONS tp
            join MIP.MART.V_BAR_INDEX vb
              on vb.SYMBOL = tp.SYMBOL
             and vb.MARKET_TYPE = tp.MARKET_TYPE
             and vb.INTERVAL_MINUTES = 1440
             and vb.TS = ?
            where tp.HOLD_UNTIL_INDEX <= vb.BAR_INDEX
        ';
        v_position_rs := (execute immediate :v_position_sql using (v_bar_ts));
        for position_row in v_position_rs do
            declare
                v_sell_price number(18,8);
                v_sell_exec_price number(18,8);
                v_sell_notional number(18,8);
                v_sell_fee number(18,8);
                v_sell_pnl number(18,8);
                v_position_symbol string;
                v_position_market_type string;
                v_position_entry_ts timestamp_ntz;
                v_position_entry_price number(18,8);
                v_position_qty number(18,8);
                v_position_cost_basis number(18,8);
                v_position_entry_score number(18,10);
            begin
                v_position_symbol := position_row.SYMBOL;
                v_position_market_type := position_row.MARKET_TYPE;
                v_position_entry_ts := position_row.ENTRY_TS;
                v_position_entry_price := position_row.ENTRY_PRICE;
                v_position_qty := position_row.QUANTITY;
                v_position_cost_basis := position_row.COST_BASIS;
                v_position_entry_score := position_row.ENTRY_SCORE;

                select CLOSE
                  into v_sell_price
                  from MIP.MART.V_BAR_INDEX
                 where SYMBOL = :v_position_symbol
                   and MARKET_TYPE = :v_position_market_type
                   and INTERVAL_MINUTES = 1440
                   and TS = :v_bar_ts;

                if (v_sell_price is not null) then
                    v_sell_exec_price := v_sell_price * (1 - ((v_slippage_bps + (v_spread_bps / 2)) / 10000));
                    v_sell_notional := v_sell_exec_price * v_position_qty;
                    v_sell_fee := greatest(coalesce(v_min_fee, 0), abs(v_sell_notional) * v_fee_bps / 10000);
                    v_sell_pnl := v_sell_notional - v_sell_fee - v_position_cost_basis;
                    v_cash := v_cash + v_sell_notional - v_sell_fee;
                    v_trade_candidates := v_trade_candidates + 1;
                    v_trade_day := date_trunc('day', v_bar_ts);

                    -- MERGE dedup key: stable columns only.
                    -- NEVER use PRICE or QUANTITY in the match key — they are
                    -- computed from v_cash which drifts between re-runs, causing
                    -- the MERGE to miss existing rows and insert duplicates.
                    -- Natural key: (PORTFOLIO_ID, EPISODE_ID, TRADE_DAY, SYMBOL, SIDE)
                    -- is unique for simulation trades (one trade per symbol/side/day).
                    merge into MIP.APP.PORTFOLIO_TRADES as target
                    using (
                        select
                            :v_portfolio_id as PORTFOLIO_ID,
                            :v_run_id as RUN_ID,
                            :v_episode_id as EPISODE_ID,
                            :v_position_symbol as SYMBOL,
                            :v_position_market_type as MARKET_TYPE,
                            1440 as INTERVAL_MINUTES,
                            :v_bar_ts as TRADE_TS,
                            'SELL' as SIDE,
                            :v_sell_exec_price as PRICE,
                            :v_position_qty as QUANTITY,
                            :v_sell_notional as NOTIONAL,
                            :v_sell_pnl as REALIZED_PNL,
                            :v_cash as CASH_AFTER,
                            :v_position_entry_score as SCORE,
                            :v_trade_day as TRADE_DAY
                    ) as source
                    on target.PORTFOLIO_ID = source.PORTFOLIO_ID
                       and target.PROPOSAL_ID is null
                       and (target.EPISODE_ID = source.EPISODE_ID or (target.EPISODE_ID is null and source.EPISODE_ID is null))
                       and date_trunc('day', target.TRADE_TS) = source.TRADE_DAY
                       and target.SYMBOL = source.SYMBOL
                       and target.SIDE = source.SIDE
                    when not matched then
                        insert (
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

        -- Size positions from available CASH, not total equity.
        -- Equity includes unrealized position value which is not secure.
        v_max_position_value := v_cash * v_max_position_pct;

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
                    v_buy_exec_price number(18,8);
                    v_target_value number(18,8);
                    v_buy_qty number(18,8);
                    v_buy_notional number(18,8);
                    v_buy_fee number(18,8);
                    v_total_cost number(18,8);
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
                            v_buy_exec_price := v_buy_price * (1 + ((v_slippage_bps + (v_spread_bps / 2)) / 10000));
                            v_buy_notional := v_buy_qty * v_buy_exec_price;
                            v_buy_fee := greatest(coalesce(v_min_fee, 0), abs(v_buy_notional) * v_fee_bps / 10000);
                            v_total_cost := v_buy_notional + v_buy_fee;

                            if (v_buy_qty > 0 and v_total_cost <= v_cash) then
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
                                    :v_buy_exec_price,
                                    :v_buy_qty,
                                    :v_total_cost,
                                    :v_signal_score,
                                    :v_signal_entry_index,
                                    :v_signal_hold_until_index
                                );

                                v_cash := v_cash - v_total_cost;
                                v_trade_candidates := v_trade_candidates + 1;
                                v_trade_day := date_trunc('day', v_signal_entry_ts);

                                -- MERGE dedup key: stable columns only (see SELL trade comment above).
                                merge into MIP.APP.PORTFOLIO_TRADES as target
                                using (
                                    select
                                        :v_portfolio_id as PORTFOLIO_ID,
                                        :v_run_id as RUN_ID,
                                        :v_episode_id as EPISODE_ID,
                                        :v_signal_symbol as SYMBOL,
                                        :v_signal_market_type as MARKET_TYPE,
                                        1440 as INTERVAL_MINUTES,
                                        :v_signal_entry_ts as TRADE_TS,
                                        'BUY' as SIDE,
                                        :v_buy_exec_price as PRICE,
                                        :v_buy_qty as QUANTITY,
                                        :v_buy_notional as NOTIONAL,
                                        null as REALIZED_PNL,
                                        :v_cash as CASH_AFTER,
                                        :v_signal_score as SCORE,
                                        :v_trade_day as TRADE_DAY
                                ) as source
                                on target.PORTFOLIO_ID = source.PORTFOLIO_ID
                                   and target.PROPOSAL_ID is null
                                   and (target.EPISODE_ID = source.EPISODE_ID or (target.EPISODE_ID is null and source.EPISODE_ID is null))
                                   and date_trunc('day', target.TRADE_TS) = source.TRADE_DAY
                                   and target.SYMBOL = source.SYMBOL
                                   and target.SIDE = source.SIDE
                                when not matched then
                                    insert (
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

                                v_trade_rows_affected := SQLROWCOUNT;
                                if (v_trade_rows_affected > 0) then
                                    v_trade_inserted := v_trade_inserted + v_trade_rows_affected;
                                    v_trade_count := v_trade_count + v_trade_rows_affected;
                                else
                                    v_trade_dedup_skipped := v_trade_dedup_skipped + 1;
                                end if;

                                -- Use MERGE to prevent duplicate positions on pipeline re-runs.
                                -- Match on (PORTFOLIO_ID, SYMBOL, MARKET_TYPE, ENTRY_TS).
                                -- NOTE: ENTRY_INDEX excluded from match key because V_BAR_INDEX
                                -- computes it via ROW_NUMBER() which can drift; ENTRY_TS is stable.
                                merge into MIP.APP.PORTFOLIO_POSITIONS as ptgt
                                using (
                                    select
                                        :v_portfolio_id as PORTFOLIO_ID,
                                        :v_run_id as RUN_ID,
                                        :v_episode_id as EPISODE_ID,
                                        :v_signal_symbol as SYMBOL,
                                        :v_signal_market_type as MARKET_TYPE,
                                        1440 as INTERVAL_MINUTES,
                                        :v_signal_entry_ts as ENTRY_TS,
                                        :v_buy_exec_price as ENTRY_PRICE,
                                        :v_buy_qty as QUANTITY,
                                        :v_total_cost as COST_BASIS,
                                        :v_signal_score as ENTRY_SCORE,
                                        :v_signal_entry_index as ENTRY_INDEX,
                                        :v_signal_hold_until_index as HOLD_UNTIL_INDEX
                                ) as psrc
                                on ptgt.PORTFOLIO_ID = psrc.PORTFOLIO_ID
                                   and ptgt.SYMBOL = psrc.SYMBOL
                                   and ptgt.MARKET_TYPE = psrc.MARKET_TYPE
                                   and ptgt.ENTRY_TS = psrc.ENTRY_TS
                                when not matched then
                                    insert (
                                        PORTFOLIO_ID, RUN_ID, EPISODE_ID, SYMBOL, MARKET_TYPE,
                                        INTERVAL_MINUTES, ENTRY_TS, ENTRY_PRICE, QUANTITY,
                                        COST_BASIS, ENTRY_SCORE, ENTRY_INDEX, HOLD_UNTIL_INDEX
                                    )
                                    values (
                                        psrc.PORTFOLIO_ID, psrc.RUN_ID, psrc.EPISODE_ID, psrc.SYMBOL, psrc.MARKET_TYPE,
                                        psrc.INTERVAL_MINUTES, psrc.ENTRY_TS, psrc.ENTRY_PRICE, psrc.QUANTITY,
                                        psrc.COST_BASIS, psrc.ENTRY_SCORE, psrc.ENTRY_INDEX, psrc.HOLD_UNTIL_INDEX
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
        v_open_positions := (select count(*) from TEMP_POSITIONS);

        insert into TEMP_DAILY_SNAPSHOT (TS, CASH, EQUITY_VALUE, OPEN_POSITIONS)
        values (:v_bar_ts, :v_cash, :v_equity_value, :v_open_positions);
    end for;

    -- Count position-days for diagnostic output (how many position×bar combinations)
    select count(*) into v_position_days_expanded from TEMP_DAILY_SNAPSHOT where EQUITY_VALUE > 0;

    -- ============================================================
    -- POST-PROCESS: Correct daily CASH from actual trade history.
    --
    -- During replay (processing from episode start), v_cash in the bar loop
    -- starts from the latest CASH_AFTER and only changes when NEW sells fire.
    -- Buy deductions are missed because TEMP_SIGNALS excludes signals with
    -- existing trades. This leaves daily CASH wrong for bars before the buys.
    --
    -- Fix: replace each snapshot's CASH with the last CASH_AFTER from
    -- PORTFOLIO_TRADES on or before that date. For dates before the first
    -- trade, use starting_cash. This gives the true cash state at each day.
    -- ============================================================
    -- Build a cash timeline from existing trade records.
    -- For each trade day, get the last CASH_AFTER (i.e. cash state at end of that day).
    create or replace temporary table TEMP_TRADE_CASH_TIMELINE as
    select
        date_trunc('day', TRADE_TS) as TRADE_DAY,
        CASH_AFTER
    from MIP.APP.PORTFOLIO_TRADES
    where PORTFOLIO_ID = :v_portfolio_id
      and (EPISODE_ID = :v_episode_id or (:v_episode_id is null and EPISODE_ID is null))
    qualify row_number() over (
        partition by date_trunc('day', TRADE_TS)
        order by TRADE_TS desc, TRADE_ID desc
    ) = 1;

    -- For each daily snapshot, find the correct cash: the last CASH_AFTER
    -- from trades on or before that date, or starting_cash if none.
    -- Uses a left join + window function to carry forward the last known cash.
    update TEMP_DAILY_SNAPSHOT ds
       set CASH = cc.CORRECT_CASH
      from (
          select
              snap.TS,
              coalesce(
                  tc.CASH_AFTER,
                  last_value(tc.CASH_AFTER ignore nulls) over (order by snap.TS rows between unbounded preceding and current row),
                  :v_starting_cash
              ) as CORRECT_CASH
          from TEMP_DAILY_SNAPSHOT snap
          left join TEMP_TRADE_CASH_TIMELINE tc
            on tc.TRADE_DAY = snap.TS
      ) cc
     where ds.TS = cc.TS;

    -- ============================================================
    -- POST-PROCESS: Correct daily EQUITY_VALUE from position history.
    --
    -- During replay, TEMP_POSITIONS only contains currently-open positions
    -- (from the canonical view). Historical bars incorrectly show these
    -- positions' values even on bars BEFORE they were opened, and miss
    -- positions that were opened and closed within the episode.
    --
    -- Fix: use FIFO matching to determine which positions were open on
    -- each bar date. For each symbol, rank positions by ENTRY_TS (earliest
    -- first). Count cumulative sells per symbol up to each date. A position
    -- is open if its FIFO rank > the number of sells for that symbol by
    -- that date.
    -- ============================================================

    -- Step 1: Use BUY trades (not PORTFOLIO_POSITIONS) as the authoritative
    -- source of position openings. PORTFOLIO_POSITIONS may have gaps from
    -- data repairs. Cross-join each bar date with BUY trades on or before it.
    -- Rank per symbol in FIFO order (earliest buy first).
    create or replace temporary table TEMP_POSITION_TIMELINE as
    select
        snap.TS as BAR_TS,
        bt.SYMBOL,
        bt.MARKET_TYPE,
        bt.QUANTITY,
        bt.TRADE_TS as ENTRY_TS,
        vb.CLOSE as BAR_CLOSE,
        row_number() over (
            partition by snap.TS, bt.SYMBOL, bt.MARKET_TYPE
            order by bt.TRADE_TS, bt.TRADE_ID
        ) as POS_RANK
    from TEMP_DAILY_SNAPSHOT snap
    cross join MIP.APP.PORTFOLIO_TRADES bt
    left join MIP.MART.V_BAR_INDEX vb
      on vb.SYMBOL = bt.SYMBOL
     and vb.MARKET_TYPE = bt.MARKET_TYPE
     and vb.INTERVAL_MINUTES = 1440
     and vb.TS = snap.TS
    where bt.PORTFOLIO_ID = :v_portfolio_id
      and (bt.EPISODE_ID = :v_episode_id or (:v_episode_id is null and bt.EPISODE_ID is null))
      and bt.SIDE = 'BUY'
      and date_trunc('day', bt.TRADE_TS) <= snap.TS;

    -- Step 2: Count cumulative sells per symbol per bar date.
    create or replace temporary table TEMP_SELL_COUNTS as
    select
        snap.TS as BAR_TS,
        sell.SYMBOL,
        sell.MARKET_TYPE,
        count(*) as SELL_COUNT
    from TEMP_DAILY_SNAPSHOT snap
    join MIP.APP.PORTFOLIO_TRADES sell
      on sell.PORTFOLIO_ID = :v_portfolio_id
     and (sell.EPISODE_ID = :v_episode_id or (:v_episode_id is null and sell.EPISODE_ID is null))
     and sell.SIDE = 'SELL'
     and date_trunc('day', sell.TRADE_TS) <= snap.TS
    group by snap.TS, sell.SYMBOL, sell.MARKET_TYPE;

    -- Step 3: A position is OPEN if its FIFO rank > cumulative sells for that symbol.
    -- First reset equity to 0 for ALL bars (handles bars with no open positions,
    -- e.g. after all positions were sold that day — the bar loop's original equity
    -- from TEMP_POSITIONS would otherwise persist).
    update TEMP_DAILY_SNAPSHOT set EQUITY_VALUE = 0, OPEN_POSITIONS = 0;

    -- Then update bars that DO have open positions with correct values.
    update TEMP_DAILY_SNAPSHOT ds
       set EQUITY_VALUE = eq.CORRECT_EQUITY,
           OPEN_POSITIONS = eq.CORRECT_OPEN_POS
      from (
          select
              pt.BAR_TS,
              sum(pt.QUANTITY * pt.BAR_CLOSE) as CORRECT_EQUITY,
              count(*) as CORRECT_OPEN_POS
          from TEMP_POSITION_TIMELINE pt
          left join TEMP_SELL_COUNTS sc
            on sc.BAR_TS = pt.BAR_TS
           and sc.SYMBOL = pt.SYMBOL
           and sc.MARKET_TYPE = pt.MARKET_TYPE
          where pt.POS_RANK > coalesce(sc.SELL_COUNT, 0)
            and pt.BAR_CLOSE is not null
          group by pt.BAR_TS
      ) eq
     where ds.TS = eq.BAR_TS;

    -- ============================================================
    -- DELETE existing PORTFOLIO_DAILY rows before re-inserting.
    -- Without this, every pipeline re-run would add DUPLICATE rows
    -- for the same (PORTFOLIO_ID, TS), causing cumulative metrics
    -- (P&L, equity, drawdown) to inflate on each run.
    -- Scoped to the current portfolio + episode + date range.
    -- ============================================================
    delete from MIP.APP.PORTFOLIO_DAILY
     where PORTFOLIO_ID = :v_portfolio_id
       and TS between :v_effective_from_ts and :v_to_ts
       and (
           (EPISODE_ID is not null and EPISODE_ID = :v_episode_id)
           or (EPISODE_ID is null and :v_episode_id is null)
       );

    insert into MIP.APP.PORTFOLIO_DAILY (
        PORTFOLIO_ID,
        RUN_ID,
        EPISODE_ID,
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
    -- TEMP_DAILY_SNAPSHOT is the authoritative source: it's written inside the
    -- bar loop AFTER sold positions are removed from TEMP_POSITIONS, so equity
    -- values correctly exclude positions that have been sold.
    with daily_calc as (
        select
            TS,
            CASH,
            EQUITY_VALUE,
            OPEN_POSITIONS,
            CASH + EQUITY_VALUE as TOTAL_EQUITY,
            lag(CASH + EQUITY_VALUE) over (order by TS) as PREV_TOTAL_EQUITY,
            max(CASH + EQUITY_VALUE) over (order by TS rows between unbounded preceding and current row) as PEAK_EQUITY
        from TEMP_DAILY_SNAPSHOT
    )
    select
        :v_portfolio_id,
        :v_run_id,
        :v_episode_id,
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
      from TEMP_DAILY_SNAPSHOT;

    -- Compute stats scoped to the current episode.
    -- Filter by both RUN_ID and EPISODE_ID to prevent cross-episode contamination.
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
        where PORTFOLIO_ID = :v_portfolio_id
          and RUN_ID = :v_run_id
          and (EPISODE_ID = :v_episode_id or (:v_episode_id is null and EPISODE_ID is null))
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
     where PORTFOLIO_ID = :v_portfolio_id;

    -- Post-step: check profile-driven crystallization (profit target); end episode, write results, start next.
    call MIP.APP.SP_CHECK_CRYSTALLIZE(
        :v_portfolio_id,
        :v_run_id,
        :v_final_equity,
        :v_to_ts,
        :v_win_days,
        :v_loss_days,
        :v_max_drawdown,
        :v_trade_count
    );

    call MIP.APP.SP_LOG_EVENT(
        'PORTFOLIO_SIM',
        'SUCCESS',
        'SUCCESS',
        :v_daily_count,
        object_construct(
            'portfolio_id', :v_portfolio_id,
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
        'portfolio_id', :v_portfolio_id,
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
        v_error_query_id := last_query_id();
        
        call MIP.APP.SP_LOG_EVENT(
            'PORTFOLIO_SIM',
            'FAIL',
            'ERROR',
            null,
            object_construct(
                'portfolio_id', :v_portfolio_id,
                'run_id', :v_run_id
            ),
            :sqlerrm,
            :v_run_id,
            null,
            null,                    -- P_ROOT_RUN_ID
            null,                    -- P_EVENT_RUN_ID
            :sqlstate,               -- P_ERROR_SQLSTATE
            :v_error_query_id,       -- P_ERROR_QUERY_ID
            object_construct(        -- P_ERROR_CONTEXT
                'proc_name', 'SP_RUN_PORTFOLIO_SIMULATION',
                'portfolio_id', :v_portfolio_id,
                'run_id', :v_run_id,
                'from_ts', :v_from_ts,
                'to_ts', :v_to_ts
            ),
            null                     -- P_DURATION_MS (not tracked at this level)
        );

        return object_construct(
            'status', 'ERROR',
            'run_id', :v_run_id,
            'portfolio_id', :v_portfolio_id,
            'error', :sqlerrm,
            'sqlstate', :sqlstate,
            'query_id', :v_error_query_id
        );
end;
$$;

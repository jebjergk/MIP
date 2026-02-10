-- 170_sp_check_crystallize.sql
-- Purpose: After simulation, check if profit target hit (EOD); end episode, write results, start next.
-- No hardcoded rules; all from MIP.APP.PORTFOLIO_PROFILE (CRYSTALLIZE_*).

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.APP.PORTFOLIO
    add column if not exists COOLDOWN_UNTIL_TS timestamp_ntz;

create or replace procedure MIP.APP.SP_CHECK_CRYSTALLIZE(
    P_PORTFOLIO_ID      number,
    P_RUN_ID            string,
    P_FINAL_EQUITY      number,
    P_FINAL_TS          timestamp_ntz,
    P_WIN_DAYS          number,
    P_LOSS_DAYS         number,
    P_MAX_DRAWDOWN_PCT  number,
    P_TRADES_COUNT      number
)
returns boolean
language sql
execute as caller
as
$$
declare
    -- Copy parameters to local variables to avoid binding issues
    v_portfolio_id number := :P_PORTFOLIO_ID;
    v_run_id string := :P_RUN_ID;
    v_final_equity number := :P_FINAL_EQUITY;
    v_final_ts timestamp_ntz := :P_FINAL_TS;
    v_episode_id number;
    v_profile_id number;
    v_start_equity number(18,2);
    v_start_ts timestamp_ntz;
    v_crystallize_enabled boolean;
    v_profit_target_pct number(18,6);
    v_crystallize_mode varchar(32);
    v_cooldown_days number;
    v_ended_ts timestamp_ntz;
    v_return_pct number(18,6);
    v_distribution_amt number(18,2) := 0;
    v_baseline_cash number(18,2);
    v_next_start_equity number(18,2);
    v_win_days number;
    v_loss_days number;
    v_max_dd_pct number(18,6);
    v_trades_count number;
begin
    -- Initialize v_ended_ts here (can't use :v_final_ts in declare block)
    v_ended_ts := coalesce(:v_final_ts, current_timestamp());
    
    if (v_final_equity is null) then
        return false;
    end if;

    -- Replaced cursor c_ep with SELECT INTO
    select EPISODE_ID, PROFILE_ID, START_EQUITY, START_TS
      into :v_episode_id, :v_profile_id, :v_start_equity, :v_start_ts
      from MIP.APP.PORTFOLIO_EPISODE
     where PORTFOLIO_ID = :v_portfolio_id and STATUS = 'ACTIVE'
     limit 1;

    if (v_start_equity is null) then
        -- Replaced cursor c_baseline with SELECT INTO
        select STARTING_CASH into :v_start_equity
          from MIP.APP.PORTFOLIO
         where PORTFOLIO_ID = :v_portfolio_id;
    end if;

    if (v_episode_id is null or v_start_equity is null or v_start_equity <= 0) then
        return false;
    end if;

    -- Replaced cursor c_prof with SELECT INTO
    select coalesce(CRYSTALLIZE_ENABLED, false), PROFIT_TARGET_PCT, CRYSTALLIZE_MODE, COOLDOWN_DAYS
      into :v_crystallize_enabled, :v_profit_target_pct, :v_crystallize_mode, :v_cooldown_days
      from MIP.APP.PORTFOLIO_PROFILE
     where PROFILE_ID = :v_profile_id;

    if (not v_crystallize_enabled or v_profit_target_pct is null) then
        return false;
    end if;

    v_return_pct := (v_final_equity / v_start_equity) - 1;
    if (v_return_pct is null or v_return_pct < v_profit_target_pct) then
        return false;
    end if;

    -- Episode-scoped stats from PORTFOLIO_DAILY and PORTFOLIO_TRADES (full episode window)
    -- Replaced cursor c_daily with SELECT INTO
    select
        coalesce(sum(case when daily.DAILY_PNL > 0 then 1 else 0 end), 0),
        coalesce(sum(case when daily.DAILY_PNL < 0 then 1 else 0 end), 0),
        coalesce(max(daily.DRAWDOWN), 0)
      into :v_win_days, :v_loss_days, :v_max_dd_pct
      from (
        select TS, TOTAL_EQUITY, DRAWDOWN,
            (TOTAL_EQUITY - lag(TOTAL_EQUITY) over (order by TS)) as DAILY_PNL
        from MIP.APP.PORTFOLIO_DAILY
        where PORTFOLIO_ID = :v_portfolio_id and TS >= :v_start_ts and TS <= :v_ended_ts
      ) daily;

    -- Replaced cursor c_trades with SELECT INTO
    select count(*) into :v_trades_count
      from MIP.APP.PORTFOLIO_TRADES
     where PORTFOLIO_ID = :v_portfolio_id and TRADE_TS >= :v_start_ts and TRADE_TS <= :v_ended_ts;

    -- Emit event
    call MIP.APP.SP_LOG_EVENT(
        'PORTFOLIO_SIM',
        'PROFIT_TARGET_HIT',
        'INFO',
        null,
        object_construct(
            'portfolio_id', :v_portfolio_id,
            'episode_id', :v_episode_id,
            'run_id', :v_run_id,
            'final_equity', :v_final_equity,
            'start_equity', :v_start_equity,
            'return_pct', :v_return_pct,
            'profit_target_pct', :v_profit_target_pct,
            'crystallize_mode', :v_crystallize_mode
        ),
        null,
        :v_run_id,
        null
    );

    -- End episode
    update MIP.APP.PORTFOLIO_EPISODE
       set END_TS = :v_ended_ts, STATUS = 'ENDED', END_REASON = 'PROFIT_TARGET_HIT'
     where PORTFOLIO_ID = :v_portfolio_id and EPISODE_ID = :v_episode_id;

    -- Distribution amount
    if (v_crystallize_mode = 'WITHDRAW_PROFITS') then
        v_distribution_amt := greatest(0, v_final_equity - v_start_equity);
    else
        v_distribution_amt := 0;
    end if;

    -- Write episode results
    merge into MIP.APP.PORTFOLIO_EPISODE_RESULTS as target
    using (
        select
            :v_portfolio_id as PORTFOLIO_ID,
            :v_episode_id as EPISODE_ID,
            :v_start_equity as START_EQUITY,
            :v_final_equity as END_EQUITY,
            (:v_final_equity - :v_start_equity) as REALIZED_PNL,
            :v_return_pct as RETURN_PCT,
            :v_max_dd_pct as MAX_DRAWDOWN_PCT,
            :v_trades_count as TRADES_COUNT,
            :v_win_days as WIN_DAYS,
            :v_loss_days as LOSS_DAYS,
            :v_distribution_amt as DISTRIBUTION_AMOUNT,
            :v_crystallize_mode as DISTRIBUTION_MODE,
            'PROFIT_TARGET_HIT' as ENDED_REASON,
            :v_ended_ts as ENDED_AT_TS
    ) as source
    on target.PORTFOLIO_ID = source.PORTFOLIO_ID and target.EPISODE_ID = source.EPISODE_ID
    when matched then update set
        END_EQUITY = source.END_EQUITY,
        REALIZED_PNL = source.REALIZED_PNL,
        RETURN_PCT = source.RETURN_PCT,
        MAX_DRAWDOWN_PCT = source.MAX_DRAWDOWN_PCT,
        TRADES_COUNT = source.TRADES_COUNT,
        WIN_DAYS = source.WIN_DAYS,
        LOSS_DAYS = source.LOSS_DAYS,
        DISTRIBUTION_AMOUNT = source.DISTRIBUTION_AMOUNT,
        DISTRIBUTION_MODE = source.DISTRIBUTION_MODE,
        ENDED_REASON = source.ENDED_REASON,
        ENDED_AT_TS = source.ENDED_AT_TS,
        UPDATED_AT = current_timestamp()
    when not matched then insert (
        PORTFOLIO_ID, EPISODE_ID, START_EQUITY, END_EQUITY, REALIZED_PNL, RETURN_PCT,
        MAX_DRAWDOWN_PCT, TRADES_COUNT, WIN_DAYS, LOSS_DAYS,
        DISTRIBUTION_AMOUNT, DISTRIBUTION_MODE, ENDED_REASON, ENDED_AT_TS
    )
    values (
        source.PORTFOLIO_ID, source.EPISODE_ID, source.START_EQUITY, source.END_EQUITY, source.REALIZED_PNL, source.RETURN_PCT,
        source.MAX_DRAWDOWN_PCT, source.TRADES_COUNT, source.WIN_DAYS, source.LOSS_DAYS,
        source.DISTRIBUTION_AMOUNT, source.DISTRIBUTION_MODE, source.ENDED_REASON, source.ENDED_AT_TS
    );

    -- Next episode start equity
    if (v_crystallize_mode = 'REBASE') then
        v_next_start_equity := v_final_equity;
    else
        -- Replaced cursor c_baseline with SELECT INTO
        select STARTING_CASH into :v_baseline_cash
          from MIP.APP.PORTFOLIO
         where PORTFOLIO_ID = :v_portfolio_id;
        v_next_start_equity := v_baseline_cash;
    end if;

    -- Start next episode (same profile)
    call MIP.APP.SP_START_PORTFOLIO_EPISODE(:v_portfolio_id, :v_profile_id, 'PROFIT_TARGET_HIT', :v_next_start_equity);

    -- Reset PORTFOLIO header stats for the new episode.
    -- Without this, the old episode's win/loss days, max drawdown, total return
    -- persist until the next full simulation run, causing confusing UI display.
    update MIP.APP.PORTFOLIO
       set FINAL_EQUITY = :v_next_start_equity,
           TOTAL_RETURN = 0,
           MAX_DRAWDOWN = 0,
           WIN_DAYS = 0,
           LOSS_DAYS = 0,
           BUST_AT = null,
           UPDATED_AT = current_timestamp()
     where PORTFOLIO_ID = :v_portfolio_id;

    -- Cooldown: block new entries until COOLDOWN_DAYS have passed
    if (v_cooldown_days is not null and v_cooldown_days > 0) then
        update MIP.APP.PORTFOLIO
           set COOLDOWN_UNTIL_TS = dateadd(day, :v_cooldown_days, :v_ended_ts),
               UPDATED_AT = current_timestamp()
         where PORTFOLIO_ID = :v_portfolio_id;
    end if;

    -- ── Lifecycle event: record CRYSTALLIZE (non-fatal) ──────────────────────
    -- If PORTFOLIO_LIFECYCLE_EVENT table does not exist or insert fails,
    -- crystallization still succeeds — this is purely additive audit trail.
    begin
        let v_lc_cum_deposited number(18,2) := 0;
        let v_lc_cum_withdrawn number(18,2) := 0;
        let v_lc_cum_pnl number(18,2) := 0;
        begin
            -- Get latest running totals
            select CUMULATIVE_DEPOSITED, CUMULATIVE_WITHDRAWN
              into :v_lc_cum_deposited, :v_lc_cum_withdrawn
              from MIP.APP.PORTFOLIO_LIFECYCLE_EVENT
             where PORTFOLIO_ID = :v_portfolio_id
             order by EVENT_TS desc, EVENT_ID desc
             limit 1;
        exception when other then
            v_lc_cum_deposited := :v_start_equity;
            v_lc_cum_withdrawn := 0;
        end;

        -- Crystallize payout counts as a withdrawal
        if (:v_distribution_amt > 0) then
            v_lc_cum_withdrawn := :v_lc_cum_withdrawn + :v_distribution_amt;
        end if;
        v_lc_cum_pnl := :v_next_start_equity - (:v_lc_cum_deposited - :v_lc_cum_withdrawn);

        insert into MIP.APP.PORTFOLIO_LIFECYCLE_EVENT (
            PORTFOLIO_ID, EVENT_TS, EVENT_TYPE, AMOUNT,
            CASH_BEFORE, CASH_AFTER, EQUITY_BEFORE, EQUITY_AFTER,
            CUMULATIVE_DEPOSITED, CUMULATIVE_WITHDRAWN, CUMULATIVE_PNL,
            EPISODE_ID, PROFILE_ID, NOTES, CREATED_BY
        ) values (
            :v_portfolio_id, :v_ended_ts, 'CRYSTALLIZE', :v_distribution_amt,
            :v_final_equity, :v_next_start_equity, :v_final_equity, :v_next_start_equity,
            :v_lc_cum_deposited, :v_lc_cum_withdrawn, :v_lc_cum_pnl,
            :v_episode_id, :v_profile_id,
            'Profit target hit (' || to_varchar(:v_return_pct * 100, '999.99') || '%). '
                || :v_crystallize_mode || ' — distributed $' || to_varchar(:v_distribution_amt, '999,999,999.00'),
            current_user()
        );
    exception
        when other then null;  -- lifecycle logging is non-fatal
    end;

    return true;
end;
$$;

-- 220_portfolio_lifecycle_event.sql
-- Purpose: Portfolio lifecycle event ledger + safe CRUD stored procedures.
-- Every meaningful portfolio state change is recorded as an immutable event.
-- Stored procedures use EXECUTE AS OWNER so the UI API role only needs USAGE grants.
-- Safety: all write SPs check for active pipeline runs before allowing changes.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE: PORTFOLIO_LIFECYCLE_EVENT
-- ═══════════════════════════════════════════════════════════════════════════════

create table if not exists MIP.APP.PORTFOLIO_LIFECYCLE_EVENT (
    EVENT_ID              number          autoincrement,
    PORTFOLIO_ID          number          not null,
    EVENT_TS              timestamp_ntz   default current_timestamp(),
    EVENT_TYPE            varchar(32)     not null,
        -- CREATE | DEPOSIT | WITHDRAW | CRYSTALLIZE | PROFILE_CHANGE
        -- | EPISODE_START | EPISODE_END | BUST | CASH_ADJUST
    AMOUNT                number(18,2),               -- deposit/withdraw/crystallize amount
    CASH_BEFORE           number(18,2),
    CASH_AFTER            number(18,2),
    EQUITY_BEFORE         number(18,2),
    EQUITY_AFTER          number(18,2),
    CUMULATIVE_DEPOSITED  number(18,2)    default 0,  -- running total cash in
    CUMULATIVE_WITHDRAWN  number(18,2)    default 0,  -- running total cash out (incl. crystallize payouts)
    CUMULATIVE_PNL        number(18,2)    default 0,  -- equity - net_contributed
    EPISODE_ID            number,
    PROFILE_ID            number,
    NOTES                 varchar(1000),
    CREATED_BY            varchar(128)    default current_user(),
    constraint PK_PORTFOLIO_LIFECYCLE_EVENT primary key (EVENT_ID),
    constraint FK_LIFECYCLE_EVENT_PORTFOLIO foreign key (PORTFOLIO_ID)
        references MIP.APP.PORTFOLIO(PORTFOLIO_ID)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- HELPER: Check whether a pipeline is actively running (safety guard)
-- Returns TRUE if it is safe to proceed (no active pipeline).
-- ═══════════════════════════════════════════════════════════════════════════════

create or replace procedure MIP.APP.SP_CHECK_PIPELINE_SAFE_FOR_EDIT(
    P_PORTFOLIO_ID number
)
returns boolean
language sql
execute as owner
as
$$
declare
    v_active_count number := 0;
begin
    -- A pipeline is considered active if it has a START event in the last 2 hours
    -- that has not yet been followed by a SUCCESS/FAIL/SUCCESS_WITH_SKIPS event.
    select count(*)
      into :v_active_count
      from MIP.APP.MIP_AUDIT_LOG start_evt
     where start_evt.EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
       and start_evt.STATUS = 'START'
       and start_evt.EVENT_TS > dateadd(hour, -2, current_timestamp())
       and not exists (
           select 1
             from MIP.APP.MIP_AUDIT_LOG end_evt
            where end_evt.EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
              and end_evt.RUN_ID = start_evt.RUN_ID
              and end_evt.STATUS in ('SUCCESS', 'SUCCESS_WITH_SKIPS', 'FAIL')
       );

    return (:v_active_count = 0);
end;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- HELPER: Get the latest cumulative totals for a portfolio
-- ═══════════════════════════════════════════════════════════════════════════════

create or replace procedure MIP.APP.SP_GET_LIFECYCLE_RUNNING_TOTALS(
    P_PORTFOLIO_ID number
)
returns variant
language sql
execute as owner
as
$$
declare
    v_cum_deposited number(18,2) := 0;
    v_cum_withdrawn number(18,2) := 0;
    v_cum_pnl number(18,2) := 0;
begin
    select CUMULATIVE_DEPOSITED, CUMULATIVE_WITHDRAWN, CUMULATIVE_PNL
      into :v_cum_deposited, :v_cum_withdrawn, :v_cum_pnl
      from MIP.APP.PORTFOLIO_LIFECYCLE_EVENT
     where PORTFOLIO_ID = :P_PORTFOLIO_ID
     order by EVENT_TS desc, EVENT_ID desc
     limit 1;

    return object_construct(
        'cumulative_deposited', :v_cum_deposited,
        'cumulative_withdrawn', :v_cum_withdrawn,
        'cumulative_pnl', :v_cum_pnl
    );
exception
    when other then
        return object_construct(
            'cumulative_deposited', 0,
            'cumulative_withdrawn', 0,
            'cumulative_pnl', 0
        );
end;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- SP_UPSERT_PORTFOLIO: Create or update a portfolio (safe)
-- ═══════════════════════════════════════════════════════════════════════════════
-- When P_PORTFOLIO_ID IS NULL: creates a new portfolio + first episode + CREATE lifecycle event.
-- When P_PORTFOLIO_ID is given: updates allowed fields (name, notes, base_currency).
-- STARTING_CASH can only be set on creation; use SP_PORTFOLIO_CASH_EVENT for deposits/withdrawals.

create or replace procedure MIP.APP.SP_UPSERT_PORTFOLIO(
    P_PORTFOLIO_ID   number,
    P_NAME           varchar,
    P_BASE_CURRENCY  varchar,
    P_STARTING_CASH  number(18,2),
    P_PROFILE_ID     number,
    P_NOTES          varchar
)
returns variant
language sql
execute as owner
as
$$
declare
    v_portfolio_id number := :P_PORTFOLIO_ID;
    v_name varchar := :P_NAME;
    v_currency varchar := coalesce(:P_BASE_CURRENCY, 'USD');
    v_cash number(18,2) := :P_STARTING_CASH;
    v_profile_id number := :P_PROFILE_ID;
    v_notes varchar := :P_NOTES;
    v_is_safe boolean;
    v_episode_id number;
    v_existing_name varchar;
begin
    -- ── CREATE ──
    if (v_portfolio_id is null) then
        -- Validate required fields
        if (v_name is null or v_cash is null or v_cash <= 0) then
            return object_construct(
                'status', 'ERROR',
                'error', 'NAME and STARTING_CASH (> 0) are required for portfolio creation.'
            );
        end if;
        if (v_profile_id is null) then
            return object_construct(
                'status', 'ERROR',
                'error', 'PROFILE_ID is required for portfolio creation.'
            );
        end if;

        -- Insert portfolio
        insert into MIP.APP.PORTFOLIO (
            PROFILE_ID, NAME, BASE_CURRENCY, STARTING_CASH, FINAL_EQUITY,
            TOTAL_RETURN, MAX_DRAWDOWN, WIN_DAYS, LOSS_DAYS, STATUS, NOTES,
            CREATED_AT, UPDATED_AT
        ) values (
            :v_profile_id, :v_name, :v_currency, :v_cash, :v_cash,
            0, 0, 0, 0, 'ACTIVE', :v_notes,
            current_timestamp(), current_timestamp()
        );

        -- Retrieve auto-generated PORTFOLIO_ID
        select max(PORTFOLIO_ID)
          into :v_portfolio_id
          from MIP.APP.PORTFOLIO
         where NAME = :v_name;

        -- Start first episode
        call MIP.APP.SP_START_PORTFOLIO_EPISODE(:v_portfolio_id, :v_profile_id, 'PORTFOLIO_CREATED', :v_cash);

        select EPISODE_ID into :v_episode_id
          from MIP.APP.PORTFOLIO_EPISODE
         where PORTFOLIO_ID = :v_portfolio_id and STATUS = 'ACTIVE'
         limit 1;

        -- Record CREATE lifecycle event
        insert into MIP.APP.PORTFOLIO_LIFECYCLE_EVENT (
            PORTFOLIO_ID, EVENT_TS, EVENT_TYPE, AMOUNT,
            CASH_BEFORE, CASH_AFTER, EQUITY_BEFORE, EQUITY_AFTER,
            CUMULATIVE_DEPOSITED, CUMULATIVE_WITHDRAWN, CUMULATIVE_PNL,
            EPISODE_ID, PROFILE_ID, NOTES, CREATED_BY
        ) values (
            :v_portfolio_id, current_timestamp(), 'CREATE', :v_cash,
            0, :v_cash, 0, :v_cash,
            :v_cash, 0, 0,
            :v_episode_id, :v_profile_id, 'Portfolio created with starting cash ' || :v_cash, current_user()
        );

        return object_construct(
            'status', 'SUCCESS',
            'action', 'CREATED',
            'portfolio_id', :v_portfolio_id,
            'episode_id', :v_episode_id
        );

    -- ── UPDATE ──
    else
        -- Safety check: no active pipeline
        call MIP.APP.SP_CHECK_PIPELINE_SAFE_FOR_EDIT(:v_portfolio_id);
        v_is_safe := (select :v_portfolio_id is not null);  -- result from the call
        -- Re-check via direct call
        v_is_safe := (call MIP.APP.SP_CHECK_PIPELINE_SAFE_FOR_EDIT(:v_portfolio_id));
        if (not v_is_safe) then
            return object_construct(
                'status', 'ERROR',
                'error', 'A pipeline is currently running. Please try again shortly.'
            );
        end if;

        -- Validate portfolio exists
        select NAME into :v_existing_name
          from MIP.APP.PORTFOLIO where PORTFOLIO_ID = :v_portfolio_id;
        if (v_existing_name is null) then
            return object_construct(
                'status', 'ERROR',
                'error', 'Portfolio not found.'
            );
        end if;

        -- Update allowed fields (never touch STARTING_CASH here)
        update MIP.APP.PORTFOLIO
           set NAME = coalesce(:v_name, NAME),
               BASE_CURRENCY = coalesce(:v_currency, BASE_CURRENCY),
               NOTES = coalesce(:v_notes, NOTES),
               UPDATED_AT = current_timestamp()
         where PORTFOLIO_ID = :v_portfolio_id;

        return object_construct(
            'status', 'SUCCESS',
            'action', 'UPDATED',
            'portfolio_id', :v_portfolio_id
        );
    end if;
end;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- SP_PORTFOLIO_CASH_EVENT: Register a DEPOSIT or WITHDRAW
-- ═══════════════════════════════════════════════════════════════════════════════
-- Updates cash in the portfolio and records a lifecycle event.
-- The overall lifetime P&L tracking stays intact because we adjust the
-- cumulative_deposited / cumulative_withdrawn running totals.

create or replace procedure MIP.APP.SP_PORTFOLIO_CASH_EVENT(
    P_PORTFOLIO_ID   number,
    P_EVENT_TYPE     varchar,
    P_AMOUNT         number(18,2),
    P_NOTES          varchar
)
returns variant
language sql
execute as owner
as
$$
declare
    v_portfolio_id number := :P_PORTFOLIO_ID;
    v_event_type varchar := upper(:P_EVENT_TYPE);
    v_amount number(18,2) := :P_AMOUNT;
    v_notes varchar := :P_NOTES;
    v_is_safe boolean;
    v_current_cash number(18,2);
    v_current_equity number(18,2);
    v_new_cash number(18,2);
    v_starting_cash number(18,2);
    v_episode_id number;
    v_profile_id number;
    v_cum_deposited number(18,2) := 0;
    v_cum_withdrawn number(18,2) := 0;
    v_cum_pnl number(18,2) := 0;
    v_prev_totals variant;
    v_status varchar;
begin
    -- Validate event type
    if (v_event_type not in ('DEPOSIT', 'WITHDRAW')) then
        return object_construct('status', 'ERROR', 'error', 'EVENT_TYPE must be DEPOSIT or WITHDRAW.');
    end if;

    -- Validate amount
    if (v_amount is null or v_amount <= 0) then
        return object_construct('status', 'ERROR', 'error', 'AMOUNT must be greater than 0.');
    end if;

    -- Safety check: no active pipeline
    v_is_safe := (call MIP.APP.SP_CHECK_PIPELINE_SAFE_FOR_EDIT(:v_portfolio_id));
    if (not v_is_safe) then
        return object_construct('status', 'ERROR', 'error', 'A pipeline is currently running. Please try again shortly.');
    end if;

    -- Load current portfolio state
    select STARTING_CASH, coalesce(FINAL_EQUITY, STARTING_CASH), STATUS, PROFILE_ID
      into :v_starting_cash, :v_current_equity, :v_status, :v_profile_id
      from MIP.APP.PORTFOLIO
     where PORTFOLIO_ID = :v_portfolio_id;

    if (v_starting_cash is null) then
        return object_construct('status', 'ERROR', 'error', 'Portfolio not found.');
    end if;
    if (v_status != 'ACTIVE') then
        return object_construct('status', 'ERROR', 'error', 'Portfolio is not ACTIVE (status: ' || v_status || ').');
    end if;

    -- Get active episode
    select EPISODE_ID into :v_episode_id
      from MIP.APP.PORTFOLIO_EPISODE
     where PORTFOLIO_ID = :v_portfolio_id and STATUS = 'ACTIVE'
     limit 1;

    -- Compute current cash from latest daily or starting cash
    begin
        select CASH into :v_current_cash
          from MIP.APP.PORTFOLIO_DAILY
         where PORTFOLIO_ID = :v_portfolio_id
         order by TS desc
         limit 1;
    exception
        when other then v_current_cash := :v_starting_cash;
    end;

    -- Validate sufficient cash for withdrawal
    if (v_event_type = 'WITHDRAW' and v_amount > v_current_cash) then
        return object_construct('status', 'ERROR', 'error',
            'Insufficient cash. Available: ' || v_current_cash || ', requested: ' || v_amount);
    end if;

    -- Compute new cash
    if (v_event_type = 'DEPOSIT') then
        v_new_cash := v_current_cash + v_amount;
    else
        v_new_cash := v_current_cash - v_amount;
    end if;

    -- Get previous running totals
    v_prev_totals := (call MIP.APP.SP_GET_LIFECYCLE_RUNNING_TOTALS(:v_portfolio_id));
    v_cum_deposited := coalesce(:v_prev_totals:cumulative_deposited::number, 0);
    v_cum_withdrawn := coalesce(:v_prev_totals:cumulative_withdrawn::number, 0);

    -- Update running totals
    if (v_event_type = 'DEPOSIT') then
        v_cum_deposited := v_cum_deposited + v_amount;
    else
        v_cum_withdrawn := v_cum_withdrawn + v_amount;
    end if;

    -- Compute lifetime P&L = current equity (after cash change) - net contributed
    -- For a deposit: equity goes up by the deposit, but so does net contributed, so PnL unchanged
    -- For a withdraw: equity goes down by the withdraw, but net contributed also goes down, so PnL unchanged
    v_cum_pnl := (v_current_equity + (case when v_event_type = 'DEPOSIT' then v_amount else -v_amount end))
                 - (v_cum_deposited - v_cum_withdrawn);

    -- Update portfolio STARTING_CASH (the "cost basis" for the portfolio adjusts with cash events)
    update MIP.APP.PORTFOLIO
       set STARTING_CASH = :v_new_cash,
           FINAL_EQUITY = coalesce(FINAL_EQUITY, :v_starting_cash) + (case when :v_event_type = 'DEPOSIT' then :v_amount else -:v_amount end),
           UPDATED_AT = current_timestamp()
     where PORTFOLIO_ID = :v_portfolio_id;

    -- Record lifecycle event
    insert into MIP.APP.PORTFOLIO_LIFECYCLE_EVENT (
        PORTFOLIO_ID, EVENT_TS, EVENT_TYPE, AMOUNT,
        CASH_BEFORE, CASH_AFTER, EQUITY_BEFORE, EQUITY_AFTER,
        CUMULATIVE_DEPOSITED, CUMULATIVE_WITHDRAWN, CUMULATIVE_PNL,
        EPISODE_ID, PROFILE_ID, NOTES, CREATED_BY
    ) values (
        :v_portfolio_id, current_timestamp(), :v_event_type, :v_amount,
        :v_current_cash, :v_new_cash,
        :v_current_equity, :v_current_equity + (case when :v_event_type = 'DEPOSIT' then :v_amount else -:v_amount end),
        :v_cum_deposited, :v_cum_withdrawn, :v_cum_pnl,
        :v_episode_id, :v_profile_id, :v_notes, current_user()
    );

    return object_construct(
        'status', 'SUCCESS',
        'event_type', :v_event_type,
        'amount', :v_amount,
        'cash_before', :v_current_cash,
        'cash_after', :v_new_cash,
        'cumulative_deposited', :v_cum_deposited,
        'cumulative_withdrawn', :v_cum_withdrawn,
        'cumulative_pnl', :v_cum_pnl
    );
end;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- SP_ATTACH_PROFILE: Change the profile on a portfolio (safe)
-- ═══════════════════════════════════════════════════════════════════════════════
-- Ends current episode, starts a new one with the new profile.
-- Records a PROFILE_CHANGE lifecycle event.

create or replace procedure MIP.APP.SP_ATTACH_PROFILE(
    P_PORTFOLIO_ID   number,
    P_PROFILE_ID     number
)
returns variant
language sql
execute as owner
as
$$
declare
    v_portfolio_id number := :P_PORTFOLIO_ID;
    v_new_profile_id number := :P_PROFILE_ID;
    v_is_safe boolean;
    v_current_profile_id number;
    v_current_equity number(18,2);
    v_current_cash number(18,2);
    v_starting_cash number(18,2);
    v_old_profile_name varchar;
    v_new_profile_name varchar;
    v_old_episode_id number;
    v_new_episode_id number;
    v_status varchar;
    v_prev_totals variant;
    v_cum_deposited number(18,2);
    v_cum_withdrawn number(18,2);
    v_cum_pnl number(18,2);
begin
    -- Safety check
    v_is_safe := (call MIP.APP.SP_CHECK_PIPELINE_SAFE_FOR_EDIT(:v_portfolio_id));
    if (not v_is_safe) then
        return object_construct('status', 'ERROR', 'error', 'A pipeline is currently running. Please try again shortly.');
    end if;

    -- Load portfolio
    select PROFILE_ID, coalesce(FINAL_EQUITY, STARTING_CASH), STARTING_CASH, STATUS
      into :v_current_profile_id, :v_current_equity, :v_starting_cash, :v_status
      from MIP.APP.PORTFOLIO
     where PORTFOLIO_ID = :v_portfolio_id;

    if (v_current_profile_id is null) then
        return object_construct('status', 'ERROR', 'error', 'Portfolio not found.');
    end if;
    if (v_status != 'ACTIVE') then
        return object_construct('status', 'ERROR', 'error', 'Portfolio is not ACTIVE.');
    end if;

    -- Validate new profile exists
    select NAME into :v_new_profile_name
      from MIP.APP.PORTFOLIO_PROFILE where PROFILE_ID = :v_new_profile_id;
    if (v_new_profile_name is null) then
        return object_construct('status', 'ERROR', 'error', 'Profile not found.');
    end if;

    -- If same profile, no-op
    if (v_current_profile_id = v_new_profile_id) then
        return object_construct('status', 'SUCCESS', 'action', 'NO_CHANGE', 'message', 'Profile is already attached.');
    end if;

    -- Get old profile name
    select NAME into :v_old_profile_name
      from MIP.APP.PORTFOLIO_PROFILE where PROFILE_ID = :v_current_profile_id;

    -- Get current episode
    select EPISODE_ID into :v_old_episode_id
      from MIP.APP.PORTFOLIO_EPISODE
     where PORTFOLIO_ID = :v_portfolio_id and STATUS = 'ACTIVE'
     limit 1;

    -- Get current cash
    begin
        select CASH into :v_current_cash
          from MIP.APP.PORTFOLIO_DAILY
         where PORTFOLIO_ID = :v_portfolio_id order by TS desc limit 1;
    exception when other then v_current_cash := :v_starting_cash;
    end;

    -- End current episode, start new one with the new profile
    v_new_episode_id := (call MIP.APP.SP_START_PORTFOLIO_EPISODE(
        :v_portfolio_id, :v_new_profile_id, 'PROFILE_CHANGE', :v_current_equity
    ));

    -- Get running totals
    v_prev_totals := (call MIP.APP.SP_GET_LIFECYCLE_RUNNING_TOTALS(:v_portfolio_id));
    v_cum_deposited := coalesce(:v_prev_totals:cumulative_deposited::number, 0);
    v_cum_withdrawn := coalesce(:v_prev_totals:cumulative_withdrawn::number, 0);
    v_cum_pnl := v_current_equity - (v_cum_deposited - v_cum_withdrawn);

    -- Record PROFILE_CHANGE lifecycle event
    insert into MIP.APP.PORTFOLIO_LIFECYCLE_EVENT (
        PORTFOLIO_ID, EVENT_TS, EVENT_TYPE, AMOUNT,
        CASH_BEFORE, CASH_AFTER, EQUITY_BEFORE, EQUITY_AFTER,
        CUMULATIVE_DEPOSITED, CUMULATIVE_WITHDRAWN, CUMULATIVE_PNL,
        EPISODE_ID, PROFILE_ID, NOTES, CREATED_BY
    ) values (
        :v_portfolio_id, current_timestamp(), 'PROFILE_CHANGE', null,
        :v_current_cash, :v_current_cash, :v_current_equity, :v_current_equity,
        :v_cum_deposited, :v_cum_withdrawn, :v_cum_pnl,
        :v_new_episode_id, :v_new_profile_id,
        'Profile changed from ' || coalesce(:v_old_profile_name, 'unknown') || ' to ' || :v_new_profile_name,
        current_user()
    );

    return object_construct(
        'status', 'SUCCESS',
        'action', 'PROFILE_CHANGED',
        'old_profile', :v_old_profile_name,
        'new_profile', :v_new_profile_name,
        'old_episode_id', :v_old_episode_id,
        'new_episode_id', :v_new_episode_id
    );
end;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- SP_UPSERT_PORTFOLIO_PROFILE: Create or update a profile
-- ═══════════════════════════════════════════════════════════════════════════════
-- Safe: profile changes only take effect on the next simulation run.

create or replace procedure MIP.APP.SP_UPSERT_PORTFOLIO_PROFILE(
    P_PROFILE_ID            number,
    P_NAME                  varchar,
    P_MAX_POSITIONS         number,
    P_MAX_POSITION_PCT      number(18,6),
    P_BUST_EQUITY_PCT       number(18,6),
    P_BUST_ACTION           varchar,
    P_DRAWDOWN_STOP_PCT     number(18,6),
    P_CRYSTALLIZE_ENABLED   boolean,
    P_PROFIT_TARGET_PCT     number(18,6),
    P_CRYSTALLIZE_MODE      varchar,
    P_COOLDOWN_DAYS         number,
    P_MAX_EPISODE_DAYS      number,
    P_TAKE_PROFIT_ON        varchar,
    P_DESCRIPTION           varchar
)
returns variant
language sql
execute as owner
as
$$
declare
    v_profile_id number := :P_PROFILE_ID;
    v_name varchar := :P_NAME;
    v_existing_name varchar;
begin
    -- ── CREATE ──
    if (v_profile_id is null) then
        if (v_name is null) then
            return object_construct('status', 'ERROR', 'error', 'NAME is required for profile creation.');
        end if;

        -- Check uniqueness
        select NAME into :v_existing_name
          from MIP.APP.PORTFOLIO_PROFILE where NAME = :v_name;
        if (v_existing_name is not null) then
            return object_construct('status', 'ERROR', 'error', 'A profile with name "' || :v_name || '" already exists.');
        end if;

        insert into MIP.APP.PORTFOLIO_PROFILE (
            NAME, MAX_POSITIONS, MAX_POSITION_PCT, BUST_EQUITY_PCT, BUST_ACTION,
            DRAWDOWN_STOP_PCT, CRYSTALLIZE_ENABLED, PROFIT_TARGET_PCT,
            CRYSTALLIZE_MODE, COOLDOWN_DAYS, MAX_EPISODE_DAYS, TAKE_PROFIT_ON,
            DESCRIPTION, CREATED_AT
        ) values (
            :v_name,
            :P_MAX_POSITIONS,
            :P_MAX_POSITION_PCT,
            :P_BUST_EQUITY_PCT,
            coalesce(:P_BUST_ACTION, 'ALLOW_EXITS_ONLY'),
            :P_DRAWDOWN_STOP_PCT,
            coalesce(:P_CRYSTALLIZE_ENABLED, false),
            :P_PROFIT_TARGET_PCT,
            :P_CRYSTALLIZE_MODE,
            :P_COOLDOWN_DAYS,
            :P_MAX_EPISODE_DAYS,
            coalesce(:P_TAKE_PROFIT_ON, 'EOD'),
            :P_DESCRIPTION,
            current_timestamp()
        );

        select max(PROFILE_ID) into :v_profile_id
          from MIP.APP.PORTFOLIO_PROFILE where NAME = :v_name;

        return object_construct(
            'status', 'SUCCESS',
            'action', 'CREATED',
            'profile_id', :v_profile_id
        );

    -- ── UPDATE ──
    else
        select NAME into :v_existing_name
          from MIP.APP.PORTFOLIO_PROFILE where PROFILE_ID = :v_profile_id;
        if (v_existing_name is null) then
            return object_construct('status', 'ERROR', 'error', 'Profile not found.');
        end if;

        update MIP.APP.PORTFOLIO_PROFILE
           set NAME                 = coalesce(:P_NAME, NAME),
               MAX_POSITIONS        = coalesce(:P_MAX_POSITIONS, MAX_POSITIONS),
               MAX_POSITION_PCT     = coalesce(:P_MAX_POSITION_PCT, MAX_POSITION_PCT),
               BUST_EQUITY_PCT      = coalesce(:P_BUST_EQUITY_PCT, BUST_EQUITY_PCT),
               BUST_ACTION          = coalesce(:P_BUST_ACTION, BUST_ACTION),
               DRAWDOWN_STOP_PCT    = coalesce(:P_DRAWDOWN_STOP_PCT, DRAWDOWN_STOP_PCT),
               CRYSTALLIZE_ENABLED  = coalesce(:P_CRYSTALLIZE_ENABLED, CRYSTALLIZE_ENABLED),
               PROFIT_TARGET_PCT    = coalesce(:P_PROFIT_TARGET_PCT, PROFIT_TARGET_PCT),
               CRYSTALLIZE_MODE     = coalesce(:P_CRYSTALLIZE_MODE, CRYSTALLIZE_MODE),
               COOLDOWN_DAYS        = coalesce(:P_COOLDOWN_DAYS, COOLDOWN_DAYS),
               MAX_EPISODE_DAYS     = coalesce(:P_MAX_EPISODE_DAYS, MAX_EPISODE_DAYS),
               TAKE_PROFIT_ON       = coalesce(:P_TAKE_PROFIT_ON, TAKE_PROFIT_ON),
               DESCRIPTION          = coalesce(:P_DESCRIPTION, DESCRIPTION)
         where PROFILE_ID = :v_profile_id;

        return object_construct(
            'status', 'SUCCESS',
            'action', 'UPDATED',
            'profile_id', :v_profile_id
        );
    end if;
end;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- BACKFILL: Seed CREATE events for existing portfolios that predate this table.
-- Idempotent: only inserts for portfolios with no lifecycle events yet.
-- ═══════════════════════════════════════════════════════════════════════════════

merge into MIP.APP.PORTFOLIO_LIFECYCLE_EVENT as target
using (
    select
        p.PORTFOLIO_ID,
        p.CREATED_AT                                as EVENT_TS,
        'CREATE'                                    as EVENT_TYPE,
        p.STARTING_CASH                             as AMOUNT,
        0                                           as CASH_BEFORE,
        p.STARTING_CASH                             as CASH_AFTER,
        0                                           as EQUITY_BEFORE,
        p.STARTING_CASH                             as EQUITY_AFTER,
        p.STARTING_CASH                             as CUMULATIVE_DEPOSITED,
        0                                           as CUMULATIVE_WITHDRAWN,
        coalesce(p.FINAL_EQUITY, p.STARTING_CASH) - p.STARTING_CASH as CUMULATIVE_PNL,
        ae.EPISODE_ID                               as EPISODE_ID,
        p.PROFILE_ID                                as PROFILE_ID,
        'Backfilled: portfolio existed before lifecycle tracking was introduced' as NOTES
    from MIP.APP.PORTFOLIO p
    left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE ae
      on ae.PORTFOLIO_ID = p.PORTFOLIO_ID
    where p.PORTFOLIO_ID not in (
        select distinct PORTFOLIO_ID from MIP.APP.PORTFOLIO_LIFECYCLE_EVENT
    )
) as source
on target.PORTFOLIO_ID = source.PORTFOLIO_ID
   and target.EVENT_TYPE = 'CREATE'
when not matched then insert (
    PORTFOLIO_ID, EVENT_TS, EVENT_TYPE, AMOUNT,
    CASH_BEFORE, CASH_AFTER, EQUITY_BEFORE, EQUITY_AFTER,
    CUMULATIVE_DEPOSITED, CUMULATIVE_WITHDRAWN, CUMULATIVE_PNL,
    EPISODE_ID, PROFILE_ID, NOTES, CREATED_BY
) values (
    source.PORTFOLIO_ID, source.EVENT_TS, source.EVENT_TYPE, source.AMOUNT,
    source.CASH_BEFORE, source.CASH_AFTER, source.EQUITY_BEFORE, source.EQUITY_AFTER,
    source.CUMULATIVE_DEPOSITED, source.CUMULATIVE_WITHDRAWN, source.CUMULATIVE_PNL,
    source.EPISODE_ID, source.PROFILE_ID, source.NOTES, current_user()
);

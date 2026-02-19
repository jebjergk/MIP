-- 169_portfolio_profile_crystallize_and_episode_results.sql
-- Purpose: Profile-driven crystallization exit strategy + persist episode results.
-- Part 1: Extend PORTFOLIO_PROFILE. Part 2: PORTFOLIO_EPISODE_RESULTS + view.

use role MIP_ADMIN_ROLE;
use database MIP;

-----------------------------
-- Part 1: PORTFOLIO_PROFILE exit strategy columns
-----------------------------
alter table MIP.APP.PORTFOLIO_PROFILE
    add column if not exists CRYSTALLIZE_ENABLED boolean default false;
alter table MIP.APP.PORTFOLIO_PROFILE
    add column if not exists PROFIT_TARGET_PCT number(18,6);
alter table MIP.APP.PORTFOLIO_PROFILE
    add column if not exists CRYSTALLIZE_MODE varchar(32);
alter table MIP.APP.PORTFOLIO_PROFILE
    add column if not exists COOLDOWN_DAYS number;
alter table MIP.APP.PORTFOLIO_PROFILE
    add column if not exists MAX_EPISODE_DAYS number;
alter table MIP.APP.PORTFOLIO_PROFILE
    add column if not exists TAKE_PROFIT_ON varchar(32) default 'EOD';

-----------------------------
-- PORTFOLIO_EPISODE.START_EQUITY (for episode return calculation)
-----------------------------
alter table MIP.APP.PORTFOLIO_EPISODE
    add column if not exists START_EQUITY number(18,2);

-----------------------------
-- Part 2: PORTFOLIO_EPISODE_RESULTS
-----------------------------
create table if not exists MIP.APP.PORTFOLIO_EPISODE_RESULTS (
    PORTFOLIO_ID         number          not null,
    EPISODE_ID           number          not null,
    START_EQUITY         number(18,2)    not null,
    END_EQUITY           number(18,2)    not null,
    REALIZED_PNL         number(18,2),
    RETURN_PCT           number(18,6),
    MAX_DRAWDOWN_PCT     number(18,6),
    TRADES_COUNT         number,
    WIN_DAYS             number,
    LOSS_DAYS            number,
    DISTRIBUTION_AMOUNT  number(18,2),
    DISTRIBUTION_MODE    varchar(32),
    ENDED_REASON         varchar(64),
    ENDED_AT_TS          timestamp_ntz,
    UPDATED_AT           timestamp_ntz  default current_timestamp(),
    constraint PK_PORTFOLIO_EPISODE_RESULTS primary key (PORTFOLIO_ID, EPISODE_ID),
    constraint FK_EPISODE_RESULTS_EPISODE foreign key (EPISODE_ID)
        references MIP.APP.PORTFOLIO_EPISODE(EPISODE_ID)
);

-----------------------------
-- View: EPISODE + RESULTS for UI
-----------------------------
create or replace view MIP.MART.V_PORTFOLIO_EPISODE_RESULTS as
select
    e.PORTFOLIO_ID,
    e.EPISODE_ID,
    e.PROFILE_ID,
    e.START_TS,
    e.END_TS,
    e.STATUS,
    e.END_REASON,
    e.START_EQUITY,
    r.END_EQUITY,
    r.REALIZED_PNL,
    r.RETURN_PCT,
    r.MAX_DRAWDOWN_PCT,
    r.TRADES_COUNT,
    r.WIN_DAYS,
    r.LOSS_DAYS,
    r.DISTRIBUTION_AMOUNT,
    r.DISTRIBUTION_MODE,
    r.ENDED_AT_TS
from MIP.APP.PORTFOLIO_EPISODE e
left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
  on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID;

-----------------------------
-- SP_START_PORTFOLIO_EPISODE (replace with version that sets START_EQUITY)
-----------------------------
create or replace procedure MIP.APP.SP_START_PORTFOLIO_EPISODE(
    P_PORTFOLIO_ID   number,
    P_PROFILE_ID     number,
    P_END_REASON     varchar default 'MANUAL_RESET',
    P_START_EQUITY   number default null
)
returns number
language sql
execute as caller
as
$$
declare
    v_end_ts timestamp_ntz := current_timestamp();
    v_new_episode_id number;
    v_start_equity number(18,2) := :P_START_EQUITY;
    v_old_episode_id number;
    v_old_equity number(18,2);
    v_old_cash number(18,2);
begin
    if (v_start_equity is null) then
        select STARTING_CASH into :v_start_equity
          from MIP.APP.PORTFOLIO where PORTFOLIO_ID = :P_PORTFOLIO_ID;
    end if;

    -- Capture old episode state before ending it
    begin
        select EPISODE_ID into :v_old_episode_id
          from MIP.APP.PORTFOLIO_EPISODE
         where PORTFOLIO_ID = :P_PORTFOLIO_ID and STATUS = 'ACTIVE';
    exception when other then
        v_old_episode_id := null;
    end;
    begin
        select TOTAL_EQUITY, CASH into :v_old_equity, :v_old_cash
          from MIP.APP.PORTFOLIO_DAILY
         where PORTFOLIO_ID = :P_PORTFOLIO_ID
         order by TS desc
         limit 1;
    exception when other then
        v_old_equity := :v_start_equity;
        v_old_cash := :v_start_equity;
    end;

    update MIP.APP.PORTFOLIO_EPISODE
       set END_TS = :v_end_ts, STATUS = 'ENDED', END_REASON = coalesce(:P_END_REASON, 'MANUAL_RESET')
     where PORTFOLIO_ID = :P_PORTFOLIO_ID and STATUS = 'ACTIVE';

    insert into MIP.APP.PORTFOLIO_EPISODE (
        PORTFOLIO_ID, PROFILE_ID, START_TS, END_TS, STATUS, END_REASON, START_EQUITY
    )
    values (
        :P_PORTFOLIO_ID, :P_PROFILE_ID, :v_end_ts, null, 'ACTIVE', null, :v_start_equity
    );

    select EPISODE_ID into :v_new_episode_id
      from MIP.APP.PORTFOLIO_EPISODE
     where PORTFOLIO_ID = :P_PORTFOLIO_ID and STATUS = 'ACTIVE' and START_TS = :v_end_ts;

    update MIP.APP.PORTFOLIO
       set PROFILE_ID = :P_PROFILE_ID, UPDATED_AT = :v_end_ts
     where PORTFOLIO_ID = :P_PORTFOLIO_ID;

    -- Lifecycle events: EPISODE_END + EPISODE_START (non-fatal)
    begin
        let v_lc_dep number(18,2) := 0;
        let v_lc_wth number(18,2) := 0;
        let v_lc_pnl number(18,2) := 0;
        begin
            select CUMULATIVE_DEPOSITED, CUMULATIVE_WITHDRAWN, CUMULATIVE_PNL
              into :v_lc_dep, :v_lc_wth, :v_lc_pnl
              from MIP.APP.PORTFOLIO_LIFECYCLE_EVENT
             where PORTFOLIO_ID = :P_PORTFOLIO_ID
             order by EVENT_TS desc, EVENT_ID desc
             limit 1;
        exception when other then
            v_lc_dep := :v_start_equity;
            v_lc_wth := 0;
            v_lc_pnl := 0;
        end;

        if (:v_old_episode_id is not null) then
            let v_ep_pnl number(18,2) := coalesce(:v_old_equity, :v_start_equity) - :v_start_equity;
            v_lc_pnl := :v_lc_pnl + :v_ep_pnl;
            insert into MIP.APP.PORTFOLIO_LIFECYCLE_EVENT (
                PORTFOLIO_ID, EVENT_TS, EVENT_TYPE, AMOUNT,
                CASH_BEFORE, CASH_AFTER, EQUITY_BEFORE, EQUITY_AFTER,
                CUMULATIVE_DEPOSITED, CUMULATIVE_WITHDRAWN, CUMULATIVE_PNL,
                EPISODE_ID, PROFILE_ID, NOTES, CREATED_BY
            ) values (
                :P_PORTFOLIO_ID, :v_end_ts, 'EPISODE_END',
                coalesce(:v_old_equity, 0) - :v_start_equity,
                :v_old_cash, :v_old_cash,
                :v_old_equity, :v_old_equity,
                :v_lc_dep, :v_lc_wth, :v_lc_pnl,
                :v_old_episode_id, :P_PROFILE_ID,
                'Episode ended: ' || coalesce(:P_END_REASON, 'MANUAL_RESET'),
                current_user()
            );
        end if;

        insert into MIP.APP.PORTFOLIO_LIFECYCLE_EVENT (
            PORTFOLIO_ID, EVENT_TS, EVENT_TYPE, AMOUNT,
            CASH_BEFORE, CASH_AFTER, EQUITY_BEFORE, EQUITY_AFTER,
            CUMULATIVE_DEPOSITED, CUMULATIVE_WITHDRAWN, CUMULATIVE_PNL,
            EPISODE_ID, PROFILE_ID, NOTES, CREATED_BY
        ) values (
            :P_PORTFOLIO_ID, :v_end_ts, 'EPISODE_START', :v_start_equity,
            :v_start_equity, :v_start_equity,
            :v_start_equity, :v_start_equity,
            :v_lc_dep, :v_lc_wth, :v_lc_pnl,
            :v_new_episode_id, :P_PROFILE_ID,
            'New episode started',
            current_user()
        );
    exception when other then
        null;
    end;

    return :v_new_episode_id;
end;
$$;

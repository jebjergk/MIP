-- 168_portfolio_episode.sql
-- Purpose: Portfolio episodes (profile generations) for evolution timeline and episode-scoped KPIs.
-- One long-lived PORTFOLIO; each reset starts a new EPISODE with auditable start/end.
-- UX KPIs/risk are computed since active episode START_TS.

use role MIP_ADMIN_ROLE;
use database MIP;

-----------------------------
-- PORTFOLIO_EPISODE
-----------------------------
create table if not exists MIP.APP.PORTFOLIO_EPISODE (
    PORTFOLIO_ID   number          not null,
    EPISODE_ID     number          autoincrement start 1 increment 1,
    PROFILE_ID     number          not null,
    START_TS       timestamp_ntz    not null,
    END_TS         timestamp_ntz    null,
    STATUS         varchar(32)     not null,   -- 'ACTIVE' | 'ENDED'
    END_REASON     varchar(64)     null,       -- 'MANUAL_RESET' | 'DRAWDOWN_STOP' | 'BUST' | etc.
    CREATED_AT     timestamp_ntz   default current_timestamp(),
    constraint PK_PORTFOLIO_EPISODE primary key (EPISODE_ID),
    constraint FK_PORTFOLIO_EPISODE_PORTFOLIO foreign key (PORTFOLIO_ID)
        references MIP.APP.PORTFOLIO(PORTFOLIO_ID),
    constraint FK_PORTFOLIO_EPISODE_PROFILE foreign key (PROFILE_ID)
        references MIP.APP.PORTFOLIO_PROFILE(PROFILE_ID)
);

-- One ACTIVE episode per portfolio enforced in SP_START_PORTFOLIO_EPISODE (Snowflake no partial unique).

-----------------------------
-- V_PORTFOLIO_ACTIVE_EPISODE
-----------------------------
create or replace view MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE as
select
    PORTFOLIO_ID,
    EPISODE_ID,
    PROFILE_ID,
    START_TS
from MIP.APP.PORTFOLIO_EPISODE
where STATUS = 'ACTIVE';

-----------------------------
-- SP_START_PORTFOLIO_EPISODE
-----------------------------
create or replace procedure MIP.APP.SP_START_PORTFOLIO_EPISODE(
    P_PORTFOLIO_ID number,
    P_PROFILE_ID    number,
    P_END_REASON    varchar default 'MANUAL_RESET'
)
returns number
language sql
execute as caller
as
$$
declare
    v_end_ts timestamp_ntz := current_timestamp();
    v_new_episode_id number;
begin
    -- Close existing ACTIVE episode for this portfolio
    update MIP.APP.PORTFOLIO_EPISODE
       set END_TS     = :v_end_ts,
           STATUS     = 'ENDED',
           END_REASON = coalesce(:P_END_REASON, 'MANUAL_RESET')
     where PORTFOLIO_ID = :P_PORTFOLIO_ID
       and STATUS = 'ACTIVE';

    -- Insert new episode
    insert into MIP.APP.PORTFOLIO_EPISODE (
        PORTFOLIO_ID,
        PROFILE_ID,
        START_TS,
        END_TS,
        STATUS,
        END_REASON
    )
    values (
        :P_PORTFOLIO_ID,
        :P_PROFILE_ID,
        :v_end_ts,
        null,
        'ACTIVE',
        null
    );

    select EPISODE_ID
      into :v_new_episode_id
      from MIP.APP.PORTFOLIO_EPISODE
     where PORTFOLIO_ID = :P_PORTFOLIO_ID
       and STATUS = 'ACTIVE'
       and START_TS = :v_end_ts;

    -- Point portfolio to this profile (PORTFOLIO has PROFILE_ID)
    update MIP.APP.PORTFOLIO
       set PROFILE_ID = :P_PROFILE_ID,
           UPDATED_AT = :v_end_ts
     where PORTFOLIO_ID = :P_PORTFOLIO_ID;

    return :v_new_episode_id;
end;
$$;

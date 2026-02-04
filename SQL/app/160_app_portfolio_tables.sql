-- 160_app_portfolio_tables.sql
-- Purpose: Portfolio simulation tables and supporting view

use role MIP_ADMIN_ROLE;
use database MIP;

-----------------------------
-- 0. PORTFOLIO_PROFILE
-----------------------------
create table if not exists MIP.APP.PORTFOLIO_PROFILE (
    PROFILE_ID          number        autoincrement,
    NAME                string        not null,
    MAX_POSITIONS       number,
    MAX_POSITION_PCT    number(18,6),
    BUST_EQUITY_PCT     number(18,6),
    BUST_ACTION         string        not null default 'ALLOW_EXITS_ONLY',
    DRAWDOWN_STOP_PCT   number(18,6),
    DESCRIPTION         string,
    CREATED_AT          timestamp_ntz default CURRENT_TIMESTAMP(),
    constraint PK_PORTFOLIO_PROFILE primary key (PROFILE_ID),
    constraint UQ_PORTFOLIO_PROFILE_NAME unique (NAME)
);

alter table MIP.APP.PORTFOLIO_PROFILE
    add column if not exists BUST_ACTION string default 'ALLOW_EXITS_ONLY';

update MIP.APP.PORTFOLIO_PROFILE
   set BUST_ACTION = 'ALLOW_EXITS_ONLY'
 where BUST_ACTION is null;

merge into MIP.APP.PORTFOLIO_PROFILE as target
using (
    select
        column1 as NAME,
        column2 as MAX_POSITIONS,
        column3 as MAX_POSITION_PCT,
        column4 as BUST_EQUITY_PCT,
        column5 as BUST_ACTION,
        column6 as DRAWDOWN_STOP_PCT,
        column7 as DESCRIPTION
    from values
        ('PRIVATE_SAVINGS', 5, 0.05, 0.60, 'ALLOW_EXITS_ONLY', 0.10, 'Capital preservation with tight risk controls.'),
        ('LOW_RISK', 8, 0.08, 0.50, 'LIQUIDATE_NEXT_BAR', 0.15, 'Conservative risk with moderate drawdown limits.'),
        ('HIGH_RISK', 15, 0.15, 0.35, 'LIQUIDATE_IMMEDIATE', 0.30, 'Aggressive risk targeting higher volatility.')
) as source
on target.NAME = source.NAME
when not matched then
    insert (
        NAME,
        MAX_POSITIONS,
        MAX_POSITION_PCT,
        BUST_EQUITY_PCT,
        BUST_ACTION,
        DRAWDOWN_STOP_PCT,
        DESCRIPTION
    )
    values (
        source.NAME,
        source.MAX_POSITIONS,
        source.MAX_POSITION_PCT,
        source.BUST_EQUITY_PCT,
        source.BUST_ACTION,
        source.DRAWDOWN_STOP_PCT,
        source.DESCRIPTION
    );

-----------------------------
-- 1. PORTFOLIO
-----------------------------
create table if not exists MIP.APP.PORTFOLIO (
    PORTFOLIO_ID            number        autoincrement,
    PROFILE_ID              number,
    NAME                    string        not null,
    BASE_CURRENCY           string        default 'USD',
    STARTING_CASH           number(18,2)  not null,
    LAST_SIMULATION_RUN_ID  string,
    LAST_SIMULATED_AT       timestamp_ntz,
    FINAL_EQUITY            number(18,2),
    TOTAL_RETURN            number(18,6),
    MAX_DRAWDOWN            number(18,6),
    WIN_DAYS                number,
    LOSS_DAYS               number,
    STATUS                  string        default 'ACTIVE',
    BUST_AT                 timestamp_ntz,
    NOTES                   string,
    CREATED_AT              timestamp_ntz default CURRENT_TIMESTAMP(),
    UPDATED_AT              timestamp_ntz default CURRENT_TIMESTAMP(),
    constraint PK_PORTFOLIO primary key (PORTFOLIO_ID),
    constraint FK_PORTFOLIO_PROFILE foreign key (PROFILE_ID)
        references MIP.APP.PORTFOLIO_PROFILE(PROFILE_ID)
);

-- Backfill missing PORTFOLIO_ID for legacy tables
alter table if exists MIP.APP.PORTFOLIO
    add column if not exists PORTFOLIO_ID number;

-- Ensure BUST_AT exists (required by V_PORTFOLIO_RISK_STATE)
alter table if exists MIP.APP.PORTFOLIO
    add column if not exists BUST_AT timestamp_ntz;

update MIP.APP.PORTFOLIO
   set PORTFOLIO_ID = seq4()
 where PORTFOLIO_ID is null;

-----------------------------
-- 2. PORTFOLIO_POSITIONS
-----------------------------
create table if not exists MIP.APP.PORTFOLIO_POSITIONS (
    PORTFOLIO_ID     number        not null,
    RUN_ID           string        not null,
    SYMBOL           string        not null,
    MARKET_TYPE      string        not null,
    INTERVAL_MINUTES number        not null,
    ENTRY_TS         timestamp_ntz not null,
    ENTRY_PRICE      number(18,8)  not null,
    QUANTITY         number(18,8)  not null,
    COST_BASIS       number(18,8)  not null,
    ENTRY_SCORE      number(18,10),
    ENTRY_INDEX      number        not null,
    HOLD_UNTIL_INDEX number        not null,
    CREATED_AT       timestamp_ntz default CURRENT_TIMESTAMP(),
    constraint PK_PORTFOLIO_POSITIONS primary key (
        PORTFOLIO_ID,
        RUN_ID,
        SYMBOL,
        ENTRY_TS
    )
);

-----------------------------
-- 3. PORTFOLIO_TRADES
-----------------------------
create table if not exists MIP.APP.PORTFOLIO_TRADES (
    TRADE_ID         number        autoincrement,
    PROPOSAL_ID      number,
    PORTFOLIO_ID     number        not null,
    RUN_ID           string        not null,
    SYMBOL           string        not null,
    MARKET_TYPE      string        not null,
    INTERVAL_MINUTES number        not null,
    TRADE_TS         timestamp_ntz not null,
    SIDE             string        not null,
    PRICE            number(18,8)  not null,
    QUANTITY         number(18,8)  not null,
    NOTIONAL         number(18,8)  not null,
    REALIZED_PNL     number(18,8),
    CASH_AFTER       number(18,2)  not null,
    SCORE            number(18,10),
    CREATED_AT       timestamp_ntz default CURRENT_TIMESTAMP(),
    constraint PK_PORTFOLIO_TRADES primary key (TRADE_ID)
);

alter table if exists MIP.APP.PORTFOLIO_TRADES
    add column if not exists PROPOSAL_ID number;

-----------------------------
-- 4. PORTFOLIO_DAILY
-----------------------------
create table if not exists MIP.APP.PORTFOLIO_DAILY (
    PORTFOLIO_ID     number        not null,
    RUN_ID           string        not null,
    TS               timestamp_ntz not null,
    CASH             number(18,2)  not null,
    EQUITY_VALUE     number(18,2)  not null,
    TOTAL_EQUITY     number(18,2)  not null,
    OPEN_POSITIONS   number        not null,
    DAILY_PNL        number(18,2),
    DAILY_RETURN     number(18,6),
    PEAK_EQUITY      number(18,2),
    DRAWDOWN         number(18,6),
    STATUS           string        default 'ACTIVE',
    CREATED_AT       timestamp_ntz default CURRENT_TIMESTAMP(),
    constraint PK_PORTFOLIO_DAILY primary key (PORTFOLIO_ID, RUN_ID, TS)
);

-----------------------------
-- 5. V_OPPORTUNITY_FEED
-----------------------------
create or replace view MIP.APP.V_OPPORTUNITY_FEED as
select
    RECOMMENDATION_ID,
    PATTERN_ID,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    TS,
    GENERATED_AT,
    SCORE,
    DETAILS
from MIP.APP.RECOMMENDATION_LOG;

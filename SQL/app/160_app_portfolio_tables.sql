-- 160_app_portfolio_tables.sql
-- Purpose: Portfolio simulation tables and supporting view

use role MIP_ADMIN_ROLE;
use database MIP;

-----------------------------
-- 1. PORTFOLIO
-----------------------------
create table if not exists MIP.APP.PORTFOLIO (
    PORTFOLIO_ID            number        autoincrement,
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
    NOTES                   string,
    CREATED_AT              timestamp_ntz default current_timestamp(),
    UPDATED_AT              timestamp_ntz default current_timestamp(),
    constraint PK_PORTFOLIO primary key (PORTFOLIO_ID)
);

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
    CREATED_AT       timestamp_ntz default current_timestamp(),
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
    CREATED_AT       timestamp_ntz default current_timestamp(),
    constraint PK_PORTFOLIO_TRADES primary key (TRADE_ID)
);

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
    CREATED_AT       timestamp_ntz default current_timestamp(),
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

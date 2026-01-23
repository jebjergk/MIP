-- 187_agent_out_order_proposals.sql
-- Purpose: Persist agent trade proposals

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.AGENT_OUT;

create table if not exists MIP.AGENT_OUT.ORDER_PROPOSALS (
    PROPOSAL_ID        number identity,
    RUN_ID             number,
    PORTFOLIO_ID       number,
    PROPOSED_AT        timestamp_ntz default current_timestamp(),
    SYMBOL             varchar,
    MARKET_TYPE        varchar,
    INTERVAL_MINUTES   number,
    SIDE               varchar,
    TARGET_WEIGHT      float,
    RECOMMENDATION_ID  number(38,0),
    SIGNAL_TS          timestamp_ntz,
    SIGNAL_PATTERN_ID  number(38,0),
    SIGNAL_INTERVAL_MINUTES number(38,0),
    SIGNAL_RUN_ID      varchar,
    SIGNAL_SNAPSHOT    variant,
    SOURCE_SIGNALS     variant,
    RATIONALE          variant,
    STATUS             varchar default 'PROPOSED',
    VALIDATION_ERRORS  variant,
    APPROVED_AT        timestamp_ntz,
    EXECUTED_AT        timestamp_ntz,
    constraint PK_ORDER_PROPOSALS primary key (PROPOSAL_ID)
);

alter table if exists MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists INTERVAL_MINUTES number;

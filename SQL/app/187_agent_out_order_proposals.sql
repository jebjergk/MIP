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
    SIDE               varchar,
    TARGET_WEIGHT      float,
    SOURCE_SIGNALS     variant,
    RATIONALE          variant,
    STATUS             varchar default 'PROPOSED',
    VALIDATION_ERRORS  variant,
    APPROVED_AT        timestamp_ntz,
    EXECUTED_AT        timestamp_ntz,
    constraint PK_ORDER_PROPOSALS primary key (PROPOSAL_ID)
);

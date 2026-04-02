-- Lifecycle broker reconciliation v1 (IB truth vs MIP lifecycle linkage).
-- Run as MIP_ADMIN_ROLE. Idempotent.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema LIVE;

create table if not exists MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE (
    PORTFOLIO_ID           number(38,0)   not null,
    SYMBOL                 varchar(64)    not null,
    IBKR_ACCOUNT_ID        varchar(32),
    SECURITY_TYPE          varchar(32),
    RULE_VERSION           varchar(32)    not null,
    RECONCILIATION_CLASS   varchar(64)    not null,
    BROKER_POSITION_QTY    number(18,8),
    BROKER_SNAPSHOT_TS     timestamp_ntz,
    MIP_ENTRY_ACTION_ID    varchar(64),
    MIP_ENTRY_STATUS     varchar(64),
    HAS_ENTRY_INTEL_LINK   boolean,
    DETAILS                variant,
    UPDATED_TS             timestamp_ntz  not null default current_timestamp(),
    constraint PK_LIFECYCLE_RECONCILIATION_STATE primary key (PORTFOLIO_ID, SYMBOL)
);

create table if not exists MIP.LIVE.LIFECYCLE_RECONCILIATION_EVENT (
    EVENT_ID               varchar(36)    not null,
    PORTFOLIO_ID           number(38,0)   not null,
    SYMBOL                 varchar(64)    not null,
    RULE_VERSION           varchar(32)    not null,
    NEW_CLASS              varchar(64)    not null,
    PREVIOUS_CLASS         varchar(64),
    DETAILS                variant,
    CREATED_TS             timestamp_ntz  not null default current_timestamp(),
    constraint PK_LIFECYCLE_RECONCILIATION_EVENT primary key (EVENT_ID)
);

-- UX API persists latest state and append-only events on class transitions.
grant select, insert, update on table MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE to role MIP_UI_API_ROLE;
grant select, insert on table MIP.LIVE.LIFECYCLE_RECONCILIATION_EVENT to role MIP_UI_API_ROLE;

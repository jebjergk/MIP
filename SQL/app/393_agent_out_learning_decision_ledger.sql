-- 393_agent_out_learning_decision_ledger.sql
-- Purpose: Immutable Learning-to-Decision ledger.
-- Captures causality from training events -> decision changes -> live actions/orders.

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.AGENT_OUT;

create table if not exists MIP.AGENT_OUT.LEARNING_DECISION_LEDGER (
    LEDGER_ID            number identity,
    EVENT_TS             timestamp_ntz not null default current_timestamp(),
    EVENT_TYPE           varchar(64)   not null,   -- TRAINING_EVENT | DECISION_EVENT | LIVE_EVENT
    EVENT_NAME           varchar(128)  not null,   -- e.g. TRAINING_DIGEST_GLOBAL | PROPOSAL_SELECTION
    STATUS               varchar(64),

    RUN_ID               varchar(64),
    PARENT_RUN_ID        varchar(64),
    PORTFOLIO_ID         number,
    PROPOSAL_ID          number,
    LIVE_ACTION_ID       varchar(64),
    LIVE_ORDER_ID        varchar(64),
    SYMBOL               varchar(32),
    MARKET_TYPE          varchar(32),

    TRAINING_VERSION     varchar(128),
    POLICY_VERSION       varchar(128),
    FEATURE_FLAGS        variant,

    BEFORE_STATE         variant,   -- deterministic pre-decision/training state
    AFTER_STATE          variant,   -- deterministic post-decision/training state
    INFLUENCE_DELTA      variant,   -- explicit effects (ranking, size, eligibility, monitoring)
    CAUSALITY_LINKS      variant,   -- linked ids and trace references
    OUTCOME_STATE        variant,   -- optional realized outcome snapshots

    SOURCE_FACTS_HASH    varchar(64),
    CREATED_AT           timestamp_ntz not null default current_timestamp(),

    constraint PK_LEARNING_DECISION_LEDGER primary key (LEDGER_ID)
);

-- Keep insert path additive and query path available for API and UI users.
grant select, insert on table MIP.AGENT_OUT.LEARNING_DECISION_LEDGER to role MIP_APP_ROLE;


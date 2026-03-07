-- 394_sp_ledger_append_event.sql
-- Purpose: Append one immutable Learning-to-Decision ledger row.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_LEDGER_APPEND_EVENT(
    P_EVENT_TYPE         varchar,
    P_EVENT_NAME         varchar,
    P_STATUS             varchar,
    P_RUN_ID             varchar,
    P_PARENT_RUN_ID      varchar,
    P_PORTFOLIO_ID       number,
    P_PROPOSAL_ID        number,
    P_LIVE_ACTION_ID     varchar,
    P_LIVE_ORDER_ID      varchar,
    P_SYMBOL             varchar,
    P_MARKET_TYPE        varchar,
    P_TRAINING_VERSION   varchar,
    P_POLICY_VERSION     varchar,
    P_FEATURE_FLAGS      variant,
    P_BEFORE_STATE       variant,
    P_AFTER_STATE        variant,
    P_INFLUENCE_DELTA    variant,
    P_CAUSALITY_LINKS    variant,
    P_OUTCOME_STATE      variant,
    P_SOURCE_FACTS_HASH  varchar
)
returns variant
language sql
execute as caller
as
$$
declare
    v_event_ts timestamp_ntz := current_timestamp();
begin
    insert into MIP.AGENT_OUT.LEARNING_DECISION_LEDGER (
        EVENT_TS,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        RUN_ID,
        PARENT_RUN_ID,
        PORTFOLIO_ID,
        PROPOSAL_ID,
        LIVE_ACTION_ID,
        LIVE_ORDER_ID,
        SYMBOL,
        MARKET_TYPE,
        TRAINING_VERSION,
        POLICY_VERSION,
        FEATURE_FLAGS,
        BEFORE_STATE,
        AFTER_STATE,
        INFLUENCE_DELTA,
        CAUSALITY_LINKS,
        OUTCOME_STATE,
        SOURCE_FACTS_HASH
    )
    select
        :v_event_ts,
        :P_EVENT_TYPE,
        :P_EVENT_NAME,
        :P_STATUS,
        :P_RUN_ID,
        :P_PARENT_RUN_ID,
        :P_PORTFOLIO_ID,
        :P_PROPOSAL_ID,
        :P_LIVE_ACTION_ID,
        :P_LIVE_ORDER_ID,
        :P_SYMBOL,
        :P_MARKET_TYPE,
        :P_TRAINING_VERSION,
        :P_POLICY_VERSION,
        :P_FEATURE_FLAGS,
        :P_BEFORE_STATE,
        :P_AFTER_STATE,
        :P_INFLUENCE_DELTA,
        :P_CAUSALITY_LINKS,
        :P_OUTCOME_STATE,
        :P_SOURCE_FACTS_HASH;

    return object_construct(
        'status', 'SUCCESS',
        'event_ts', :v_event_ts
    );
end;
$$;


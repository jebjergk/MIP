-- 410_entry_intel_lifecycle.sql
-- Phase 1: Entry Intelligence Snapshot (EIS) + action link + trade closeout backbone.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema LIVE;

create table if not exists MIP.LIVE.ENTRY_INTEL_SNAPSHOT (
    SNAPSHOT_ID       varchar(36)   not null,
    PROPOSAL_ID       number(38,0)  not null,
    PORTFOLIO_ID      number(38,0)  not null,
    EIS_VERSION       number(38,0)  not null default 1,
    CREATED_TS        timestamp_ntz not null default current_timestamp(),
    SIGNAL_RUN_ID     varchar(64),
    SYMBOL            varchar(32),
    PATTERN_ID        number(38,0),
    WORLDS_SPEC       variant       not null,
    ALPHA_SPEC        variant       not null,
    SOURCE_VERSION    varchar(32),
    EIS_NOTE          varchar(1024),
    constraint PK_ENTRY_INTEL_SNAPSHOT primary key (SNAPSHOT_ID),
    constraint UQ_ENTRY_INTEL_PROPOSAL_VERSION unique (PROPOSAL_ID, EIS_VERSION)
);

create table if not exists MIP.LIVE.ENTRY_INTEL_ACTION_LINK (
    LINK_ID           varchar(36)   not null,
    SNAPSHOT_ID       varchar(36)   not null,
    PROPOSAL_ID       number(38,0)  not null,
    ENTRY_ACTION_ID   varchar(64)   not null,
    LINK_CREATED_TS   timestamp_ntz not null default current_timestamp(),
    constraint PK_ENTRY_INTEL_ACTION_LINK primary key (LINK_ID),
    constraint UQ_ENTRY_INTEL_ACTION_LINK_ACTION unique (ENTRY_ACTION_ID),
    constraint FK_EIAL_SNAPSHOT foreign key (SNAPSHOT_ID) references MIP.LIVE.ENTRY_INTEL_SNAPSHOT(SNAPSHOT_ID)
);

create table if not exists MIP.LIVE.TRADE_CLOSEOUT (
    CLOSEOUT_ID           varchar(36)   not null,
    ENTRY_ACTION_ID       varchar(64)   not null,
    SNAPSHOT_ID           varchar(36),
    EXIT_TYPE             varchar(32)   not null,
    REALIZED_RETURN_PCT   float,
    HOLDING_PERIOD_SEC    number(38,0),
    EXIT_TS               timestamp_ntz not null default current_timestamp(),
    ALIGNMENT_JSON        variant       not null,
    CREATED_TS            timestamp_ntz not null default current_timestamp(),
    constraint PK_TRADE_CLOSEOUT primary key (CLOSEOUT_ID),
    constraint UQ_TRADE_CLOSEOUT_ENTRY unique (ENTRY_ACTION_ID),
    constraint FK_TC_SNAPSHOT foreign key (SNAPSHOT_ID) references MIP.LIVE.ENTRY_INTEL_SNAPSHOT(SNAPSHOT_ID)
);

create or replace procedure MIP.APP.SP_ENSURE_ENTRY_INTEL_FOR_RUN(
    P_RUN_ID varchar,
    P_PORTFOLIO_ID number
)
returns variant
language sql
execute as caller
as
$$
declare
    v_inserted number := 0;
begin
    insert into MIP.LIVE.ENTRY_INTEL_SNAPSHOT (
        SNAPSHOT_ID,
        PROPOSAL_ID,
        PORTFOLIO_ID,
        EIS_VERSION,
        SIGNAL_RUN_ID,
        SYMBOL,
        PATTERN_ID,
        WORLDS_SPEC,
        ALPHA_SPEC,
        SOURCE_VERSION,
        EIS_NOTE
    )
    select
        uuid_string(),
        op.PROPOSAL_ID,
        op.PORTFOLIO_ID,
        1,
        op.RUN_ID_VARCHAR,
        op.SYMBOL,
        op.SIGNAL_PATTERN_ID,
        object_construct(
            'phase', 1,
            'stub', true,
            'source', 'SP_ENSURE_ENTRY_INTEL_FOR_RUN',
            'hod_placeholder', object_construct('note', 'Phase 1 stub')
        ),
        object_construct(
            'phase', 1,
            'stub', true,
            'note', 'Phase 1 stub alpha'
        ),
        'EIS_SCHEMA_V1',
        null
    from MIP.AGENT_OUT.ORDER_PROPOSALS op
    where op.RUN_ID_VARCHAR = :P_RUN_ID
      and op.PORTFOLIO_ID = :P_PORTFOLIO_ID
      and not exists (
          select 1
          from MIP.LIVE.ENTRY_INTEL_SNAPSHOT e
          where e.PROPOSAL_ID = op.PROPOSAL_ID
      );

    v_inserted := sqlrowcount;
    return object_construct(
        'status', 'SUCCESS',
        'inserted_count', :v_inserted,
        'run_id', :P_RUN_ID,
        'portfolio_id', :P_PORTFOLIO_ID
    );
end;
$$;

create or replace procedure MIP.APP.SP_ENSURE_ENTRY_INTEL_FOR_PROPOSAL(
    P_PROPOSAL_ID number
)
returns variant
language sql
execute as caller
as
$$
declare
    v_inserted number := 0;
begin
    insert into MIP.LIVE.ENTRY_INTEL_SNAPSHOT (
        SNAPSHOT_ID,
        PROPOSAL_ID,
        PORTFOLIO_ID,
        EIS_VERSION,
        SIGNAL_RUN_ID,
        SYMBOL,
        PATTERN_ID,
        WORLDS_SPEC,
        ALPHA_SPEC,
        SOURCE_VERSION,
        EIS_NOTE
    )
    select
        uuid_string(),
        op.PROPOSAL_ID,
        op.PORTFOLIO_ID,
        1,
        op.RUN_ID_VARCHAR,
        op.SYMBOL,
        op.SIGNAL_PATTERN_ID,
        object_construct(
            'phase', 1,
            'stub', true,
            'source', 'SP_ENSURE_ENTRY_INTEL_FOR_PROPOSAL',
            'hod_placeholder', object_construct('note', 'Phase 1 stub')
        ),
        object_construct('phase', 1, 'stub', true),
        'EIS_SCHEMA_V1',
        null
    from MIP.AGENT_OUT.ORDER_PROPOSALS op
    where op.PROPOSAL_ID = :P_PROPOSAL_ID
      and not exists (
          select 1 from MIP.LIVE.ENTRY_INTEL_SNAPSHOT e where e.PROPOSAL_ID = op.PROPOSAL_ID
      );

    v_inserted := sqlrowcount;
    return object_construct(
        'status', 'SUCCESS',
        'inserted_count', :v_inserted,
        'proposal_id', :P_PROPOSAL_ID
    );
end;
$$;

-- 055_app_audit_log.sql
-- Purpose: Append-only audit log for MIP procedures and tasks.
-- REPLAY_CONTEXT: when set for a run_id (day_run_id), SP_AUDIT_LOG_STEP and SP_LOG_EVENT
-- write EVENT_TYPE='REPLAY' and add mode, replay_batch_id, effective_to_ts, day_run_id to DETAILS.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.REPLAY_CONTEXT (
    RUN_ID             string primary key,   -- day_run_id for this replay day
    REPLAY_BATCH_ID    string not null,
    EFFECTIVE_TO_TS    timestamp_ntz not null,
    CREATED_AT         timestamp_ntz default current_timestamp()
);

create table if not exists MIP.APP.MIP_AUDIT_LOG (
    EVENT_TS          timestamp_ntz default CURRENT_TIMESTAMP(),
    RUN_ID            string        default uuid_string(),
    PARENT_RUN_ID     string,
    EVENT_TYPE        string        default 'GENERAL',
    EVENT_NAME        string,
    STATUS            string        default 'INFO',
    ROWS_AFFECTED     number,
    DETAILS           variant,
    ERROR_MESSAGE     string,
    INVOKED_BY_USER   string        default current_user(),
    INVOKED_BY_ROLE   string        default current_role(),
    INVOKED_WAREHOUSE string        default current_warehouse(),
    QUERY_ID          string        default last_query_id(),
    SESSION_ID        string        default current_session()
);

create or replace procedure MIP.APP.SP_LOG_EVENT(
    P_EVENT_TYPE string,
    P_EVENT_NAME string,
    P_STATUS string,
    P_ROWS_AFFECTED number,
    P_DETAILS variant,
    P_ERROR_MESSAGE string,
    P_RUN_ID string default null,
    P_PARENT_RUN_ID string default null,
    P_ROOT_RUN_ID string default null,
    P_EVENT_RUN_ID string default null
)
returns varchar
language sql
execute as owner
as
$$
declare
    v_run_id string;
    v_parent_run_id string;
    v_event_type string := :P_EVENT_TYPE;
    v_details variant := coalesce(try_parse_json(:P_DETAILS), :P_DETAILS);
    v_replay_batch_id string;
    v_effective_to_ts timestamp_ntz;
    v_replay_run_id string;
begin
    if (:P_EVENT_TYPE = 'PIPELINE') then
        v_run_id := coalesce(:P_RUN_ID, nullif(current_query_tag(), ''), uuid_string());
        v_parent_run_id := coalesce(nullif(:P_PARENT_RUN_ID, ''), null);
    else
        v_parent_run_id := coalesce(
            nullif(:P_PARENT_RUN_ID, ''),
            nullif(:P_ROOT_RUN_ID, ''),
            nullif(current_query_tag(), '')
        );
        v_run_id := coalesce(nullif(:P_EVENT_RUN_ID, ''), uuid_string());
    end if;

    -- Replay tagging: if this run_id or parent is in REPLAY_CONTEXT, use EVENT_TYPE=REPLAY and add replay DETAILS
    select REPLAY_BATCH_ID, EFFECTIVE_TO_TS, RUN_ID
      into :v_replay_batch_id, :v_effective_to_ts, :v_replay_run_id
      from MIP.APP.REPLAY_CONTEXT
     where RUN_ID = :v_run_id or RUN_ID = :v_parent_run_id
     limit 1;
    if (v_replay_batch_id is not null) then
        v_event_type := 'REPLAY';
        v_details := coalesce(v_details, object_construct());
        -- Remove replay keys first so object_insert is idempotent (caller may already pass them)
        v_details := object_delete(v_details, 'mode');
        v_details := object_delete(v_details, 'replay_batch_id');
        v_details := object_delete(v_details, 'effective_to_ts');
        v_details := object_delete(v_details, 'day_run_id');
        v_details := object_insert(v_details, 'mode', 'REPLAY');
        v_details := object_insert(v_details, 'replay_batch_id', :v_replay_batch_id);
        v_details := object_insert(v_details, 'effective_to_ts', :v_effective_to_ts);
        v_details := object_insert(v_details, 'day_run_id', :v_replay_run_id);
    end if;

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        PARENT_RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS,
        ERROR_MESSAGE
    )
    select
        CURRENT_TIMESTAMP(),
        :v_run_id,
        :v_parent_run_id,
        :v_event_type,
        :P_EVENT_NAME,
        :P_STATUS,
        :P_ROWS_AFFECTED,
        :v_details,
        :P_ERROR_MESSAGE;

    return v_run_id;
end;
$$;

create or replace procedure MIP.APP.SP_AUDIT_LOG_STEP(
    P_PARENT_RUN_ID string,
    P_EVENT_NAME string,
    P_STATUS string,
    P_ROWS_AFFECTED number,
    P_DETAILS variant,
    P_ERROR_MESSAGE string default null
)
returns string
language sql
execute as owner
as
$$
declare
    v_step_name string;
    v_scope string;
    v_scope_key string;
    v_existing_run_id string;
    v_new_run_id string := uuid_string();
    v_event_type string := 'PIPELINE_STEP';
    v_details variant := coalesce(try_parse_json(:P_DETAILS), :P_DETAILS);
    v_replay_batch_id string;
    v_effective_to_ts timestamp_ntz;
begin
    -- Extract required fields from P_DETAILS
    v_step_name := :P_DETAILS:"step_name"::string;
    v_scope := coalesce(:P_DETAILS:"scope"::string, 'AGG');
    v_scope_key := :P_DETAILS:"scope_key"::string;

    -- Replay tagging: if P_PARENT_RUN_ID is in REPLAY_CONTEXT, use EVENT_TYPE=REPLAY and add replay DETAILS
    select REPLAY_BATCH_ID, EFFECTIVE_TO_TS
      into :v_replay_batch_id, :v_effective_to_ts
      from MIP.APP.REPLAY_CONTEXT
     where RUN_ID = :P_PARENT_RUN_ID
     limit 1;
    if (v_replay_batch_id is not null) then
        v_event_type := 'REPLAY';
        -- Remove replay keys first so object_insert is idempotent
        v_details := object_delete(v_details, 'mode');
        v_details := object_delete(v_details, 'replay_batch_id');
        v_details := object_delete(v_details, 'effective_to_ts');
        v_details := object_delete(v_details, 'day_run_id');
        v_details := object_insert(v_details, 'mode', 'REPLAY');
        v_details := object_insert(v_details, 'replay_batch_id', :v_replay_batch_id);
        v_details := object_insert(v_details, 'effective_to_ts', :v_effective_to_ts);
        v_details := object_insert(v_details, 'day_run_id', :P_PARENT_RUN_ID);
    end if;

    -- Check for existing duplicate row (idempotent insert); match EVENT_TYPE used for this run
    select RUN_ID
      into :v_existing_run_id
      from MIP.APP.MIP_AUDIT_LOG
     where PARENT_RUN_ID = :P_PARENT_RUN_ID
       and EVENT_TYPE = :v_event_type
       and EVENT_NAME = :P_EVENT_NAME
       and STATUS = :P_STATUS
       and DETAILS:"step_name"::string = :v_step_name
       and coalesce(DETAILS:"scope"::string, 'AGG') = :v_scope
       and (
           (DETAILS:"scope_key"::string is null and :v_scope_key is null)
           or (DETAILS:"scope_key"::string = :v_scope_key)
       )
     limit 1;

    if (v_existing_run_id is not null) then
        return v_existing_run_id;
    end if;

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        PARENT_RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS,
        ERROR_MESSAGE
    )
    select
        CURRENT_TIMESTAMP(),
        :v_new_run_id,
        :P_PARENT_RUN_ID,
        :v_event_type,
        :P_EVENT_NAME,
        :P_STATUS,
        :P_ROWS_AFFECTED,
        :v_details,
        :P_ERROR_MESSAGE;

    return v_new_run_id;
end;
$$;

-- 055_app_audit_log.sql
-- Purpose: Append-only audit log for MIP procedures and tasks

use role MIP_ADMIN_ROLE;
use database MIP;

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
        :P_EVENT_TYPE,
        :P_EVENT_NAME,
        :P_STATUS,
        :P_ROWS_AFFECTED,
        coalesce(try_parse_json(:P_DETAILS), :P_DETAILS),
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
begin
    -- Extract required fields from P_DETAILS
    v_step_name := :P_DETAILS:"step_name"::string;
    v_scope := coalesce(:P_DETAILS:"scope"::string, 'AGG');
    v_scope_key := :P_DETAILS:"scope_key"::string;

    -- Check for existing duplicate row (idempotent insert)
    select RUN_ID
      into :v_existing_run_id
      from MIP.APP.MIP_AUDIT_LOG
     where PARENT_RUN_ID = :P_PARENT_RUN_ID
       and EVENT_TYPE = 'PIPELINE_STEP'
       and EVENT_NAME = :P_EVENT_NAME
       and STATUS = :P_STATUS
       and DETAILS:"step_name"::string = :v_step_name
       and coalesce(DETAILS:"scope"::string, 'AGG') = :v_scope
       and (
           (DETAILS:"scope_key"::string is null and :v_scope_key is null)
           or (DETAILS:"scope_key"::string = :v_scope_key)
       )
     limit 1;

    -- If duplicate exists, return existing RUN_ID
    if (v_existing_run_id is not null) then
        return v_existing_run_id;
    end if;

    -- Insert new step log with fresh UUID
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
        'PIPELINE_STEP',
        :P_EVENT_NAME,
        :P_STATUS,
        :P_ROWS_AFFECTED,
        coalesce(try_parse_json(:P_DETAILS), :P_DETAILS),
        :P_ERROR_MESSAGE;

    return v_new_run_id;
end;
$$;

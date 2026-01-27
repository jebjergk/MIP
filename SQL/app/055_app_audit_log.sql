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
    P_PARENT_RUN_ID string default null
)
returns varchar
language sql
execute as owner
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, nullif(current_query_tag(), ''), uuid_string());
    v_parent_run_id string := coalesce(
        nullif(:P_PARENT_RUN_ID, ''),
        iff(:P_EVENT_TYPE <> 'PIPELINE', nullif(current_query_tag(), ''), null)
    );
begin
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

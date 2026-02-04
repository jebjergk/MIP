-- 190_sp_enforce_run_scoping.sql
-- Purpose: Helper procedure to validate run scoping and prevent cross-run contamination.
-- Optional P_EFFECTIVE_TO_TS: when set (e.g. replay/time-travel), stores override so downstream
-- "latest ts" logic uses TS <= P_EFFECTIVE_TO_TS instead of max(TS).

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.RUN_SCOPE_OVERRIDE (
    RUN_ID            string primary key,
    EFFECTIVE_TO_TS    timestamp_ntz not null,
    CREATED_AT         timestamp_ntz default current_timestamp()
);

create or replace procedure MIP.APP.SP_ENFORCE_RUN_SCOPING(
    P_RUN_ID string,   -- canonical pipeline run id (UUID/varchar)
    P_PORTFOLIO_ID number default null,
    P_EFFECTIVE_TO_TS timestamp_ntz default null  -- when set, time-travel/replay: downstream uses this as "latest" cap
)
returns variant
language sql
execute as caller
as
$$
declare
    -- Copy parameters to local variables to avoid binding issues with exception handlers
    v_run_id_param string := :P_RUN_ID;
    v_portfolio_id number := :P_PORTFOLIO_ID;
    v_effective_to_ts timestamp_ntz := :P_EFFECTIVE_TO_TS;
    v_validation_errors array := array_construct();
    v_validation_status string := 'PASS';
    v_result variant;
    v_audit_count number := 0;
    v_mismatch_count number := 0;
begin
    -- Effective-date override (replay/time-travel): store so downstream procs can cap "latest ts"
    if (:v_effective_to_ts is not null) then
        merge into MIP.APP.RUN_SCOPE_OVERRIDE t
        using (select :v_run_id_param as RUN_ID, :v_effective_to_ts as EFFECTIVE_TO_TS) s
        on t.RUN_ID = s.RUN_ID
        when matched then update set t.EFFECTIVE_TO_TS = s.EFFECTIVE_TO_TS, t.CREATED_AT = current_timestamp()
        when not matched then insert (RUN_ID, EFFECTIVE_TO_TS) values (s.RUN_ID, s.EFFECTIVE_TO_TS);
        return object_construct(
            'status', 'PASS',
            'run_id', :v_run_id_param,
            'portfolio_id', :v_portfolio_id,
            'effective_to_ts', :v_effective_to_ts,
            'validation_errors', array_construct(),
            'timestamp', current_timestamp()
        );
    end if;

    -- Validation 1: Ensure pipeline run ID exists in audit log
    select count(*) into :v_audit_count
      from MIP.APP.MIP_AUDIT_LOG
     where RUN_ID = :v_run_id_param
       and EVENT_TYPE = 'PIPELINE'
       and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE';
    if (v_audit_count = 0) then
        v_validation_errors := array_append(
            :v_validation_errors,
            'INVALID_RUN_ID: Run ID ' || :v_run_id_param || ' not found in audit log'
        );
        v_validation_status := 'FAIL';
    end if;

    -- Validation 2: If portfolio ID provided, check proposals scoped by RUN_ID_VARCHAR only
    if (:v_portfolio_id is not null and :v_run_id_param is not null) then
        select count(*) into :v_mismatch_count
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where PORTFOLIO_ID = :v_portfolio_id
           and coalesce(RUN_ID_VARCHAR, '') != :v_run_id_param
           and PROPOSED_AT >= current_timestamp() - interval '1 hour';
        if (v_mismatch_count > 0) then
            v_validation_errors := array_append(
                :v_validation_errors,
                'PROPOSAL_RUN_ID_MISMATCH: Found proposals with different run ID for portfolio ' || :v_portfolio_id
            );
            v_validation_status := 'WARN';
        end if;
    end if;

    v_result := object_construct(
        'status', :v_validation_status,
        'run_id', :v_run_id_param,
        'portfolio_id', :v_portfolio_id,
        'validation_errors', :v_validation_errors,
        'timestamp', current_timestamp()
    );

    -- Log validation result
    if (v_validation_status != 'PASS') then
        call MIP.APP.SP_LOG_EVENT(
            'VALIDATION',
            'SP_ENFORCE_RUN_SCOPING',
            :v_validation_status,
            null,
            :v_result,
            null,
            :v_run_id_param,
            null
        );
    end if;

    return :v_result;
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'VALIDATION',
            'SP_ENFORCE_RUN_SCOPING',
            'FAIL',
            null,
            object_construct('error', :sqlerrm),
            :sqlerrm,
            :v_run_id_param,
            null
        );
        raise;
end;
$$;

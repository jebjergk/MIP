-- 190_sp_enforce_run_scoping.sql
-- Purpose: Helper procedure to validate run scoping and prevent cross-run contamination

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_ENFORCE_RUN_SCOPING(
    P_RUN_ID string,   -- canonical pipeline run id (UUID/varchar)
    P_PORTFOLIO_ID number default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_validation_errors array := array_construct();
    v_validation_status string := 'PASS';
    v_result variant;
begin
    -- Validation 1: Ensure pipeline run ID exists in audit log
    if not exists (
        select 1
        from MIP.APP.MIP_AUDIT_LOG
        where RUN_ID = :P_RUN_ID
          and EVENT_TYPE = 'PIPELINE'
          and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
        limit 1
    ) then
        v_validation_errors := array_append(
            :v_validation_errors,
            'INVALID_RUN_ID: Run ID ' || :P_RUN_ID || ' not found in audit log'
        );
        v_validation_status := 'FAIL';
    end if;

    -- Validation 2: If portfolio ID provided, check proposals scoped by RUN_ID_VARCHAR only
    if (:P_PORTFOLIO_ID is not null and :P_RUN_ID is not null) then
        if exists (
            select 1
            from MIP.AGENT_OUT.ORDER_PROPOSALS
            where PORTFOLIO_ID = :P_PORTFOLIO_ID
              and coalesce(RUN_ID_VARCHAR, '') != :P_RUN_ID
              and PROPOSED_AT >= current_timestamp() - interval '1 hour'
            limit 1
        ) then
            v_validation_errors := array_append(
                :v_validation_errors,
                'PROPOSAL_RUN_ID_MISMATCH: Found proposals with different run ID for portfolio ' || :P_PORTFOLIO_ID
            );
            v_validation_status := 'WARN';
        end if;
    end if;

    v_result := object_construct(
        'status', :v_validation_status,
        'run_id', :P_RUN_ID,
        'portfolio_id', :P_PORTFOLIO_ID,
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
            :P_RUN_ID,
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
            :P_RUN_ID,
            null
        );
        raise;
end;
$$;

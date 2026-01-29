-- 195_sp_agent_run_all.sql
-- [A3] Pipeline step wrapper: run all agents (future-proof). For now calls SP_AGENT_GENERATE_MORNING_BRIEF only.
-- Returns JSON: { agent_results: [{ agent_name, status, brief_id }] }

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_AGENT_RUN_ALL(
    P_AS_OF_TS      timestamp_ntz,
    P_SIGNAL_RUN_ID string   -- pipeline run id (recommendations DETAILS:run_id) for deterministic tie-back
)
returns variant
language sql
execute as caller
as
$$
declare
    v_agent_name   string := 'AGENT_V0_MORNING_BRIEF';
    v_brief_result variant;
    v_brief_id     number;
    v_status       string := 'SUCCESS';
    v_agent_results array := array_construct();
begin
    -- Call morning brief agent
    v_brief_result := (call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF(:P_AS_OF_TS, :P_SIGNAL_RUN_ID));

    -- If procedure returned error object (has status='ERROR'), capture status
    if (v_brief_result is not null and v_brief_result:status::string = 'ERROR') then
        v_status := 'ERROR';
        v_brief_id := null;
    else
        v_brief_id := (
            select BRIEF_ID
            from MIP.AGENT_OUT.MORNING_BRIEF
            where PORTFOLIO_ID = 0
              and RUN_ID = :v_agent_name || '_' || to_varchar(:P_AS_OF_TS, 'YYYY-MM-DD"T"HH24:MI:SS.FF3') || '_' || :P_SIGNAL_RUN_ID
            limit 1
        );
    end if;

    v_agent_results := array_construct(
        object_construct('agent_name', :v_agent_name, 'status', :v_status, 'brief_id', :v_brief_id)
    );

    return object_construct('agent_results', :v_agent_results);
end;
$$;

-- 195_sp_agent_run_all.sql
-- [A3] Pipeline step wrapper: run all agents (future-proof).
-- AGENT_V0_MORNING_BRIEF removed: no longer calls SP_AGENT_GENERATE_MORNING_BRIEF (no portfolio_id=0 writes).
-- Returns JSON: { agent_results: [{ agent_name, status, brief_id }] }

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_AGENT_RUN_ALL(
    P_AS_OF_TS      timestamp_ntz,
    P_RUN_ID        varchar   -- pipeline run id (UUID); trusted signals view is RUN_ID-keyed
)
returns variant
language sql
execute as caller
as
$$
declare
    v_agent_name   string := 'AGENT_V0_MORNING_BRIEF';
    v_status       string := 'SKIPPED';
    v_agent_results array := array_construct();
begin
    -- AGENT_V0_MORNING_BRIEF disabled: do not call SP_AGENT_GENERATE_MORNING_BRIEF (no writes to MORNING_BRIEF with portfolio_id=0).
    v_agent_results := array_construct(
        object_construct('agent_name', :v_agent_name, 'status', :v_status, 'brief_id', null)
    );

    return object_construct('agent_results', :v_agent_results);
end;
$$;
